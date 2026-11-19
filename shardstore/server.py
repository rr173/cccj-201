"""shardstore server: a local sharded MVCC row store with online resharding.

Layout under --dir:
  control.sqlite     routing epochs, commit decisions (with payloads, the
                     recovery basis), read tokens, split state machine
  shards/<id>.db     one SQLite file per shard: append-only row versions
                     keyed by (tenant, key, seq) plus an applied frontier

Commit protocol (single serialized writer):
  1. dedup by request_id (same payload -> stored receipt, different -> reject)
  2. append the commit decision (seq, payload, target shards) to the log
  3. apply the batch to every involved shard (per-shard transactions)
  4. advance the visible frontier
A reader pins (read_seq, routing epoch) in an explicit token and therefore
observes exactly one commit frontier: all changes of a commit become visible
together, and a token never mixes two commits.

Split protocol (one at a time):
  * register: record the split (COPYING), keep the old routing in service
  * every later commit is double-written to the two target shards, and the
    source/target frontiers are all bumped to the commit seq
  * a background worker copies the pre-split state in bounded batches
    (latest version per key as of copy_seq, tombstones dropped), persisting
    its cursor so a restart resumes instead of restarting
  * when the copy is done and both targets hold the same commit frontier as
    the source, one control transaction publishes the new routing epoch
    (never left half first); writers only wait for that short barrier
  * the retired source keeps serving old read tokens and is reclaimed only
    when no unreleased token or recovery evidence depends on it

Crash recovery: replay unapplied commit decisions onto shards (idempotent),
resume or finish an interrupted split/reclaim from the persisted state.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import signal
import socket
import sqlite3
import sys
import threading
import time
import traceback

from shardstore import common
from shardstore.common import ProtoError

SCHEMA_CONTROL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commits (
  seq INTEGER PRIMARY KEY,
  request_id TEXT NOT NULL UNIQUE,
  request_hash TEXT NOT NULL,
  receipt_json TEXT NOT NULL,
  apply_json TEXT NOT NULL,
  bump_json TEXT NOT NULL,
  applied INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS epochs (
  epoch INTEGER NOT NULL,
  shard_id TEXT NOT NULL,
  lo_t TEXT, lo_k INTEGER,
  hi_t TEXT, hi_k INTEGER,
  PRIMARY KEY (epoch, shard_id)
);
CREATE TABLE IF NOT EXISTS shards (
  shard_id TEXT PRIMARY KEY,
  state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tokens (
  token_id TEXT PRIMARY KEY,
  epoch INTEGER NOT NULL,
  read_seq INTEGER NOT NULL,
  released INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS splits (
  split_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  bt TEXT NOT NULL, bk INTEGER NOT NULL,
  left_id TEXT NOT NULL, right_id TEXT NOT NULL,
  copy_seq INTEGER NOT NULL,
  cur_t TEXT, cur_k INTEGER,
  copy_done INTEGER NOT NULL DEFAULT 0,
  phase TEXT NOT NULL,
  new_epoch INTEGER
);
"""

SCHEMA_SHARD = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS kv (
  tenant TEXT NOT NULL COLLATE BINARY,
  k INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  op TEXT NOT NULL,
  vt TEXT, vi INTEGER, vb BLOB,
  PRIMARY KEY (tenant, k, seq)
);
CREATE TABLE IF NOT EXISTS frontier (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  seq INTEGER NOT NULL
);
INSERT OR IGNORE INTO frontier(id, seq) VALUES (1, 0);
"""

CRASH_POINTS = {
    "after_decision",   # write: commit record durable, before shard apply
    "during_copy",      # split: after a copy batch is persisted
    "after_copy",       # split: bulk copy done, before the catch-up check
    "after_catchup",    # split: frontiers verified, before the publish barrier
    "before_publish",   # split: inside the barrier, just before the publish tx
    "after_publish",    # split: just after the publish tx commits
    "during_reclaim",   # reclaim: after marking RECLAIMING, before file delete
}

ACTIVE_SPLIT_PHASES = ("COPYING", "CATCHING_UP")


class StoreError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def _max_bound(a, b):
    """Larger of two lower bounds (None means -infinity)."""
    if a is None:
        return b
    if b is None:
        return a
    return a if common.bound_ck(a) >= common.bound_ck(b) else b


def _min_bound(a, b):
    """Smaller of two upper bounds (None means +infinity)."""
    if a is None:
        return b
    if b is None:
        return a
    return a if common.bound_ck(a) <= common.bound_ck(b) else b


class Server:
    def __init__(self, directory, batch_size=500):
        self.dir = directory
        self.shards_dir = os.path.join(directory, "shards")
        os.makedirs(self.shards_dir, exist_ok=True)
        self.control_path = os.path.join(directory, "control.sqlite")
        self.batch_size = batch_size
        self.commit_lock = threading.RLock()
        self.shutdown_flag = threading.Event()
        self.copy_paused = threading.Event()       # test hook
        self.crash_points = set()                  # test hook (in-memory only)
        self.inflight_lock = threading.Lock()
        self.inflight = {}                         # token -> [epoch, read_seq, refs]
        self._init_control()
        self._recover()

    # ------------------------------------------------------------ db help
    def _control(self):
        c = sqlite3.connect(self.control_path, timeout=60)
        c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA busy_timeout=60000")
        return c

    def _shard_path(self, shard_id):
        return os.path.join(self.shards_dir, shard_id + ".db")

    def _shard_conn(self, shard_id):
        p = self._shard_path(shard_id)
        if not os.path.exists(p):
            raise StoreError("INTERNAL", f"shard file missing: {shard_id}")
        c = sqlite3.connect(p, timeout=60)
        c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA busy_timeout=60000")
        return c

    def _create_shard(self, shard_id):
        c = sqlite3.connect(self._shard_path(shard_id))
        try:
            c.executescript(SCHEMA_SHARD)
            c.commit()
        finally:
            c.close()

    @staticmethod
    def _delete_shard_files(shard_path):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(shard_path + suffix)
            except FileNotFoundError:
                pass

    def _init_control(self):
        fresh = not os.path.exists(self.control_path)
        if fresh:
            # create the initial shard file before registering it
            self._create_shard("s1")
        c = sqlite3.connect(self.control_path)
        try:
            c.executescript(SCHEMA_CONTROL)
            row = c.execute("SELECT v FROM meta WHERE k='visible_seq'").fetchone()
            if row is None:
                c.execute("INSERT INTO meta(k,v) VALUES ('visible_seq','0')")
                c.execute("INSERT INTO meta(k,v) VALUES ('epoch','1')")
                c.execute("INSERT INTO meta(k,v) VALUES ('shard_counter','1')")
                c.execute("INSERT INTO shards(shard_id,state) VALUES ('s1','active')")
                c.execute("INSERT INTO epochs(epoch,shard_id,lo_t,lo_k,hi_t,hi_k)"
                          " VALUES (1,'s1',NULL,NULL,NULL,NULL)")
            c.commit()
        finally:
            c.close()

    # ------------------------------------------------------------ recovery
    def _recover(self):
        with self.commit_lock:
            self._recover_commits()
            c = self._control()
            try:
                # finish interrupted reclaims (file deletion is idempotent)
                rows = c.execute(
                    "SELECT split_id, source FROM splits WHERE phase='RECLAIMING'"
                ).fetchall()
                for split_id, source in rows:
                    self._delete_shard_files(self._shard_path(source))
                    c.execute("UPDATE shards SET state='reclaimed' WHERE shard_id=?",
                              (source,))
                    c.execute("UPDATE splits SET phase='RECLAIMED' WHERE split_id=?",
                              (split_id,))
                    c.commit()
                # resume an unpublished split, if any
                row = c.execute(
                    "SELECT split_id, source, left_id, right_id, copy_seq, copy_done"
                    " FROM splits WHERE phase IN ('COPYING','CATCHING_UP')").fetchone()
                if row is not None:
                    split_id, source, left_id, right_id, copy_seq, copy_done = row
                    # frontier bumps that may have been lost between transactions:
                    # the source bump at registration, and (once the bulk copy is
                    # done) the target bumps at the copy-done mark
                    self._bump_frontier(source, copy_seq)
                    if copy_done:
                        self._bump_frontier(left_id, copy_seq)
                        self._bump_frontier(right_id, copy_seq)
            finally:
                c.close()
            if row is not None:
                self._start_copy_worker(split_id)

    def _recover_commits(self):
        """Replay commit decisions that did not finish applying (idempotent)."""
        c = self._control()
        try:
            rows = c.execute(
                "SELECT seq, apply_json, bump_json FROM commits"
                " WHERE applied=0 ORDER BY seq").fetchall()
            for seq, apply_json, bump_json in rows:
                self._apply_to_shards(json.loads(apply_json), json.loads(bump_json), seq)
                c.execute("UPDATE commits SET applied=1 WHERE seq=?", (seq,))
                c.execute("UPDATE meta SET v=? WHERE k='visible_seq'", (str(seq),))
                c.commit()
        finally:
            c.close()

    # ------------------------------------------------------------ small io
    def _meta_int(self, c, k):
        row = c.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        if row is None:
            raise StoreError("INTERNAL", f"meta key missing: {k}")
        return int(row[0])

    def _bump_frontier(self, shard_id, seq):
        conn = self._shard_conn(shard_id)
        try:
            conn.execute(
                "INSERT INTO frontier(id,seq) VALUES (1,?) "
                "ON CONFLICT(id) DO UPDATE SET seq=MAX(seq, excluded.seq)", (seq,))
            conn.commit()
        finally:
            conn.close()

    def _frontier(self, shard_id):
        conn = self._shard_conn(shard_id)
        try:
            row = conn.execute("SELECT seq FROM frontier WHERE id=1").fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def _routes(self, c, epoch):
        rows = c.execute(
            "SELECT shard_id, lo_t, lo_k, hi_t, hi_k FROM epochs WHERE epoch=?",
            (epoch,)).fetchall()
        routes = []
        for sid, lt, lk, ht, hk in rows:
            lo = None if lt is None else (lt, lk)
            hi = None if ht is None else (ht, hk)
            routes.append((sid, lo, hi))
        routes.sort(key=lambda r: (0,) if r[1] is None else (1, common.bound_ck(r[1])))
        return routes

    @staticmethod
    def _route_lookup(routes, tenant, key):
        k = common.ck(tenant, key)
        for sid, lo, hi in routes:
            if lo is not None and k < common.bound_ck(lo):
                continue
            if hi is not None and k >= common.bound_ck(hi):
                continue
            return sid
        raise StoreError("INTERNAL", "routing hole: no shard covers key")

    def _checkpoint(self, point):
        if point in self.crash_points:
            sys.stderr.write(f"crash point reached: {point}\n")
            sys.stderr.flush()
            os._exit(1)

    # ------------------------------------------------------------ write path
    def handle_write(self, request_id, wire_ops):
        if not isinstance(request_id, str) or not request_id:
            raise StoreError("BAD_REQUEST", "request_id must be a non-empty string")
        if not isinstance(wire_ops, list):
            raise StoreError("BAD_REQUEST", "ops must be a list")
        ops = []
        for o in wire_ops:
            try:
                ops.append(common.op_from_wire(o))
            except ProtoError as e:
                raise StoreError("BAD_REQUEST", str(e))
        if not ops:
            raise StoreError("BAD_REQUEST", "empty write")
        req_hash = common.request_hash(wire_ops)
        with self.commit_lock:
            c = self._control()
            try:
                row = c.execute(
                    "SELECT request_hash, receipt_json FROM commits WHERE request_id=?",
                    (request_id,)).fetchone()
                if row is not None:
                    if row[0] != req_hash:
                        raise StoreError(
                            "CONFLICT", "request_id reused with a different request")
                    # retransmission of an already-decided request: same receipt,
                    # no re-execution
                    return json.loads(row[1])

                seq = self._meta_int(c, "visible_seq") + 1
                epoch = self._meta_int(c, "epoch")
                routes = self._routes(c, epoch)
                apply_map = {}
                for op in ops:
                    sid = self._route_lookup(routes, op[1], op[2])
                    apply_map.setdefault(sid, []).append(op)
                bump = []
                sp = c.execute(
                    "SELECT source, bt, bk, left_id, right_id FROM splits"
                    " WHERE phase IN ('COPYING','CATCHING_UP')").fetchone()
                if sp is not None:
                    source, bt, bk, left_id, right_id = sp
                    src_route = next(r for r in routes if r[0] == source)
                    lo = common.bound_ck(src_route[1])
                    hi = common.bound_ck(src_route[2])
                    boundary = common.ck(bt, bk)
                    for op in ops:
                        k = common.ck(op[1], op[2])
                        if (lo is None or k >= lo) and (hi is None or k < hi):
                            tgt = left_id if k < boundary else right_id
                            apply_map.setdefault(tgt, []).append(op)
                    bump = [source, left_id, right_id]
                wire_apply = {sid: [common.op_to_wire(op) for op in sos]
                              for sid, sos in apply_map.items()}
                receipt = {"commit_seq": seq, "epoch": epoch,
                           "request_id": request_id, "ops_applied": len(ops)}
                # the durable commit decision
                c.execute(
                    "INSERT INTO commits(seq,request_id,request_hash,receipt_json,"
                    "apply_json,bump_json,applied) VALUES (?,?,?,?,?,?,0)",
                    (seq, request_id, req_hash, json.dumps(receipt),
                     json.dumps(wire_apply), json.dumps(bump)))
                c.commit()
                self._checkpoint("after_decision")
                try:
                    self._apply_to_shards(wire_apply, bump, seq)
                    c.execute("UPDATE commits SET applied=1 WHERE seq=?", (seq,))
                    c.execute("UPDATE meta SET v=? WHERE k='visible_seq'", (str(seq),))
                    c.commit()
                except Exception:
                    # converge via the idempotent recovery path; fail-stop if
                    # even that cannot apply the durable decision
                    try:
                        self._recover_commits()
                    except Exception:
                        traceback.print_exc()
                        os._exit(1)
                    raise
                return receipt
            finally:
                c.close()

    def _apply_to_shards(self, wire_apply, bump, seq):
        for sid, wire_ops in wire_apply.items():
            conn = self._shard_conn(sid)
            try:
                conn.execute("BEGIN IMMEDIATE")
                for o in wire_ops:
                    op = common.op_from_wire(o)
                    if op[0] == "put":
                        _, t, k, (vt, vi, vb) = op
                        conn.execute(
                            "INSERT OR REPLACE INTO kv(tenant,k,seq,op,vt,vi,vb)"
                            " VALUES (?,?,?,'put',?,?,?)", (t, k, seq, vt, vi, vb))
                    else:
                        _, t, k = op
                        conn.execute(
                            "INSERT OR REPLACE INTO kv(tenant,k,seq,op,vt,vi,vb)"
                            " VALUES (?,?,?,'del',NULL,NULL,NULL)", (t, k, seq))
                conn.execute(
                    "INSERT INTO frontier(id,seq) VALUES (1,?) "
                    "ON CONFLICT(id) DO UPDATE SET seq=MAX(seq, excluded.seq)", (seq,))
                conn.commit()
            finally:
                conn.close()
        for sid in bump:
            if sid in wire_apply:
                continue  # already bumped inside its apply transaction
            self._bump_frontier(sid, seq)

    # ------------------------------------------------------------ read path
    def handle_open_read(self):
        with self.commit_lock:
            c = self._control()
            try:
                seq = self._meta_int(c, "visible_seq")
                epoch = self._meta_int(c, "epoch")
                token = "rt" + secrets.token_hex(12)
                c.execute("INSERT INTO tokens(token_id,epoch,read_seq,released)"
                          " VALUES (?,?,?,0)", (token, epoch, seq))
                c.commit()
                return {"token": token, "read_seq": seq, "epoch": epoch}
            finally:
                c.close()

    def _token_begin(self, token_id):
        """Validate a token and register an in-flight read on it."""
        c = self._control()
        try:
            row = c.execute(
                "SELECT epoch, read_seq, released FROM tokens WHERE token_id=?",
                (token_id,)).fetchone()
        finally:
            c.close()
        if row is None or row[2]:
            raise StoreError("BAD_TOKEN", "unknown or released read token")
        epoch, read_seq = row[0], row[1]
        with self.inflight_lock:
            ent = self.inflight.get(token_id)
            if ent is None:
                self.inflight[token_id] = [epoch, read_seq, 1]
            else:
                ent[2] += 1
        return epoch, read_seq

    def _token_end(self, token_id):
        with self.inflight_lock:
            ent = self.inflight.get(token_id)
            if ent is not None:
                ent[2] -= 1
                if ent[2] <= 0:
                    del self.inflight[token_id]

    def _token_routes(self, token_id):
        epoch, read_seq = self._token_begin(token_id)
        c = self._control()
        try:
            routes = self._routes(c, epoch)
        finally:
            c.close()
        return epoch, read_seq, routes

    def handle_get(self, token_id, tenant, key):
        common.check_tenant(tenant)
        common.check_i64(key, "key")
        epoch, read_seq, routes = self._token_routes(token_id)
        try:
            sid = self._route_lookup(routes, tenant, key)
            conn = self._shard_conn(sid)
            try:
                row = conn.execute(
                    "SELECT op, vt, vi, vb FROM kv"
                    " WHERE tenant=? AND k=? AND seq<=?"
                    " ORDER BY seq DESC LIMIT 1", (tenant, key, read_seq)).fetchone()
            finally:
                conn.close()
        finally:
            self._token_end(token_id)
        if row is None or row[0] == "del":
            return {"found": False}
        return {"found": True, "value": common.value_to_wire((row[1], row[2], row[3]))}

    def handle_scan(self, token_id, lo, hi, cursor, limit):
        if limit < 1 or limit > 1000000:
            raise StoreError("BAD_REQUEST", "limit out of range")
        epoch, read_seq, routes = self._token_routes(token_id)
        try:
            out = []
            for sid, rlo, rhi in routes:
                if len(out) > limit:
                    break
                if (cursor is not None and rhi is not None
                        and common.bound_ck(cursor) >= common.bound_ck(rhi)):
                    continue  # whole route is behind the cursor
                eff_lo = _max_bound(lo, rlo)
                eff_hi = _min_bound(hi, rhi)
                if (eff_lo is not None and eff_hi is not None
                        and common.bound_ck(eff_lo) >= common.bound_ck(eff_hi)):
                    continue  # no overlap
                exclusive = False
                if cursor is not None and (eff_lo is None
                                           or common.bound_ck(cursor)
                                           >= common.bound_ck(eff_lo)):
                    eff_lo = cursor
                    exclusive = True
                rows = self._shard_scan(sid, read_seq, eff_lo, exclusive, eff_hi,
                                        limit + 1 - len(out))
                out.extend(rows)
        finally:
            self._token_end(token_id)
        more = len(out) > limit
        out = out[:limit]
        next_cursor = None
        if out and more:
            next_cursor = {"tenant": out[-1][0], "key": str(out[-1][1])}
        rows_wire = [{"tenant": t, "key": str(k), "value": common.value_to_wire(v)}
                     for (t, k, v) in out]
        return {"rows": rows_wire, "next_cursor": next_cursor, "done": not more}

    def _shard_scan(self, shard_id, read_seq, lo, lo_exclusive, hi, limit):
        conn = self._shard_conn(shard_id)
        try:
            sql = ("SELECT tenant, k, vt, vi, vb FROM ("
                   "SELECT tenant, k, MAX(seq) AS mseq, op, vt, vi, vb FROM kv"
                   " WHERE seq <= ?")
            params = [read_seq]
            if lo is not None:
                sql += " AND (tenant, k) > (?, ?)" if lo_exclusive \
                    else " AND (tenant, k) >= (?, ?)"
                params.extend([lo[0], lo[1]])
            if hi is not None:
                sql += " AND (tenant, k) < (?, ?)"
                params.extend([hi[0], hi[1]])
            sql += " GROUP BY tenant, k) WHERE op = 'put' ORDER BY tenant, k LIMIT ?"
            params.append(limit)
            return [(t, k, (vt, vi, vb))
                    for (t, k, vt, vi, vb) in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def handle_release(self, token_id):
        with self.commit_lock:
            c = self._control()
            try:
                row = c.execute("SELECT released FROM tokens WHERE token_id=?",
                                (token_id,)).fetchone()
                if row is None:
                    raise StoreError("BAD_TOKEN", "unknown read token")
                if not row[0]:
                    c.execute("UPDATE tokens SET released=1 WHERE token_id=?",
                              (token_id,))
                    c.commit()
            finally:
                c.close()
        self._gc_pass()
        return {"released": True}

    def _gc_pass(self):
        """Drop row versions no active read point or split copy can still see."""
        with self.commit_lock:
            c = self._control()
            try:
                pins = [self._meta_int(c, "visible_seq")]
                pins += [r[0] for r in c.execute(
                    "SELECT read_seq FROM tokens WHERE released=0").fetchall()]
                pins += [r[0] for r in c.execute(
                    "SELECT copy_seq FROM splits WHERE phase IN ('COPYING','CATCHING_UP')"
                ).fetchall()]
            finally:
                c.close()
            with self.inflight_lock:
                pins += [ent[1] for ent in self.inflight.values()]
            p = min(pins)
            c = self._control()
            try:
                shards = [r[0] for r in c.execute(
                    "SELECT shard_id FROM shards WHERE state != 'reclaimed'").fetchall()]
            finally:
                c.close()
            for sid in shards:
                try:
                    conn = self._shard_conn(sid)
                    try:
                        conn.execute(
                            "DELETE FROM kv WHERE seq < ? AND (tenant, k, seq) NOT IN ("
                            "  SELECT tenant, k, MAX(seq) FROM kv WHERE seq <= ?"
                            "  GROUP BY tenant, k)", (p, p))
                        conn.commit()
                    finally:
                        conn.close()
                except Exception:
                    traceback.print_exc()

    # ------------------------------------------------------------ split
    def handle_split(self, shard_id, bt, bk):
        try:
            common.check_tenant(bt)
            common.check_i64(bk, "boundary key")
        except ProtoError as e:
            raise StoreError("BAD_REQUEST", str(e))
        with self.commit_lock:
            c = self._control()
            try:
                row = c.execute(
                    "SELECT split_id FROM splits"
                    " WHERE phase IN ('COPYING','CATCHING_UP')").fetchone()
                if row is not None:
                    raise StoreError("SPLIT_IN_PROGRESS",
                                     "another split is already in progress")
                epoch = self._meta_int(c, "epoch")
                routes = self._routes(c, epoch)
                src = next((r for r in routes if r[0] == shard_id), None)
                if src is None:
                    raise StoreError("UNKNOWN_SHARD",
                                     f"shard {shard_id!r} not in current routing")
                bkey = common.ck(bt, bk)
                lo = common.bound_ck(src[1])
                hi = common.bound_ck(src[2])
                if lo is not None and bkey <= lo:
                    raise StoreError(
                        "BAD_BOUNDARY",
                        "boundary not strictly inside the shard range")
                if hi is not None and bkey >= hi:
                    raise StoreError(
                        "BAD_BOUNDARY",
                        "boundary not strictly inside the shard range")
                counter = self._meta_int(c, "shard_counter")
                left_id, right_id = f"s{counter + 1}", f"s{counter + 2}"
                split_id = "sp" + secrets.token_hex(8)
                copy_seq = self._meta_int(c, "visible_seq")
                # create the target files before registering the split
                self._create_shard(left_id)
                self._create_shard(right_id)
                c.execute("UPDATE meta SET v=? WHERE k='shard_counter'",
                          (str(counter + 2),))
                c.execute("INSERT INTO shards(shard_id,state) VALUES (?,'copying')",
                          (left_id,))
                c.execute("INSERT INTO shards(shard_id,state) VALUES (?,'copying')",
                          (right_id,))
                c.execute("UPDATE shards SET state='splitting' WHERE shard_id=?",
                          (shard_id,))
                c.execute(
                    "INSERT INTO splits(split_id,source,bt,bk,left_id,right_id,"
                    "copy_seq,cur_t,cur_k,copy_done,phase,new_epoch)"
                    " VALUES (?,?,?,?,?,?,?,NULL,NULL,0,'COPYING',NULL)",
                    (split_id, shard_id, bt, bk, left_id, right_id, copy_seq))
                c.commit()
            finally:
                c.close()
            # bring the source frontier up to the copy point (outside the
            # control transaction; recovery repeats it if we die here)
            self._bump_frontier(shard_id, copy_seq)
            self._start_copy_worker(split_id)
            return {"split_id": split_id, "left": left_id, "right": right_id,
                    "copy_seq": copy_seq}

    def _start_copy_worker(self, split_id):
        t = threading.Thread(target=self._copy_worker, args=(split_id,),
                             name=f"copy-{split_id}", daemon=True)
        t.start()

    def _copy_worker(self, split_id):
        try:
            while not self.shutdown_flag.is_set():
                if self.copy_paused.is_set():
                    time.sleep(0.02)
                    continue
                c = self._control()
                try:
                    sp = c.execute(
                        "SELECT source, bt, bk, left_id, right_id, copy_seq,"
                        " cur_t, cur_k, phase FROM splits WHERE split_id=?",
                        (split_id,)).fetchone()
                finally:
                    c.close()
                if sp is None or sp[8] not in ACTIVE_SPLIT_PHASES:
                    return
                source, bt, bk, left_id, right_id, copy_seq, cur_t, cur_k, phase = sp
                if phase == "COPYING":
                    cursor = None if cur_t is None else (cur_t, cur_k)
                    batch = self._copy_batch(source, cursor, copy_seq)
                    if batch:
                        self._apply_copy_batch(batch, bt, bk, left_id, right_id)
                        last = batch[-1]
                        c = self._control()
                        try:
                            c.execute("UPDATE splits SET cur_t=?, cur_k=?"
                                      " WHERE split_id=?",
                                      (last[0], last[1], split_id))
                            c.commit()
                        finally:
                            c.close()
                        self._checkpoint("during_copy")
                        continue
                    # bulk copy finished: targets now hold everything <= copy_seq
                    with self.commit_lock:
                        c = self._control()
                        try:
                            c.execute("UPDATE splits SET copy_done=1,"
                                      " phase='CATCHING_UP' WHERE split_id=?",
                                      (split_id,))
                            c.commit()
                        finally:
                            c.close()
                        self._bump_frontier(left_id, copy_seq)
                        self._bump_frontier(right_id, copy_seq)
                    self._checkpoint("after_copy")
                    phase = "CATCHING_UP"
                # CATCHING_UP: both targets must hold the same complete commit
                # frontier as the source before the routing may switch
                self._checkpoint("after_catchup")
                with self.commit_lock:
                    self._checkpoint("before_publish")
                    self._publish(split_id)
                self._checkpoint("after_publish")
                return
        except Exception:
            traceback.print_exc()
            # state is persisted; a restart resumes (or abandons) safely

    def _copy_batch(self, source, cursor, copy_seq):
        conn = self._shard_conn(source)
        try:
            sql = ("SELECT tenant, k, mseq, vt, vi, vb FROM ("
                   "SELECT tenant, k, MAX(seq) AS mseq, op, vt, vi, vb FROM kv"
                   " WHERE seq <= ?")
            params = [copy_seq]
            if cursor is not None:
                sql += " AND (tenant, k) > (?, ?)"
                params.extend([cursor[0], cursor[1]])
            sql += (" GROUP BY tenant, k) WHERE op = 'put'"
                    " ORDER BY tenant, k LIMIT ?")
            params.append(self.batch_size)
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def _apply_copy_batch(self, batch, bt, bk, left_id, right_id):
        boundary = common.ck(bt, bk)
        left_rows = [r for r in batch if common.ck(r[0], r[1]) < boundary]
        right_rows = [r for r in batch if common.ck(r[0], r[1]) >= boundary]
        for sid, rows in ((left_id, left_rows), (right_id, right_rows)):
            if not rows:
                continue
            conn = self._shard_conn(sid)
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    "INSERT OR REPLACE INTO kv(tenant,k,seq,op,vt,vi,vb)"
                    " VALUES (?,?,?,'put',?,?,?)",
                    [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows])
                conn.commit()
            finally:
                conn.close()

    def _publish(self, split_id):
        """Atomically switch the routing epoch.  Caller holds commit_lock."""
        c = self._control()
        try:
            sp = c.execute(
                "SELECT source, bt, bk, left_id, right_id, phase FROM splits"
                " WHERE split_id=?", (split_id,)).fetchone()
            if sp is None or sp[5] != "CATCHING_UP":
                return
            source, bt, bk, left_id, right_id, _ = sp
            visible = self._meta_int(c, "visible_seq")
            now = {sid: self._frontier(sid)
                   for sid in (source, left_id, right_id)}
            if not (now[source] == now[left_id] == now[right_id] == visible):
                raise StoreError(
                    "INTERNAL",
                    f"frontier mismatch at publish: {now} visible={visible}")
            epoch = self._meta_int(c, "epoch")
            new_epoch = epoch + 1
            routes = self._routes(c, epoch)
            for sid, lo, hi in routes:
                if sid == source:
                    continue
                c.execute(
                    "INSERT INTO epochs(epoch,shard_id,lo_t,lo_k,hi_t,hi_k)"
                    " VALUES (?,?,?,?,?,?)",
                    (new_epoch, sid,
                     lo[0] if lo else None, lo[1] if lo else None,
                     hi[0] if hi else None, hi[1] if hi else None))
            src = next(r for r in routes if r[0] == source)
            c.execute(
                "INSERT INTO epochs(epoch,shard_id,lo_t,lo_k,hi_t,hi_k)"
                " VALUES (?,?,?,?,?,?)",
                (new_epoch, left_id,
                 src[1][0] if src[1] else None, src[1][1] if src[1] else None,
                 bt, bk))
            c.execute(
                "INSERT INTO epochs(epoch,shard_id,lo_t,lo_k,hi_t,hi_k)"
                " VALUES (?,?,?,?,?,?)",
                (new_epoch, right_id, bt, bk,
                 src[2][0] if src[2] else None, src[2][1] if src[2] else None))
            c.execute("UPDATE meta SET v=? WHERE k='epoch'", (str(new_epoch),))
            c.execute("UPDATE shards SET state='retired' WHERE shard_id=?", (source,))
            c.execute("UPDATE shards SET state='active' WHERE shard_id IN (?,?)",
                      (left_id, right_id))
            c.execute("UPDATE splits SET phase='PUBLISHED', new_epoch=?"
                      " WHERE split_id=?", (new_epoch, split_id))
            c.commit()
        finally:
            c.close()

    # ------------------------------------------------------------ reclaim
    def handle_reclaim(self):
        report = {"reclaimed": [], "blocked": []}
        with self.commit_lock:
            c = self._control()
            try:
                retired = [r[0] for r in c.execute(
                    "SELECT shard_id FROM shards WHERE state='retired'").fetchall()]
                token_epochs = {r[0] for r in c.execute(
                    "SELECT DISTINCT epoch FROM tokens WHERE released=0").fetchall()}
                with self.inflight_lock:
                    token_epochs |= {ent[0] for ent in self.inflight.values()}
                for sid in retired:
                    epochs_with_s = {r[0] for r in c.execute(
                        "SELECT epoch FROM epochs WHERE shard_id=?",
                        (sid,)).fetchall()}
                    if token_epochs & epochs_with_s:
                        report["blocked"].append(sid)
                        continue
                    c.execute("UPDATE shards SET state='reclaiming' WHERE shard_id=?",
                              (sid,))
                    c.execute("UPDATE splits SET phase='RECLAIMING'"
                              " WHERE source=? AND phase='PUBLISHED'", (sid,))
                    c.commit()
                    self._checkpoint("during_reclaim")
                    self._delete_shard_files(self._shard_path(sid))
                    c.execute("UPDATE shards SET state='reclaimed' WHERE shard_id=?",
                              (sid,))
                    c.execute("UPDATE splits SET phase='RECLAIMED'"
                              " WHERE source=? AND phase='RECLAIMING'", (sid,))
                    c.commit()
                    report["reclaimed"].append(sid)
            finally:
                c.close()
        self._gc_pass()
        return report

    # ------------------------------------------------------------ status
    def handle_status(self):
        with self.commit_lock:
            c = self._control()
            try:
                epoch = self._meta_int(c, "epoch")
                visible = self._meta_int(c, "visible_seq")
                sp = c.execute(
                    "SELECT split_id, source, bt, bk, left_id, right_id, copy_seq,"
                    " cur_t, cur_k, copy_done, phase, new_epoch FROM splits"
                    " ORDER BY rowid DESC LIMIT 1").fetchone()
                shards = c.execute("SELECT shard_id, state FROM shards"
                                   " ORDER BY shard_id").fetchall()
                tokens_active = c.execute(
                    "SELECT COUNT(*) FROM tokens WHERE released=0").fetchone()[0]
            finally:
                c.close()
        frontiers = {}
        for sid, state in shards:
            if state == "reclaimed":
                frontiers[sid] = None
                continue
            try:
                frontiers[sid] = self._frontier(sid)
            except Exception:
                frontiers[sid] = None
        split_info = None
        if sp is not None:
            split_info = {
                "split_id": sp[0], "source": sp[1],
                "boundary": {"tenant": sp[2], "key": str(sp[3])},
                "left": sp[4], "right": sp[5], "copy_seq": sp[6],
                "cursor": None if sp[7] is None else {"tenant": sp[7],
                                                      "key": str(sp[8])},
                "copy_done": bool(sp[9]), "phase": sp[10], "new_epoch": sp[11],
            }
        return {
            "epoch": epoch, "visible_seq": visible,
            "split": split_info,
            "shards": [{"shard_id": sid, "state": state, "frontier": frontiers[sid]}
                       for sid, state in shards],
            "active_tokens": tokens_active,
        }

    def handle_routing(self):
        with self.commit_lock:
            c = self._control()
            try:
                epoch = self._meta_int(c, "epoch")
                routes = self._routes(c, epoch)
                states = dict(c.execute("SELECT shard_id, state FROM shards").fetchall())
            finally:
                c.close()

        def enc_bound(b):
            return None if b is None else {"tenant": b[0], "key": str(b[1])}

        return {
            "epoch": epoch,
            "routes": [{"shard": sid, "lo": enc_bound(lo), "hi": enc_bound(hi),
                        "state": states.get(sid)} for sid, lo, hi in routes],
        }

    # ------------------------------------------------------------ test hooks
    def handle_test_arm_crash(self, point):
        if point not in CRASH_POINTS:
            raise StoreError("BAD_REQUEST", f"unknown crash point {point!r}")
        self.crash_points.add(point)
        return {"armed": point}

    def handle_test_pause_copy(self, on):
        if on:
            self.copy_paused.set()
        else:
            self.copy_paused.clear()
        return {"paused": bool(on)}

    def handle_shutdown(self):
        self.shutdown_flag.set()
        return {"bye": True}

    # ------------------------------------------------------------ serving
    def _dispatch(self, req):
        op = req.get("op")
        args = req.get("args") or {}
        if op == "ping":
            return {"pong": True}
        if op == "write":
            return self.handle_write(args["request_id"], args["ops"])
        if op == "open_read":
            return self.handle_open_read()
        if op == "get":
            return self.handle_get(args["token"], args["tenant"], int(args["key"]))
        if op == "scan":
            return self.handle_scan(args["token"], _parse_bound(args.get("lo")),
                                    _parse_bound(args.get("hi")),
                                    _parse_bound(args.get("cursor")),
                                    int(args.get("limit", 1000)))
        if op == "release":
            return self.handle_release(args["token"])
        if op == "split":
            b = args["boundary"]
            return self.handle_split(args["shard_id"], b["tenant"], int(b["key"]))
        if op == "migration_status":
            return self.handle_status()
        if op == "routing":
            return self.handle_routing()
        if op == "reclaim":
            return self.handle_reclaim()
        if op == "shutdown":
            return self.handle_shutdown()
        if op == "test_arm_crash":
            return self.handle_test_arm_crash(args["point"])
        if op == "test_pause_copy":
            return self.handle_test_pause_copy(bool(args["on"]))
        raise StoreError("BAD_REQUEST", f"unknown op {op!r}")

    def _handle_conn(self, conn):
        try:
            while True:
                req = common.recv_msg(conn)
                try:
                    result = self._dispatch(req)
                    common.send_msg(conn, {"ok": True, "result": result})
                except StoreError as e:
                    common.send_msg(conn, {"ok": False, "error": {
                        "code": e.code, "message": e.message}})
                except (ProtoError, KeyError, TypeError, ValueError) as e:
                    common.send_msg(conn, {"ok": False, "error": {
                        "code": "BAD_REQUEST", "message": str(e)}})
                except Exception as e:  # noqa: BLE001 - report, keep serving
                    traceback.print_exc()
                    common.send_msg(conn, {"ok": False, "error": {
                        "code": "INTERNAL", "message": repr(e)}})
        except (EOFError, ConnectionError, ProtoError, OSError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def serve(self, sock_path):
        if os.path.exists(sock_path):
            os.unlink(sock_path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(sock_path)
        srv.listen(128)
        srv.settimeout(0.2)
        try:
            while not self.shutdown_flag.is_set():
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle_conn, args=(conn,),
                                 daemon=True).start()
        finally:
            srv.close()
            try:
                os.unlink(sock_path)
            except OSError:
                pass


def _parse_bound(b):
    if b is None:
        return None
    return (b["tenant"], int(b["key"]))


def main(argv=None):
    ap = argparse.ArgumentParser(description="shardstore server")
    ap.add_argument("--dir", required=True, help="data directory")
    ap.add_argument("--sock", required=True, help="unix socket path")
    ap.add_argument("--batch-size", type=int, default=500,
                    help="rows copied per split batch")
    args = ap.parse_args(argv)
    server = Server(args.dir, batch_size=args.batch_size)

    def _term(*_):
        server.shutdown_flag.set()

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    server.serve(args.sock)
    return 0


if __name__ == "__main__":
    sys.exit(main())
