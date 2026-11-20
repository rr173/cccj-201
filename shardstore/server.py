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
import datetime
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
  applied INTEGER NOT NULL DEFAULT 0,
  deliver_json TEXT,
  payload_live INTEGER NOT NULL DEFAULT 1,
  prior_json TEXT
);
CREATE TABLE IF NOT EXISTS subscriptions (
  name TEXT PRIMARY KEY,
  origin_seq INTEGER NOT NULL,
  acked_seq INTEGER NOT NULL,
  delivered_seq INTEGER NOT NULL,
  generation INTEGER NOT NULL DEFAULT 1,
  paused INTEGER NOT NULL DEFAULT 0
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
-- when an epoch became the live routing epoch (used to pick the epoch that
-- was active at a historical commit seq for view snapshots)
CREATE TABLE IF NOT EXISTS epoch_meta (
  epoch INTEGER PRIMARY KEY,
  active_from_seq INTEGER NOT NULL
);
-- persistent grouped aggregate views: a definition + one row per build
-- generation.  The active generation pointer only ever moves to a fully
-- built generation, so a query never sees a half-built view.
CREATE TABLE IF NOT EXISTS views (
  name TEXT PRIMARY KEY,
  active_gen INTEGER NOT NULL,
  start_seq INTEGER NOT NULL,
  created_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS view_generations (
  view TEXT NOT NULL,
  gen INTEGER NOT NULL,
  start_seq INTEGER NOT NULL,
  epoch INTEGER NOT NULL,
  pin_token TEXT,
  scan_done INTEGER NOT NULL DEFAULT 0,
  scan_t TEXT, scan_k INTEGER,
  agg_seq INTEGER NOT NULL,
  phase TEXT NOT NULL,
  error TEXT,
  created_seq INTEGER NOT NULL,
  PRIMARY KEY (view, gen)
);
-- append-only cumulative aggregate versions.  A group's value "as of seq W"
-- is its row with the greatest seq <= W; cnt=0 means the group is empty and
-- queries hide it.  The sum is an arbitrary-precision decimal string, never a
-- SQLite integer, so totals beyond signed 64 bits stay exact.
CREATE TABLE IF NOT EXISTS view_groups (
  view TEXT NOT NULL,
  gen INTEGER NOT NULL,
  tenant TEXT NOT NULL COLLATE BINARY,
  txt_null INTEGER NOT NULL,
  txt TEXT NOT NULL COLLATE BINARY,
  seq INTEGER NOT NULL,
  cnt INTEGER NOT NULL,
  ncnt INTEGER NOT NULL,
  total TEXT NOT NULL,
  PRIMARY KEY (view, gen, tenant, txt_null, txt, seq)
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
    "during_pull",      # subscription: after the delivery cursor advanced
    "during_ack",       # subscription: after writing the confirmed prefix
    "during_cleanup",   # subscription log: after the boundary check, before GC
    # snapshot export / restore crash window points (test hooks)
    "during_snapshot_segments",   # export: after a data segment batch/rotation
    "before_snapshot_manifest",   # export: segments durable, before manifest
    "after_snapshot_manifest",    # export: manifest durable, before publish
    "during_restore_verify",      # restore: during full backup verification
    "during_restore_build",       # restore: after a target shard build batch
    "before_restore_publish",     # restore: build done, before atomic publish
    "after_restore_publish",      # restore: just after the atomic publish
    # grouped view crash window points (test hooks)
    "during_view_build",          # view: after a source scan/undo batch
    "during_view_fold",           # view: after a fold batch into the aggregate
    "after_view_scan",            # view: scan done, before catch-up publish
    "during_view_catchup",        # view: after a catch-up aggregate batch
    "before_view_publish",        # view: aggregate complete, before the swap
    "after_view_publish",         # view: just after the atomic generation swap
}

ACTIVE_SPLIT_PHASES = ("COPYING", "CATCHING_UP")


class StoreError(Exception):
    def __init__(self, code, message, detail=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail


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
        self._view_workers = set()                 # (view, gen) with a thread
        self.view_fail_armed = False               # test hook (in-memory)
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
            c.execute("PRAGMA journal_mode=WAL")
            cols = {r[1] for r in c.execute("PRAGMA table_info(commits)").fetchall()}
            if "deliver_json" not in cols:
                c.execute("ALTER TABLE commits ADD COLUMN deliver_json TEXT")
            if "payload_live" not in cols:
                c.execute("ALTER TABLE commits ADD COLUMN payload_live"
                          " INTEGER NOT NULL DEFAULT 1")
                # commits from an older build never had a change payload;
                # they are outside the deliverable range, not a hole in it
                c.execute("UPDATE commits SET payload_live=0"
                          " WHERE deliver_json IS NULL")
            if "prior_json" not in cols:
                # per-key before-values captured at commit time, used to seed
                # a grouped view from a historical commit seq without relying
                # on MVCC retention.  Default NULL = unknown for old rows.
                c.execute("ALTER TABLE commits ADD COLUMN prior_json TEXT")
            c.execute("CREATE INDEX IF NOT EXISTS ix_commits_live"
                      " ON commits(payload_live, seq)")
            c.execute("CREATE INDEX IF NOT EXISTS ix_commits_applied"
                      " ON commits(applied)")
            # epoch activation history: the initial (or restored) epoch has
            # been live since sequence 0
            if c.execute("SELECT COUNT(*) FROM epoch_meta").fetchone()[0] == 0:
                epoch = int(c.execute(
                    "SELECT v FROM meta WHERE k='epoch'").fetchone()[0])
                c.execute("INSERT OR IGNORE INTO epoch_meta(epoch,active_from_seq)"
                          " VALUES (?,0)", (epoch,))
            c.commit()
        finally:
            c.close()

    # ------------------------------------------------------ subscription log
    # The retained change log is the set of commits with payload_live=1.
    # Its lower bound is derived from that set, never stored separately, so a
    # cleanup can never leave a partially visible range: payload release and
    # retained-range movement are the same transaction.
    def _log_floor(self, c):
        """First seq that must still be deliverable from the retained log."""
        row = c.execute("SELECT MIN(seq) FROM commits WHERE payload_live=1").fetchone()
        if row is not None and row[0] is not None:
            return int(row[0])
        # empty log: anything after the committed end is a stable position
        row = c.execute("SELECT MAX(seq) FROM commits").fetchone()
        high = 0 if row is None or row[0] is None else int(row[0])
        return max(self._meta_int(c, "visible_seq"), high) + 1

    @staticmethod
    def _write_watermark(request_id):
        """Stable logical position of a request inside the change stream
        (request_id dedup makes it unique)."""
        return "r:" + request_id

    def _deliver_view(self, c, seq):
        """The stored change payload of commit `seq`, or None if it is no
        longer retained.  Only fully applied commits are ever deliverable:
        a decided-but-unapplied commit is never handed to a subscriber."""
        row = c.execute(
            "SELECT deliver_json, payload_live, applied FROM commits WHERE seq=?",
            (seq,)).fetchone()
        if row is None or not row[1] or row[0] is None:
            return None
        if not row[2]:
            raise StoreError(
                "INTERNAL",
                f"commit {seq} is decided but not yet applied; it must not be"
                f" delivered")
        return json.loads(row[0])

    def _payload_size(self, view):
        canon = json.dumps(view, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False)
        return len(canon.encode("utf-8")), canon

    def _collect_changes(self, c, start_after, stop_at, max_commits, max_bytes):
        """Ordered, non-splittable commits from `start_after`+1 through
        `stop_at`.  A single commit bigger than max_bytes is returned whole
        and flagged; no commit is ever cut into pieces or merged with
        another."""
        items = []
        used = 0
        over_budget = False
        oversize_seq = None
        seq = start_after + 1
        while seq <= stop_at:
            if len(items) >= max_commits:
                break
            view = self._deliver_view(c, seq)
            if view is None:
                floor = self._log_floor(c)
                raise StoreError(
                    "OUT_OF_RANGE",
                    f"change payload for commit {seq} is no longer retained"
                    f" (retained log starts at {floor})",
                    {"log_low": floor, "log_high": self._meta_int(c, "visible_seq"),
                     "min_after": floor - 1})
            size, _ = self._payload_size(view)
            if items and used + size > max_bytes:
                break
            oversized = size > max_bytes
            if not items and oversized:
                over_budget = True
                oversize_seq = seq
            items.append({
                "commit_seq": seq,
                "request_id": view["request_id"],
                "epoch": view["epoch"],
                "watermark": view.get("watermark"),
                "ops": view["ops"],
                "size_bytes": size,
                "over_budget": oversized,
            })
            used += size
            seq += 1
        last = start_after if not items else items[-1]["commit_seq"]
        return items, used, over_budget, oversize_seq, last

    def _stream_bounds(self, c):
        """(min_after, max_after) that a new subscription or replay may name."""
        return (self._log_floor(c) - 1, self._meta_int(c, "visible_seq"))

    def _subscription_row(self, c, name):
        return c.execute(
            "SELECT origin_seq, acked_seq, delivered_seq, generation, paused"
            " FROM subscriptions WHERE name=?", (name,)).fetchone()

    def _sub_state(self, c, name, row=None):
        if row is None:
            row = self._subscription_row(c, name)
        if row is None:
            raise StoreError("NO_SUBSCRIPTION", f"unknown subscription {name!r}")
        origin, acked, delivered, generation, paused = row
        visible = self._meta_int(c, "visible_seq")
        log_low = self._log_floor(c)
        return {
            "name": name, "origin_seq": origin, "acked_seq": acked,
            "delivered_seq": delivered, "generation": generation,
            "paused": bool(paused),
            "log_low": log_low, "log_high": visible,
            "min_after": log_low - 1, "max_after": visible,
            "backlog_commits": max(0, delivered - acked),
            "available_commits": max(0, visible - acked),
            "deliverable_after": acked,
            "replay_after_min": max(origin, log_low - 1),
            "replay_after_max": delivered,
        }

    # ------------------------------------------------------ subscriptions
    def handle_subscribe(self, name, after_seq):
        """Register a subscription starting strictly after committed seq
        `after_seq`, then take its consumption generation.

        Registration is how a process announces itself.  A new name gets
        generation 1.  An existing name with a different after_seq is
        refused: progress is never silently rewound.  An existing name with
        the same after_seq keeps its cursor and confirmed prefix, but a new
        process taking over starts a new generation, so a delayed ack from an
        older process can no longer advance the cursor.  To re-attach without
        a takeover of an in-flight consumer, use `subscribe_status`."""
        if not isinstance(name, str) or not name:
            raise StoreError("BAD_REQUEST", "subscription name must be non-empty")
        if not isinstance(after_seq, int) or isinstance(after_seq, bool):
            raise StoreError("BAD_REQUEST", "after_seq must be an integer")
        with self.commit_lock:
            c = self._control()
            try:
                row = self._subscription_row(c, name)
                if row is not None:
                    if after_seq != row[0]:
                        raise StoreError(
                            "SUBSCRIPTION_EXISTS",
                            f"subscription {name!r} already exists with"
                            f" origin_seq {row[0]}; refusing to reset it")
                    generation = row[3] + 1
                    c.execute("UPDATE subscriptions SET generation=? WHERE name=?",
                              (generation, name))
                    c.commit()
                    st = self._sub_state(c, name)
                    st.update({"created": False, "claimed": True,
                               "generation": generation})
                    return st
                lo, hi = self._stream_bounds(c)
                if after_seq < lo or after_seq > hi:
                    raise StoreError(
                        "OUT_OF_RANGE",
                        f"after_seq {after_seq} outside the retained log"
                        f" [{lo}, {hi}]",
                        {"min_after": lo, "max_after": hi,
                         "log_low": self._log_floor(c), "log_high": hi})
                c.execute(
                    "INSERT INTO subscriptions(name,origin_seq,acked_seq,"
                    "delivered_seq,generation,paused) VALUES (?,?,?,?,1,0)",
                    (name, after_seq, after_seq, after_seq))
                c.commit()
                st = self._sub_state(c, name)
                st.update({"created": True, "claimed": False})
                return st
            finally:
                c.close()

    def handle_claim(self, name):
        """Take over consumption of an existing subscription: bump the
        generation, invalidating every older one.  Delayed pulls and acks
        from an older generation are rejected; the cursor itself is kept."""
        with self.commit_lock:
            c = self._control()
            try:
                row = self._subscription_row(c, name)
                if row is None:
                    raise StoreError("NO_SUBSCRIPTION", f"unknown subscription {name!r}")
                generation = row[3] + 1
                c.execute("UPDATE subscriptions SET generation=? WHERE name=?",
                          (generation, name))
                c.commit()
                st = self._sub_state(c, name)
                st.update({"claimed": True, "generation": generation})
                return st
            finally:
                c.close()

    @staticmethod
    def _require_generation(row, generation, name):
        if generation is None or isinstance(generation, bool) \
                or not isinstance(generation, int):
            raise StoreError("BAD_REQUEST", "generation is required")
        if generation != row[3]:
            raise StoreError(
                "STALE_GENERATION",
                f"subscription {name!r} is at generation {row[3]},"
                f" not {generation}")
        return generation

    def handle_pull(self, name, generation=None, max_commits=100,
                    max_bytes=1024 * 1024):
        if max_commits is None:
            max_commits = 100
        if max_bytes is None:
            max_bytes = 1024 * 1024
        if not isinstance(max_commits, int) or isinstance(max_commits, bool) \
                or max_commits < 1:
            raise StoreError("BAD_REQUEST", "max_commits must be >= 1")
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) \
                or max_bytes < 1:
            raise StoreError("BAD_REQUEST", "max_bytes must be >= 1")
        with self.commit_lock:
            c = self._control()
            try:
                row = self._subscription_row(c, name)
                if row is None:
                    raise StoreError("NO_SUBSCRIPTION", f"unknown subscription {name!r}")
                self._require_generation(row, generation, name)
                origin, acked, delivered, gen, paused = row
                visible = self._meta_int(c, "visible_seq")
                if paused:
                    st = self._sub_state(c, name, row)
                    st.update({"items": [], "next_after": acked,
                               "has_more": False, "over_budget": False,
                               "oversize_seq": None, "bytes": 0,
                               "delivered_seq": delivered})
                    return st
                # delivery always resumes from the confirmed prefix, so a
                # repeated pull (same acked_seq) yields exactly the same
                # commits and bytes
                items, used, over_budget, oversize_seq, last = self._collect_changes(
                    c, acked, visible, max_commits, max_bytes)
                has_more = last < visible
                if items and last > delivered:
                    c.execute("UPDATE subscriptions SET delivered_seq=? WHERE name=?",
                              (last, name))
                    c.commit()
                self._checkpoint("during_pull")
                st = self._sub_state(c, name)
                st.update({
                    "items": items, "next_after": last, "has_more": has_more,
                    "over_budget": over_budget, "oversize_seq": oversize_seq,
                    "bytes": used,
                })
                return st
            finally:
                c.close()

    def handle_ack(self, name, through_seq, generation):
        """Confirm the contiguous, fully processed prefix ending at
        through_seq.  Retries are idempotent; moving backwards, skipping past
        what was delivered, or using a stale generation is refused without
        changing the cursor."""
        if not isinstance(through_seq, int) or isinstance(through_seq, bool):
            raise StoreError("BAD_REQUEST", "through_seq must be an integer")
        with self.commit_lock:
            c = self._control()
            try:
                row = self._subscription_row(c, name)
                if row is None:
                    raise StoreError("NO_SUBSCRIPTION", f"unknown subscription {name!r}")
                self._require_generation(row, generation, name)
                origin, acked, delivered, gen, paused = row
                if through_seq > delivered:
                    raise StoreError(
                        "BAD_ACK",
                        f"cannot confirm {through_seq}: only the contiguous"
                        f" delivered prefix through {delivered} exists")
                if through_seq < acked:
                    raise StoreError(
                        "BAD_ACK",
                        f"cannot move the confirmed prefix backwards from"
                        f" {acked} to {through_seq}")
                if through_seq > acked:
                    c.execute("UPDATE subscriptions SET acked_seq=? WHERE name=?",
                              (through_seq, name))
                    c.commit()
                self._checkpoint("during_ack")
                st = self._sub_state(c, name)
                st.update({"acked": through_seq, "advanced": through_seq > acked})
                return st
            finally:
                c.close()

    def handle_set_paused(self, name, paused, generation=None):
        with self.commit_lock:
            c = self._control()
            try:
                row = self._subscription_row(c, name)
                if row is None:
                    raise StoreError("NO_SUBSCRIPTION", f"unknown subscription {name!r}")
                self._require_generation(row, generation, name)
                c.execute("UPDATE subscriptions SET paused=? WHERE name=?",
                          (1 if paused else 0, name))
                c.commit()
                st = self._sub_state(c, name)
                st.update({"paused": bool(paused)})
                return st
            finally:
                c.close()

    def handle_replay(self, name, after_seq, generation=None,
                      max_commits=100, max_bytes=1024 * 1024):
        """Re-deliver already delivered commits from a retained position.
        Never moves any cursor.  Positions before the retained range or
        beyond the delivered range are refused with the usable range."""
        if not isinstance(after_seq, int) or isinstance(after_seq, bool):
            raise StoreError("BAD_REQUEST", "after_seq must be an integer")
        if max_commits is None:
            max_commits = 100
        if max_bytes is None:
            max_bytes = 1024 * 1024
        if not isinstance(max_commits, int) or isinstance(max_commits, bool) \
                or max_commits < 1:
            raise StoreError("BAD_REQUEST", "max_commits must be >= 1")
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) \
                or max_bytes < 1:
            raise StoreError("BAD_REQUEST", "max_bytes must be >= 1")
        with self.commit_lock:
            c = self._control()
            try:
                row = self._subscription_row(c, name)
                if row is None:
                    raise StoreError("NO_SUBSCRIPTION", f"unknown subscription {name!r}")
                self._require_generation(row, generation, name)
                origin, acked, delivered, gen, paused = row
                log_low = self._log_floor(c)
                low = max(origin, log_low - 1)
                high = delivered
                if after_seq < low or after_seq > high:
                    raise StoreError(
                        "OUT_OF_RANGE",
                        f"replay position {after_seq} outside the retained"
                        f" delivered range [{low}, {high}] (origin {origin},"
                        f" log_low {log_low}, delivered {delivered})",
                        {"min_after": low, "max_after": high,
                         "log_low": log_low, "log_high": self._meta_int(
                             c, "visible_seq"),
                         "origin_seq": origin, "delivered_seq": delivered})
                items, used, over_budget, oversize_seq, last = self._collect_changes(
                    c, after_seq, high, max_commits, max_bytes)
                st = self._sub_state(c, name)
                st.update({"items": items, "next_after": last,
                           "has_more": last < high, "over_budget": over_budget,
                           "oversize_seq": oversize_seq, "bytes": used,
                           "replay": True, "after_seq": after_seq,
                           "range": {"low": low, "high": high}})
                return st
            finally:
                c.close()

    def handle_cleanup(self, up_to):
        """Release change payloads through up_to, but never past what an
        unconfirmed subscriber or crash recovery still needs.  The retained
        range moves in a single control transaction."""
        if not isinstance(up_to, int) or isinstance(up_to, bool):
            raise StoreError("BAD_REQUEST", "up_to must be an integer")
        with self.commit_lock:
            c = self._control()
            try:
                visible = self._meta_int(c, "visible_seq")
                floor = self._log_floor(c)
                if up_to > visible:
                    raise StoreError(
                        "OUT_OF_RANGE",
                        f"cleanup upper bound {up_to} is beyond the committed"
                        f" log end {visible}")
                if up_to < floor - 1:
                    # already cleaned past this point: a stable no-op
                    st = self._cleanup_report(c, floor - 1, 0, [])
                    st["requested_up_to"] = up_to
                    st["noop"] = True
                    return st
                blockers = []
                safe = visible
                for (sub_name, sub_acked) in c.execute(
                        "SELECT name, acked_seq FROM subscriptions"
                        " ORDER BY name").fetchall():
                    if sub_acked < up_to:
                        safe = min(safe, sub_acked)
                        blockers.append(sub_name)
                unapplied = c.execute(
                    "SELECT MIN(seq) FROM commits WHERE applied=0").fetchone()[0]
                if unapplied is not None and unapplied <= up_to:
                    safe = min(safe, unapplied - 1)
                    blockers.append("recovery")
                safe = max(safe, floor - 1)
                if up_to > safe:
                    raise StoreError(
                        "CLEANUP_BLOCKED",
                        f"cannot clean through {up_to}: unconfirmed subscribers"
                        f" or recovery still need commits (safe upper bound"
                        f" {safe}, blockers {blockers})",
                        {"safe_up_to": safe, "blocked_by": blockers,
                         "log_low": floor})
                released = c.execute(
                    "SELECT COUNT(*) FROM commits WHERE seq<=? AND payload_live=1",
                    (up_to,)).fetchone()[0]
                # payload release and the retained-range change are one
                # transaction: a crash leaves either the whole old range or
                # the whole new range, never a partly visible boundary
                c.execute(
                    "UPDATE commits SET payload_live=0, deliver_json=NULL,"
                    " apply_json='{}', bump_json='[]'"
                    " WHERE seq<=? AND applied=1", (up_to,))
                c.commit()
                self._checkpoint("during_cleanup")
                return self._cleanup_report(c, up_to, released, [])
            finally:
                c.close()

    def _cleanup_report(self, c, cleaned_to, released, blockers):
        low = self._log_floor(c)
        return {
            "cleaned_to": cleaned_to,
            "released_commits": released,
            "log_low": low,
            "log_high": self._meta_int(c, "visible_seq"),
            "min_after": low - 1,
            "blocked_by": blockers,
        }

    def handle_subscriptions(self):
        with self.commit_lock:
            c = self._control()
            try:
                names = [r[0] for r in c.execute(
                    "SELECT name FROM subscriptions ORDER BY name").fetchall()]
                return {"subscriptions": [self._sub_state(c, n) for n in names],
                        "log_low": self._log_floor(c),
                        "log_high": self._meta_int(c, "visible_seq"),
                        "min_after": self._log_floor(c) - 1}
            finally:
                c.close()

    def handle_subscription_status(self, name):
        with self.commit_lock:
            c = self._control()
            try:
                return self._sub_state(c, name)
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
            self._recover_views()

    def _recover_views(self):
        """Restart view builds/tailers that were interrupted.  A generation
        either resumes from its persisted scan cursor / agg_seq or, if it
        could not complete exactly, is marked FAILED; the active generation is
        never disturbed."""
        pending = []
        with self.commit_lock:
            c = self._control()
            try:
                for (view, gen, pin) in c.execute(
                        "SELECT view, gen, pin_token FROM view_generations"
                        " WHERE phase IN ('BUILDING','CATCHING_UP')"
                        " ORDER BY view, gen").fetchall():
                    # drop the pin a crashed build may have left behind; the
                    # resumed build opens a fresh one
                    if pin is not None:
                        c.execute("UPDATE tokens SET released=1"
                                  " WHERE token_id=?", (pin,))
                    pending.append((view, gen))
                # the active generation keeps tailing writes after a restart
                for (view, gen) in c.execute(
                        "SELECT v.name, v.active_gen FROM views v"
                        " JOIN view_generations g ON g.view=v.name"
                        "  AND g.gen=v.active_gen WHERE g.phase='READY'"
                ).fetchall():
                    pending.append((view, gen))
                c.commit()
            finally:
                c.close()
        seen = set()
        for view, gen in pending:
            if (view, gen) in seen:
                continue
            seen.add((view, gen))
            self._start_view_worker(view, gen)

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

    def _check_view_fail(self):
        """Test hook: raise once if a rebuild failure was injected, so the
        acceptance drill can prove the previous generation stays intact."""
        if self.view_fail_armed:
            self.view_fail_armed = False
            raise StoreError("VIEW_BUILD_FAILED",
                             "injected view build failure (test hook)")

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
                # capture the committed value of every distinct key just
                # before this commit applies, in the same control transaction
                # as the decision -> grouped views can seed from any seq
                prior = self._capture_prior(c, routes, ops, seq)
                wire_apply = {sid: [common.op_to_wire(op) for op in sos]
                              for sid, sos in apply_map.items()}
                wire_ops = [common.op_to_wire(op) for op in ops]
                receipt = {"commit_seq": seq, "epoch": epoch,
                           "request_id": request_id, "ops_applied": len(ops)}
                # the change payload: the caller's ops in caller order, with
                # the original wire forms (so retransmitted request ids stay
                # byte-identical); deletes of missing keys are included too
                deliver = {"request_id": request_id, "epoch": epoch,
                           "watermark": self._write_watermark(request_id),
                           "ops": wire_ops}
                # the durable commit decision
                c.execute(
                    "INSERT INTO commits(seq,request_id,request_hash,receipt_json,"
                    "apply_json,bump_json,applied,deliver_json,payload_live,"
                    "prior_json) VALUES (?,?,?,?,?,?,0,?,1,?)",
                    (seq, request_id, req_hash, json.dumps(receipt),
                     json.dumps(wire_apply), json.dumps(bump),
                     json.dumps(deliver), json.dumps(prior)))
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

    # ------------------------------------------------------ grouped views
    # A view is a persistent, per-(tenant, nullable text) aggregate over the
    # *logical* row state at a committed seq.  A build starts from a caller
    # supplied committed start_seq, streams the MVCC state pinned by an
    # ordinary read token in bounded pages, then folds every later commit in
    # order.  The aggregate for one generation is append-only cumulative
    # versions keyed by the commit seq they summarize: querying at watermark W
    # reads, for each group, its greatest version <= W.  A generation becomes
    # visible only in one control transaction after its aggregate has reached
    # the live committed end, so a caller sees a complete old generation or a
    # complete new one, never a partial build.
    #
    # Prior capture: the commit path stores, in the same control transaction as
    # the durable decision, the committed value of every distinct key the
    # commit touches.  Catch-up therefore folds a commit exactly from durable
    # records without depending on MVCC retention, and never splits a
    # cross-shard transaction: a commit becomes foldable only once applied.

    def _capture_prior(self, c, routes, ops, seq):
        """Committed value of each distinct op key just before `seq` applies.

        Runs inside commit_lock against the same serial writer that is about
        to install `seq`, read through the *current* routing, so it is correct
        across shard boundaries and after a split.  Result:
        {tenant: {decimal-key: wire value}}; missing = no live row."""
        wanted = {}
        for op in ops:
            wanted.setdefault(op[1], set()).add(op[2])
        prior = {}
        for tenant, keys in wanted.items():
            prior[tenant] = {}
            by_shard = {}
            for k in keys:
                by_shard.setdefault(self._route_lookup(routes, tenant, k),
                                    []).append(k)
            for sid, ks in by_shard.items():
                conn = self._shard_conn(sid)
                try:
                    qmarks = ",".join("?" for _ in ks)
                    rows = conn.execute(
                        "SELECT k, op, vt, vi, vb FROM ("
                        "  SELECT k, MAX(seq) AS mseq, op, vt, vi, vb FROM kv"
                        "  WHERE tenant=? AND k IN (" + qmarks + ")"
                        "    AND seq < ? GROUP BY k)",
                        [tenant] + ks + [seq]).fetchall()
                finally:
                    conn.close()
                for k, op_, vt, vi, vb in rows:
                    if op_ == "put":
                        prior[tenant][str(k)] = common.value_to_wire(
                            (vt, vi, vb))
        return prior

    @staticmethod
    def _view_gkey(tenant, text):
        return (tenant, "N" if text is None else "S",
                "" if text is None else text)

    @staticmethod
    def _view_apply(acc, tenant, text, delta_rows, delta_ncnt, delta_sum):
        """Accumulate one *delta* in a local {(tenant,tag,text): [n,ncnt,sum]}.

        Deltas are never normalized here: a commit may move a row out of a
        group and another row in, leaving a zero row-delta but a non-zero
        integer delta that must still be applied.  Absolute totals (where an
        empty group carries zero integer values) are normalized in
        `_view_advance`."""
        key = Server._view_gkey(tenant, text)
        ent = acc.get(key)
        if ent is None:
            ent = acc[key] = [0, 0, 0]
        ent[0] += delta_rows
        ent[1] += delta_ncnt
        ent[2] += delta_sum

    def _view_fold_ops(self, prior, ops, acc):
        """Fold one commit's ops into a tenant->group accumulator, in caller
        order.  Only the net effect of a key's repeated ops in one commit is
        charged: the captured old value moves out and the value left by the
        last op moves in (put) or just the old value moves out (delete).  A
        delete of a missing key therefore changes nothing."""
        last = {}
        for op in ops:
            last[(op[1], op[2])] = op
        for (tenant, key), op in last.items():
            pv = prior.get(tenant, {}).get(str(key))
            old = None if pv is None else (pv["t"], pv["i"])
            if op[0] == "put":
                new = (op[3][0], op[3][1])
            else:
                new = None
            if old == new:
                continue
            # every live row counts toward the group's row count; only a
            # non-NULL integer contributes to the non-null count and sum
            if old is not None:
                old_i = old[1]
                if old_i is None:
                    self._view_apply(acc, tenant, old[0], -1, 0, 0)
                else:
                    self._view_apply(acc, tenant, old[0], -1, -1, -int(old_i))
            if new is not None:
                new_i = new[1]
                if new_i is None:
                    self._view_apply(acc, tenant, new[0], 1, 0, 0)
                else:
                    self._view_apply(acc, tenant, new[0], 1, 1, int(new_i))

    def _view_advance(self, c, view, gen, seq, deltas):
        """Append cumulative versions for one commit boundary.  `deltas` maps
        (tenant, tag, text) -> (drows, dncnt, dsum); groups whose delta is
        zero are skipped, so history grows with changed groups only."""
        pending = {k: v for k, v in deltas.items()
                   if v[0] or v[1] or v[2]}
        if not pending:
            return
        rows = c.execute(
            "SELECT g.tenant, g.txt_null, g.txt, g.cnt, g.ncnt, g.total"
            " FROM view_groups g JOIN ("
            "  SELECT tenant, txt_null, txt, MAX(seq) AS mseq FROM view_groups"
            "  WHERE view=? AND gen=? GROUP BY tenant, txt_null, txt"
            " ) m ON g.tenant=m.tenant AND g.txt_null=m.txt_null"
            "  AND g.txt=m.txt AND g.seq=m.mseq AND g.view=? AND g.gen=?",
            (view, gen, view, gen)).fetchall()
        cur = {(t, tn, tx): (cnt, ncnt, int(total))
               for t, tn, tx, cnt, ncnt, total in rows}
        ins = []
        for key, (drows, dncnt, dsum) in pending.items():
            base = cur.get(key, (0, 0, 0))
            nrows = base[0] + drows
            ncnt = base[1] + dncnt
            total = base[2] + dsum
            if nrows == 0:
                ncnt = total = 0
            ins.append((view, gen, key[0], key[1], key[2], seq,
                        nrows, ncnt, str(total)))
        c.executemany(
            "INSERT OR REPLACE INTO view_groups"
            "(view,gen,tenant,txt_null,txt,seq,cnt,ncnt,total)"
            " VALUES (?,?,?,?,?,?,?,?,?)", ins)

    def _view_epoch_at(self, c, seq):
        """Routing epoch that was live at committed seq `seq`."""
        row = c.execute(
            "SELECT MAX(epoch) FROM epoch_meta WHERE active_from_seq<=?",
            (seq,)).fetchone()
        if row is None or row[0] is None:
            return self._meta_int(c, "epoch")
        return int(row[0])

    def _view_row(self, c, name):
        return c.execute(
            "SELECT active_gen, start_seq, created_seq FROM views WHERE name=?",
            (name,)).fetchone()

    def _view_gen_row(self, c, view, gen):
        return c.execute(
            "SELECT start_seq, epoch, pin_token, scan_done, scan_t, scan_k,"
            " agg_seq, phase, error FROM view_generations"
            " WHERE view=? AND gen=?", (view, gen)).fetchone()

    def _view_ready_info(self, c, name):
        """Status of the active generation, raising VIEW_NOT_READY until it is
        fully built and swapped in.  Never reports a half-built generation as
        queryable."""
        row = self._view_row(c, name)
        if row is None:
            raise StoreError("NO_VIEW", f"unknown view {name!r}")
        active_gen, start_seq, created_seq = row
        visible = self._meta_int(c, "visible_seq")
        gen_rows = c.execute(
            "SELECT gen, start_seq, epoch, phase, agg_seq, scan_done, error,"
            " pin_token FROM view_generations WHERE view=? ORDER BY gen",
            (name,)).fetchall()
        gens = []
        active = None
        for (gen, gstart, gepoch, phase, agg_seq, scan_done, error,
             pin) in gen_rows:
            materialized = phase == "READY" and bool(scan_done)
            gens.append({
                "gen": gen, "start_seq": gstart, "epoch": gepoch,
                "phase": phase, "agg_seq": agg_seq,
                "scan_done": bool(scan_done),
                "materialized": materialized, "error": error,
                "is_active": gen == active_gen,
            })
            if gen == active_gen:
                active = {"gen": gen, "start_seq": gstart, "agg_seq": agg_seq,
                          "materialized": materialized, "phase": phase}
        if active is None or not active["materialized"]:
            detail = {"generations": gens}
            if active is not None and active["phase"] == "FAILED":
                # a failed generation never materializes: report it at once
                detail["permanent"] = True
                detail["phase"] = "FAILED"
            raise StoreError(
                "VIEW_NOT_READY",
                f"view {name!r} active generation {active_gen} is not ready",
                detail)
        return {
            "name": name, "active_gen": active_gen,
            "start_seq": active["start_seq"], "created_seq": created_seq,
            "visible_seq": visible,
            "build_watermark": active["agg_seq"],
            "queryable_lo": active["start_seq"],
            "queryable_hi": active["agg_seq"],
            "generations": gens,
        }

    def handle_view_create(self, name, start_seq, rebuild=False):
        if not isinstance(name, str) or not name:
            raise StoreError("BAD_REQUEST", "view name must be non-empty")
        if not isinstance(start_seq, int) or isinstance(start_seq, bool):
            raise StoreError("BAD_REQUEST", "start_seq must be an integer")
        with self.commit_lock:
            c = self._control()
            try:
                visible = self._meta_int(c, "visible_seq")
                if start_seq < 0 or start_seq > visible:
                    raise StoreError(
                        "OUT_OF_RANGE",
                        f"view start_seq {start_seq} is not a committed seq in"
                        f" [0, {visible}]",
                        {"min_start_seq": 0, "max_start_seq": visible})
                row = self._view_row(c, name)
                if row is not None and not rebuild:
                    raise StoreError(
                        "VIEW_EXISTS",
                        f"view {name!r} already exists; rebuild to start a new"
                        f" generation")
                if row is None:
                    next_gen = 1
                else:
                    next_gen = 1 + c.execute(
                        "SELECT MAX(gen) FROM view_generations WHERE view=?",
                        (name,)).fetchone()[0]
                epoch = self._view_epoch_at(c, start_seq)
                c.execute(
                    "INSERT INTO view_generations(view,gen,start_seq,epoch,"
                    "pin_token,scan_done,scan_t,scan_k,agg_seq,phase,error,"
                    "created_seq) VALUES (?,?,?,?,NULL,0,NULL,NULL,?,"
                    "'BUILDING',NULL,?)",
                    (name, next_gen, start_seq, epoch, start_seq, visible))
                if row is None:
                    c.execute(
                        "INSERT INTO views(name,active_gen,start_seq,created_seq)"
                        " VALUES (?,?,?,?)",
                        (name, next_gen, start_seq, visible))
                c.commit()
            finally:
                c.close()
        self._start_view_worker(name, next_gen)
        return self._view_status_or_building(name)

    def _view_status_or_building(self, name):
        """Status for a just-created view: report the definition even while
        the first generation is still building (no active result yet)."""
        with self.commit_lock:
            c = self._control()
            try:
                row = self._view_row(c, name)
                if row is None:
                    raise StoreError("NO_VIEW", f"unknown view {name!r}")
                active_gen, start_seq, created_seq = row
                visible = self._meta_int(c, "visible_seq")
                gen_rows = c.execute(
                    "SELECT gen, start_seq, epoch, phase, agg_seq, scan_done,"
                    " error FROM view_generations WHERE view=? ORDER BY gen",
                    (name,)).fetchall()
                gens = [{"gen": g, "start_seq": gs, "epoch": ge, "phase": ph,
                         "agg_seq": ag, "scan_done": bool(sd),
                         "materialized": ph == "READY" and bool(sd),
                         "error": er, "is_active": g == active_gen}
                        for (g, gs, ge, ph, ag, sd, er) in gen_rows]
                active = next((x for x in gens if x["gen"] == active_gen), None)
                ready = bool(active and active["materialized"])
                return {
                    "name": name, "active_gen": active_gen,
                    "start_seq": active["start_seq"] if active else start_seq,
                    "created_seq": created_seq,
                    "visible_seq": visible,
                    "build_watermark": active["agg_seq"] if ready else None,
                    "queryable_lo": active["start_seq"] if ready else None,
                    "queryable_hi": active["agg_seq"] if ready else None,
                    "ready": ready, "generations": gens,
                }
            finally:
                c.close()

    def handle_view_rebuild(self, name, start_seq=None):
        with self.commit_lock:
            c = self._control()
            try:
                row = self._view_row(c, name)
            finally:
                c.close()
        if row is None:
            raise StoreError("NO_VIEW", f"unknown view {name!r}")
        if start_seq is None:
            start_seq = row[1]
        return self.handle_view_create(name, start_seq, rebuild=True)

    def _start_view_worker(self, name, gen):
        key = (name, gen)
        with self.inflight_lock:
            if key in self._view_workers:
                return
            self._view_workers.add(key)
        threading.Thread(target=self._view_worker, args=(name, gen),
                         name=f"view-{name}-{gen}", daemon=True).start()

    def _stop_view_worker(self, name, gen):
        with self.inflight_lock:
            self._view_workers.discard((name, gen))

    def _view_worker(self, name, gen):
        try:
            with self.commit_lock:
                c = self._control()
                try:
                    grow = self._view_gen_row(c, name, gen)
                finally:
                    c.close()
            if grow is None:
                return
            start_seq, epoch, _pin, scan_done, scan_t, scan_k, agg_seq, phase, \
                _err = grow
            if phase not in ("BUILDING", "CATCHING_UP", "READY"):
                return
            if not scan_done:
                self._view_scan(name, gen, start_seq)
            self._view_catchup(name, gen)
        except Exception as e:  # noqa: BLE001 - record, never publish partial
            traceback.print_exc()
            try:
                with self.commit_lock:
                    c = self._control()
                    try:
                        c.execute(
                            "UPDATE view_generations SET phase='FAILED',"
                            " error=? WHERE view=? AND gen=? AND phase IN"
                            " ('BUILDING','CATCHING_UP')",
                            (repr(e), name, gen))
                        c.commit()
                    finally:
                        c.close()
            except Exception:
                traceback.print_exc()
        finally:
            self._stop_view_worker(name, gen)

    def _view_scan(self, name, gen, start_seq):
        """Establish the aggregate at start_seq without depending on MVCC
        retention: read the *current* committed state, back out every commit
        after start_seq using its captured before-values, then fold the
        result in bounded pages.  A scan always runs to completion; a crash
        resumes by redoing it from the start (the staged rows are connection
        local and the base aggregate is cleared first), so progress only ever
        names a fully included commit boundary.

        The initial read uses the *current* routing epoch: build_seq is the
        current committed end, and a published split retires the old source (it
        stops receiving writes), so only the live epoch holds the complete
        state at build_seq.  The reverse-undo step only needs log records, not
        historical routing, so the state at start_seq is recovered correctly
        regardless of how many splits happened in between."""
        with self.commit_lock:
            c = self._control()
            try:
                build_seq = self._meta_int(c, "visible_seq")
                scan_epoch = self._meta_int(c, "epoch")
                routes = self._routes(c, scan_epoch)
                token = "rt" + secrets.token_hex(12)
                c.execute("INSERT INTO tokens(token_id,epoch,read_seq,released)"
                          " VALUES (?,?,?,0)", (token, scan_epoch, build_seq))
                c.execute("UPDATE view_generations SET pin_token=?"
                          " WHERE view=? AND gen=?", (token, name, gen))
                if start_seq == 0:
                    # the state before any commit is empty: nothing to read or
                    # back out, so the base aggregate is trivially established
                    c.execute("DELETE FROM view_groups WHERE view=? AND gen=?",
                              (name, gen))
                    c.execute("UPDATE view_generations SET scan_done=1,"
                              " agg_seq=0, scan_t=NULL, scan_k=NULL"
                              " WHERE view=? AND gen=?", (name, gen))
                c.commit()
            finally:
                c.close()
        if start_seq == 0:
            try:
                self.handle_release(token)
            except Exception:
                traceback.print_exc()
            self._checkpoint("after_view_scan")
            # an injected failure must be observable on every scan path,
            # including the trivially-empty start
            self._check_view_fail()
            return
        try:
            # stage the reversed ("state at start_seq") rows on disk in the
            # control database so the working set never has to fit in memory
            c = self._control()
            c.execute("CREATE TEMP TABLE IF NOT EXISTS view_undo ("
                      "  tenant TEXT NOT NULL, k INTEGER NOT NULL,"
                      "  op TEXT NOT NULL, vt TEXT, vi INTEGER, pb BLOB,"
                      "  PRIMARY KEY (tenant, k))")
            c.execute("DELETE FROM temp.view_undo")
            # a scan that never finished leaves only partial aggregate rows at
            # start_seq with scan_done=0; discard them and rebuild from the
            # top.  agg_seq only advances after scan_done is set, so no commit
            # boundary is ever half-included and progress always corresponds
            # to a fully included boundary.
            c.execute("DELETE FROM view_groups WHERE view=? AND gen=?",
                      (name, gen))
            c.execute("UPDATE view_generations SET agg_seq=start_seq,"
                      " scan_t=NULL, scan_k=NULL WHERE view=? AND gen=?",
                      (name, gen))
            c.commit()
            # 1) current live rows (as of build_seq)
            for sid, rlo, rhi in routes:
                cur = None
                while True:
                    batch = self._shard_scan(sid, build_seq, rlo, False, rhi,
                                             self.batch_size) if cur is None else \
                        self._shard_scan(sid, build_seq, cur, True, rhi,
                                         self.batch_size)
                    if not batch:
                        break
                    c.executemany(
                        "INSERT OR REPLACE INTO temp.view_undo"
                        "(tenant,k,op,vt,vi,pb) VALUES (?,?,'put',?,?,?)",
                        [(t, k, vt, vi, vb) for (t, k, (vt, vi, vb)) in batch])
                    c.commit()
                    cur = (batch[-1][0], batch[-1][1])
            # 2) undo commits build_seq..start_seq+1 in descending order.  A
            #    commit's prior holds the committed value of each touched key
            #    just before it; restoring that value (or removing the key
            #    when there was none) recovers the state before the commit.
            for seq in range(build_seq, start_seq, -1):
                row = c.execute("SELECT deliver_json, prior_json FROM commits"
                                " WHERE seq=?", (seq,)).fetchone()
                if row is None:
                    raise StoreError(
                        "VIEW_BUILD_FAILED",
                        f"no durable record for commit {seq}; cannot rebuild"
                        f" the state at {start_seq}")
                djson, pjson = row
                if djson is None or pjson is None:
                    raise StoreError(
                        "VIEW_BUILD_FAILED",
                        f"commit {seq} has no retained before-values; build"
                        f" view {name!r} from a later start_seq")
                prior = json.loads(pjson)
                # ops in the payload are wire dicts; collect (tenant,key)
                keys = set()
                for o in json.loads(djson)["ops"]:
                    keys.add((o["tenant"], int(o["key"])))
                for (t, k) in keys:
                    pv = prior.get(t, {}).get(str(k))
                    if pv is None:
                        c.execute("DELETE FROM temp.view_undo"
                                  " WHERE tenant=? AND k=?", (t, k))
                    else:
                        c.execute(
                            "INSERT OR REPLACE INTO temp.view_undo"
                            "(tenant,k,op,vt,vi,pb) VALUES (?,?,'put',?,?,?)",
                            (t, k, pv["t"], None if pv["i"] is None
                             else int(pv["i"]),
                             None if pv["b"] is None
                             else common.b64d(pv["b"])))
                c.commit()
                self._checkpoint("during_view_build")
                self._check_view_fail()
            # 3) fold the staged rows into the aggregate in bounded pages.  The
            #    staging table lives only for this connection, so a scan is
            #    always folded from the start; the base aggregate was just
            #    cleared above, which keeps resumption after a crash exact.
            cursor = None
            while True:
                sql = ("SELECT tenant, k, vt, vi FROM temp.view_undo")
                params = []
                if cursor is not None:
                    sql += " WHERE (tenant, k) > (?, ?)"
                    params.extend([cursor[0], cursor[1]])
                sql += " ORDER BY tenant, k LIMIT ?"
                params.append(self.batch_size)
                page = c.execute(sql, params).fetchall()
                if not page:
                    break
                deltas = {}
                for tenant, _k, vt, vi in page:
                    if vi is None:
                        self._view_apply(deltas, tenant, vt, 1, 0, 0)
                    else:
                        self._view_apply(deltas, tenant, vt, 1, 1, int(vi))
                self._view_advance(c, name, gen, start_seq,
                                   {k: tuple(v) for k, v in deltas.items()})
                cursor = (page[-1][0], page[-1][1])
                c.execute("UPDATE view_generations SET scan_t=?,scan_k=?"
                          " WHERE view=? AND gen=?",
                          (cursor[0], cursor[1], name, gen))
                c.commit()
                self._checkpoint("during_view_fold")
                self._check_view_fail()
            c.execute("DELETE FROM temp.view_undo")
            c.execute("UPDATE view_generations SET scan_done=1,"
                      " scan_t=NULL, scan_k=NULL WHERE view=? AND gen=?",
                      (name, gen))
            c.commit()
            self._checkpoint("after_view_scan")
        finally:
            c.close()
            # the scan is over: the pin and the staged table are no longer
            # needed (release is idempotent, and recovery also clears it)
            try:
                self.handle_release(token)
            except Exception:
                traceback.print_exc()

    def _view_ops_for_commit(self, c, seq):
        """(decoded ops, captured prior) for commit `seq`, or None when it
        cannot be folded exactly (prior capture absent)."""
        row = c.execute(
            "SELECT deliver_json, prior_json, applied FROM commits WHERE seq=?",
            (seq,)).fetchone()
        if row is None or not row[2]:
            raise StoreError("INTERNAL", f"commit {seq} missing or unapplied")
        deliver_json, prior_json = row[0], row[1]
        if deliver_json is None or prior_json is None:
            return None
        return ([common.op_from_wire(o) for o in json.loads(deliver_json)["ops"]],
                json.loads(prior_json))

    def _view_catchup(self, name, gen):
        """Fold commits in seq order, advancing agg_seq in the same control
        transaction as each commit's aggregate version.  Only fully applied
        commits are folded, so a partially applied cross-shard transaction is
        never half counted.  While the generation is BUILDING it publishes as
        soon as it reaches the live committed end; once READY it keeps tailing
        new commits so the view stays exact at every later watermark, without
        ever answering ahead of what it has materialized."""
        while not self.shutdown_flag.is_set():
            with self.commit_lock:
                c = self._control()
                try:
                    grow = self._view_gen_row(c, name, gen)
                    if grow is None or grow[7] == "FAILED":
                        return
                    agg_seq, phase = grow[6], grow[7]
                    visible = self._meta_int(c, "visible_seq")
                    nxt = None
                    if agg_seq < visible:
                        r = c.execute("SELECT applied FROM commits WHERE seq=?",
                                      (agg_seq + 1,)).fetchone()
                        if r is not None and r[0]:
                            nxt = agg_seq + 1  # fully applied: fold it
                    if nxt is None:
                        if phase != "READY" and agg_seq >= visible:
                            # initial build reached the committed end: the
                            # first generation becomes visible atomically
                            self._checkpoint("before_view_publish")
                            c.execute(
                                "UPDATE view_generations SET phase='READY'"
                                " WHERE view=? AND gen=?", (name, gen))
                            c.execute("UPDATE views SET active_gen=?"
                                      " WHERE name=?", (gen, name))
                            c.commit()
                            self._checkpoint("after_view_publish")
                            # continue as a tailer
                            continue
                        if phase != "READY":
                            c.execute(
                                "UPDATE view_generations SET phase='CATCHING_UP'"
                                " WHERE view=? AND gen=?", (name, gen))
                            c.commit()
                        nxt = "wait"
                    else:
                        ops_prior = self._view_ops_for_commit(c, nxt)
                        if ops_prior is None:
                            raise StoreError(
                                "VIEW_BUILD_FAILED",
                                f"commit {nxt} has no retained before-values;"
                                f" build view {name!r} from a later start_seq")
                        ops, prior = ops_prior
                        acc = {}
                        self._view_fold_ops(prior, ops, acc)
                        self._view_advance(c, name, gen, nxt,
                                           {k: tuple(v) for k, v in
                                            acc.items()})
                        c.execute("UPDATE view_generations SET agg_seq=?"
                                  " WHERE view=? AND gen=?", (nxt, name, gen))
                        c.commit()
                        self._checkpoint("during_view_catchup")
                finally:
                    c.close()
            if nxt == "wait":
                time.sleep(0.02)

    def handle_view_status(self, name):
        with self.commit_lock:
            c = self._control()
            try:
                return self._view_ready_info(c, name)
            finally:
                c.close()

    def handle_view_state(self, name):
        """Definition/build status, including when the active generation is
        still building (unlike query/status readiness)."""
        return self._view_status_or_building(name)

    def handle_views(self):
        out = []
        with self.commit_lock:
            c = self._control()
            try:
                names = [r[0] for r in c.execute(
                    "SELECT name FROM views ORDER BY name").fetchall()]
            finally:
                c.close()
        for n in names:
            try:
                out.append(self.handle_view_status(n))
            except StoreError as e:
                if e.code != "VIEW_NOT_READY":
                    raise
                out.append(self._view_status_or_building(n))
        return {"views": out}

    def handle_view_query(self, name, watermark=None, lo=None, hi=None,
                          wait=True, timeout_ms=30000):
        """Answer exactly at `watermark` (default: the active build
        watermark).  If it is not yet materialized, wait up to timeout_ms;
        then fail with VIEW_NOT_READY instead of silently answering at another
        watermark."""
        if watermark is not None and (not isinstance(watermark, int)
                                     or isinstance(watermark, bool)):
            raise StoreError("BAD_REQUEST", "watermark must be an integer")
        start_t = time.time()
        last = None
        while True:
            with self.commit_lock:
                c = self._control()
                try:
                    row = self._view_row(c, name)
                    if row is None:
                        raise StoreError("NO_VIEW", f"unknown view {name!r}")
                    st = self._view_ready_info(c, name)
                    target = (st["build_watermark"] if watermark is None
                              else watermark)
                    if target < st["queryable_lo"]:
                        # below the generation's start there is no aggregate and
                        # never will be: waiting cannot help -> refuse at once
                        raise StoreError(
                            "VIEW_NOT_READY",
                            f"view {name!r} starts at {st['queryable_lo']};"
                            f" watermark {target} predates it",
                            {"queryable_lo": st["queryable_lo"],
                             "queryable_hi": st["queryable_hi"],
                             "build_watermark": st["build_watermark"],
                             "watermark": target, "permanent": True})
                    if target > st["queryable_hi"]:
                        # beyond the committed end, or a commit the tailer has
                        # not folded yet: may become available by waiting
                        raise StoreError(
                            "VIEW_NOT_READY",
                            f"view {name!r} has no materialized aggregate at"
                            f" watermark {target} (available"
                            f" [{st['queryable_lo']}, {st['queryable_hi']}])",
                            {"queryable_lo": st["queryable_lo"],
                             "queryable_hi": st["queryable_hi"],
                             "build_watermark": st["build_watermark"],
                             "watermark": target})
                    groups = self._view_query_rows(
                        c, name, st["active_gen"], target, lo, hi)
                    return {"name": name, "gen": st["active_gen"],
                            "watermark": target,
                            "queryable_lo": st["queryable_lo"],
                            "queryable_hi": st["queryable_hi"],
                            "groups": groups}
                except StoreError as e:
                    if e.code != "VIEW_NOT_READY":
                        raise
                    last = e
                    if e.detail and e.detail.get("permanent"):
                        raise  # cannot become available by waiting
                finally:
                    c.close()
            if not wait or (time.time() - start_t) * 1000 >= timeout_ms:
                raise StoreError(
                    "VIEW_NOT_READY",
                    f"view {name!r} is not ready at watermark {watermark}"
                    f" (waited {int((time.time() - start_t) * 1000)} ms)",
                    last.detail if last is not None else {"watermark": watermark})
            time.sleep(0.02)

    def _view_query_rows(self, c, name, gen, watermark, lo, hi):
        """Latest cumulative version per group at or below `watermark`,
        filtered by the optional (tenant, text) key range, in stable order."""
        sql = ("SELECT g.tenant, g.txt_null, g.txt, g.cnt, g.ncnt, g.total"
               " FROM view_groups g JOIN ("
               "   SELECT tenant, txt_null, txt, MAX(seq) AS mseq"
               "   FROM view_groups WHERE view=? AND gen=? AND seq<=?"
               "   GROUP BY tenant, txt_null, txt"
               " ) m ON g.tenant=m.tenant AND g.txt_null=m.txt_null"
               "   AND g.txt=m.txt AND g.seq=m.mseq AND g.view=? AND g.gen=?")
        params = [name, gen, watermark, name, gen]
        conds = []
        if lo is not None:
            conds.append("(g.tenant, g.txt_null, g.txt) >= (?, ?, ?)")
            params.extend(self._view_gkey(lo[0], lo[1]))
        if hi is not None:
            conds.append("(g.tenant, g.txt_null, g.txt) < (?, ?, ?)")
            params.extend(self._view_gkey(hi[0], hi[1]))
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY g.tenant, g.txt_null, g.txt"
        out = []
        for tenant, tn, txt, cnt, ncnt, total in c.execute(sql, params):
            if cnt <= 0:
                continue  # an emptied group has disappeared
            out.append({"tenant": tenant,
                        "text": None if tn == "N" else txt,
                        "rows": int(cnt),
                        "int_nonnull": int(ncnt),
                        "int_sum": total})
        return out

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
            # record when this epoch became live so a historical commit seq
            # maps to the routing that was actually in service then
            c.execute("INSERT OR REPLACE INTO epoch_meta(epoch,active_from_seq)"
                      " VALUES (?,?)", (new_epoch, visible + 1))
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

    # ------------------------------------------------------------ snapshots
    # A snapshot pins one committed frontier together with the complete
    # routing view at that frontier.  The pin is an ordinary read token, so
    # concurrent commits and splits keep their existing contracts: the MVCC
    # row versions and the shard files this snapshot reads stay reachable
    # until the token is released, and GC/reclaim can never remove them.
    def _snapshot_check_absolute(self, path, what):
        from shardstore.snapshot import SnapshotError
        if not isinstance(path, str) or not path:
            raise StoreError("BAD_REQUEST", f"{what} must be a non-empty path")
        if not os.path.isabs(path):
            raise StoreError("BAD_REQUEST",
                             f"{what} must be an absolute path, got {path!r}")
        return path

    def _snapshot_routing_view(self, c, epoch):
        """The shard-independent routing view stored in the backup."""
        from shardstore import snapshot
        routes = self._routes(c, epoch)
        view = []
        for sid, lo, hi in routes:
            view.append({"shard": sid,
                         "lo": snapshot.enc_bound(lo),
                         "hi": snapshot.enc_bound(hi)})
        return view

    def handle_snapshot_export(self, backup_path):
        """Stream a portable, consistent backup of one committed frontier.

        The source service keeps accepting writes and splits for the whole
        export: rows are read through a pinned read token (read_seq + epoch),
        receipts come from the durable commit table, and the backup only
        becomes visible once every segment and the manifest are complete and
        re-verified.  Nothing is buffered wholesale in memory.
        """
        from shardstore import snapshot
        self._snapshot_check_absolute(backup_path, "backup path")
        if os.path.lexists(backup_path):
            raise StoreError(
                "BACKUP_EXISTS",
                f"backup destination {backup_path!r} already exists; choose a"
                f" new path, backups are never overwritten")
        parent = os.path.dirname(os.path.abspath(backup_path))
        if not os.path.isdir(parent):
            raise StoreError(
                "BAD_REQUEST",
                f"parent directory of the backup does not exist: {parent!r}")

        # pin the consistency point: one committed seq + one complete routing
        # epoch.  The token keeps every row version and shard file this
        # snapshot needs alive despite concurrent GC, split and reclaim.
        token = self.handle_open_read()
        token_id = token["token"]
        watermark = token["read_seq"]
        epoch = token["epoch"]
        staging = os.path.join(parent, ".snapshot-" + secrets.token_hex(12))
        try:
            try:
                return self._snapshot_export_locked(
                    snapshot, staging, backup_path, parent, token_id,
                    watermark, epoch)
            except snapshot.SnapshotError as e:
                raise StoreError(e.code, e.message, e.detail) from e
        except BaseException:
            import shutil
            shutil.rmtree(staging, ignore_errors=True)
            raise
        finally:
            # the pin is released exactly once, whatever happened above
            try:
                self.handle_release(token_id)
            except Exception:
                traceback.print_exc()

    def _snapshot_export_locked(self, snapshot, staging, backup_path, parent,
                                token_id, watermark, epoch):
        """Body of the export; the caller owns staging cleanup and the pin."""
        os.mkdir(staging)
        c = self._control()
        try:
            routes = self._routes(c, epoch)
            routing_view = self._snapshot_routing_view(c, epoch)
            # sanity: the epoch must partition the whole key domain
            if not routes or routes[0][1] is not None \
                    or routes[-1][2] is not None:
                raise StoreError("INTERNAL",
                                 f"epoch {epoch} routing is incomplete")
        finally:
            c.close()

        segment_no = 0

        def open_segment(role):
            nonlocal segment_no
            segment_no += 1
            name = f"seg-{segment_no:06d}.dat"
            return snapshot.SegmentWriter(
                os.path.join(staging, name), name)

        segments = []
        row_count = 0
        key_first = None
        key_last = None

        # ---- rows: route by route, in the global composite order, each
        # page small enough to bound memory regardless of database size
        writer = open_segment("rows")
        try:
            for sid, rlo, rhi in routes:
                cursor = None
                while True:
                    batch = self._snapshot_scan_batch(
                        sid, watermark, rlo, rhi, cursor, self.batch_size)
                    if not batch:
                        break
                    for (tenant, key, value) in batch:
                        writer.write_row(tenant, key, value)
                        if key_first is None:
                            key_first = snapshot.enc_key(tenant, key)
                        key_last = snapshot.enc_key(tenant, key)
                        row_count += 1
                    cursor = (batch[-1][0], batch[-1][1])
                    if writer.length >= snapshot.segment_target_bytes():
                        segments.append(self._close_segment(writer, "rows"))
                        writer = open_segment("rows")
                    if row_count % (self.batch_size * 4) == 0:
                        self._checkpoint("during_snapshot_segments")
        finally:
            if writer.record_count or not segments:
                segments.append(self._close_segment(writer, "rows"))
            else:
                writer.close()
                os.unlink(writer.path)

        # ---- idempotency receipts: every decided commit through the
        # watermark, including ones whose change payload was cleaned up.
        # Streamed straight from SQLite; historical payloads are not
        # exported at all.
        writer = open_segment("receipts")
        receipt_count = 0
        try:
            c = self._control()
            try:
                cursor_seq = 0
                while True:
                    rows = c.execute(
                        "SELECT seq, request_id, request_hash, receipt_json"
                        " FROM commits WHERE seq>? AND seq<=?"
                        " ORDER BY seq LIMIT ?",
                        (cursor_seq, watermark, self.batch_size)).fetchall()
                    if not rows:
                        break
                    for seq, rid, rh, rj in rows:
                        writer.write_receipt(seq, rid, rh, rj)
                        receipt_count += 1
                        cursor_seq = seq
                    if writer.length >= snapshot.segment_target_bytes():
                        segments.append(
                            self._close_segment(writer, "receipts"))
                        writer = open_segment("receipts")
                    self._checkpoint("during_snapshot_segments")
            finally:
                c.close()
        finally:
            if writer.record_count or not any(
                    s["role"] == "receipts" for s in segments):
                segments.append(self._close_segment(writer, "receipts"))
            else:
                writer.close()
                os.unlink(writer.path)

        manifest = {
            "format": snapshot.SNAPSHOT_FORMAT,
            "format_version": snapshot.SNAPSHOT_VERSION,
            "created_at": datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="seconds"),
            "watermark": watermark,
            "epoch_at_export": epoch,
            "row_count": row_count,
            "receipt_count": receipt_count,
            "key_range": {"first": key_first, "last": key_last},
            "routing": routing_view,
            "segments": segments,
        }
        self._checkpoint("before_snapshot_manifest")
        snapshot.write_manifest_atomic(staging, manifest)
        self._checkpoint("after_snapshot_manifest")

        # re-verify the whole staging backup from its bytes before it may
        # become visible; a crash here leaves only anonymous temp data
        try:
            _m, report = snapshot.verify_backup(staging)
        except snapshot.SnapshotError as e:
            raise StoreError(e.code, e.message, e.detail) from e
        if report["row_count"] != row_count \
                or report["receipt_count"] != receipt_count \
                or report["watermark"] != watermark:
            raise StoreError("INTERNAL",
                             "snapshot self-verification disagreed with"
                             " the export counters")
        # the destination was empty at entry; never overwrite a path that
        # appeared while the (potentially long) export was running
        if os.path.lexists(backup_path):
            raise StoreError(
                "BACKUP_EXISTS",
                f"backup destination {backup_path!r} appeared during the"
                f" export; the completed backup is kept in staging and is"
                f" not published over it")
        # single atomic publication; an interrupted rename leaves either
        # the staging temp data or the finished backup, never a mix
        os.replace(staging, backup_path)
        snapshot._fsync_dir(parent)
        report.update({
            "backup_path": os.path.abspath(backup_path),
            "published": True,
            "subscription_resume_after_seq": watermark,
            "retained_log_low": watermark + 1,
        })
        return report

    def _close_segment(self, writer, role):
        writer.close()
        return writer.descriptor(role)

    def _snapshot_scan_batch(self, shard_id, read_seq, rlo, rhi, cursor, limit):
        """One bounded, MVCC-pinned page of live rows from one shard."""
        conn = self._shard_conn(shard_id)
        try:
            sql = ("SELECT tenant, k, vt, vi, vb FROM ("
                   "SELECT tenant, k, MAX(seq) AS mseq, op, vt, vi, vb FROM kv"
                   " WHERE seq <= ?")
            params = [read_seq]
            if rlo is not None:
                sql += " AND (tenant, k) >= (?, ?)"
                params.extend([rlo[0], rlo[1]])
            if rhi is not None:
                sql += " AND (tenant, k) < (?, ?)"
                params.extend([rhi[0], rhi[1]])
            if cursor is not None:
                sql += " AND (tenant, k) > (?, ?)"
                params.extend([cursor[0], cursor[1]])
            sql += (" GROUP BY tenant, k) WHERE op = 'put'"
                    " ORDER BY tenant, k LIMIT ?")
            params.append(limit)
            return [(t, k, (vt, vi, vb))
                    for (t, k, vt, vi, vb)
                    in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def handle_snapshot_inspect(self, backup_path):
        from shardstore import snapshot
        self._snapshot_check_absolute(backup_path, "backup path")
        try:
            _manifest, report = snapshot.verify_backup(backup_path)
        except snapshot.SnapshotError as e:
            raise StoreError(e.code, e.message, e.detail)
        return report

    def handle_snapshot_restore(self, backup_path, target_dir):
        from shardstore import snapshot
        self._snapshot_check_absolute(backup_path, "backup path")
        self._snapshot_check_absolute(target_dir, "target directory")
        try:
            return snapshot.restore_backup(
                backup_path, target_dir,
                crash_cb=lambda point: self._checkpoint(point))
        except snapshot.SnapshotError as e:
            raise StoreError(e.code, e.message, e.detail)

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

    def handle_test_view_fail(self, on):
        """Arm a one-shot view build failure (test hook)."""
        self.view_fail_armed = bool(on)
        return {"armed": bool(on)}

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
        if op == "test_view_fail":
            return self.handle_test_view_fail(bool(args["on"]))
        # ---------------------------------------------------- subscriptions
        if op == "subscribe":
            return self.handle_subscribe(args["name"], int(args["after_seq"]))
        if op == "claim":
            return self.handle_claim(args["name"])
        if op == "pull":
            return self.handle_pull(args["name"], args.get("generation"),
                                    args.get("max_commits"),
                                    args.get("max_bytes"))
        if op == "ack":
            return self.handle_ack(args["name"], int(args["through_seq"]),
                                   args.get("generation"))
        if op == "set_paused":
            return self.handle_set_paused(args["name"], bool(args["paused"]),
                                          args.get("generation"))
        if op == "replay":
            return self.handle_replay(args["name"], int(args["after_seq"]),
                                      args.get("generation"),
                                      args.get("max_commits"),
                                      args.get("max_bytes"))
        if op == "cleanup":
            return self.handle_cleanup(int(args["up_to"]))
        if op == "subscriptions":
            return self.handle_subscriptions()
        if op == "subscription_status":
            return self.handle_subscription_status(args["name"])
        # -------------------------------------------------------- snapshots
        if op == "snapshot_export":
            return self.handle_snapshot_export(args["backup_path"])
        if op == "snapshot_inspect":
            return self.handle_snapshot_inspect(args["backup_path"])
        if op == "snapshot_restore":
            return self.handle_snapshot_restore(args["backup_path"],
                                                args["target_dir"])
        # ---------------------------------------------------- grouped views
        if op == "view_create":
            return self.handle_view_create(args["name"],
                                           int(args["start_seq"]))
        if op == "view_rebuild":
            ss = args.get("start_seq")
            return self.handle_view_rebuild(args["name"],
                                            None if ss is None else int(ss))
        if op == "view_status":
            return self.handle_view_state(args["name"])
        if op == "views":
            return self.handle_views()
        if op == "view_query":
            return self.handle_view_query(
                args["name"], args.get("watermark"),
                _parse_text_bound(args.get("lo")),
                _parse_text_bound(args.get("hi")),
                bool(args.get("wait", True)),
                int(args.get("timeout_ms", 30000)))
        raise StoreError("BAD_REQUEST", f"unknown op {op!r}")

    def _handle_conn(self, conn):
        try:
            while True:
                req = common.recv_msg(conn)
                try:
                    result = self._dispatch(req)
                    common.send_msg(conn, {"ok": True, "result": result})
                except StoreError as e:
                    err = {"code": e.code, "message": e.message}
                    if e.detail:
                        err["detail"] = e.detail
                    common.send_msg(conn, {"ok": False, "error": err})
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


def _parse_text_bound(b):
    """A view key bound is (tenant, text|None); None text is its own group."""
    if b is None:
        return None
    return (b["tenant"], b.get("text"))


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
