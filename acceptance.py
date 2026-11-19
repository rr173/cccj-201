#!/usr/bin/env python3
"""End-to-end acceptance drill for shardstore (multi-process).

Runs the real server as a subprocess and drives it from this process plus
extra client child processes over the unix socket.  Expected results come
from an independent serial dict model that keeps a full snapshot per commit
seq; the model never touches the production routing/migration code.

Phases:
  1. basic        composite-key order, NULL/int64/BLOB fidelity, dedup,
                  conflicting request_id, bounded scans, token errors
  2. main drill   the specified scenario: write 1..6, old scan page, paused
                  copy, one transaction (put 2', del 4, put 7), split at 4,
                  old scan remainder vs new scan, reclaim gating
  3. crash matrix forced kill after the commit decision, and at every split
                  stage (during copy / after copy / after catch-up / before
                  and after the routing decision) + retransmitted request_ids
                  and cross-shard transactions
  4. reclaim kill forced kill in the middle of reclamation
  5. sigkill      a real SIGKILL mid-copy; a read token survives the restart
  6. validation   bad boundaries / unknown shards / duplicate ranges are
                  rejected without changing the routing; two racing split
                  requests -> exactly one wins
  7. scale        100k rows generated at runtime, bounded-memory copy
                  (server RSS sampled), concurrent readers from child
                  processes, writes landing while the copy runs
"""
from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shardstore.client import Store, StoreError, put, delete  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
T = "t"          # main tenant for the small drills
SCALE_ROWS = int(os.environ.get("SCALE_ROWS", "100000"))
RSS_BUDGET_KB = int(os.environ.get("RSS_BUDGET_KB", str(300 * 1024)))


class Failure(Exception):
    pass


def check(cond, msg):
    if not cond:
        raise Failure(msg)


# ------------------------------------------------------------------ model
class Model:
    """Independent serial model: a plain dict plus a full snapshot per seq."""

    def __init__(self):
        self.cur = {}
        self.seq = 0
        self.snaps = {0: {}}

    def commit(self, ops):
        self.seq += 1
        for op in ops:
            if op[0] == "put":
                _, tenant, key, value = op
                self.cur[(tenant, key)] = value
            else:
                _, tenant, key = op
                self.cur.pop((tenant, key), None)
        self.snaps[self.seq] = dict(self.cur)
        return self.seq

    def get(self, seq, tenant, key):
        return self.snaps[seq].get((tenant, key))

    def scan(self, seq, lo=None, hi=None):
        items = sorted(self.snaps[seq].items(),
                       key=lambda kv: (kv[0][0].encode("utf-8"), kv[0][1]))
        out = []
        for (tenant, key), value in items:
            if lo is not None and (tenant.encode(), key) < (lo[0].encode(), lo[1]):
                continue
            if hi is not None and (tenant.encode(), key) >= (hi[0].encode(), hi[1]):
                continue
            out.append(((tenant, key), value))
        return out


def rows_of(page_rows):
    return [((r.tenant, r.key), (r.text, r.integer, r.blob)) for r in page_rows]


def check_rows(got_rows, expected, what):
    got = rows_of(got_rows)
    if got != expected:
        n = min(len(got), len(expected))
        for i in range(n):
            if got[i] != expected[i]:
                raise Failure(f"{what}: first diff at #{i}: "
                              f"got {got[i]!r} expected {expected[i]!r} "
                              f"(got {len(got)} rows, expected {len(expected)})")
        raise Failure(f"{what}: length differs: got {len(got)} "
                      f"expected {len(expected)}")


# ------------------------------------------------------------- server mgmt
class ServerProc:
    def __init__(self, directory):
        self.dir = directory
        self.sock = os.path.join(directory, "store.sock")
        self.log = os.path.join(directory, "server.log")
        self.proc = None

    def start(self):
        os.makedirs(self.dir, exist_ok=True)
        self.logf = open(self.log, "ab", buffering=0)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "shardstore.server",
             "--dir", self.dir, "--sock", self.sock],
            cwd=ROOT, stdout=self.logf, stderr=self.logf)
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise Failure(f"server exited early rc={self.proc.returncode}; "
                              f"log: {self.log}")
            try:
                Store(self.sock, timeout=2.0)._rpc("ping")
                return
            except StoreError:
                time.sleep(0.05)
        raise Failure("server did not start")

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def wait_dead(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                return
            time.sleep(0.05)
        raise Failure("server did not die")

    def kill(self, sig=signal.SIGKILL):
        if self.alive():
            os.kill(self.proc.pid, sig)
        self.proc.wait()

    def stop(self):
        if self.alive():
            try:
                Store(self.sock, timeout=5.0).shutdown()
            except StoreError:
                pass
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.kill()

    def rss_kb(self):
        try:
            with open(f"/proc/{self.proc.pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS"):
                        return int(line.split()[1])
        except OSError:
            pass
        return None


def wait_for(fn, timeout=60, interval=0.05, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(interval)
    raise Failure(f"timeout waiting for {what}")


def wait_published(store, timeout=120):
    wait_for(lambda: store.migration_status()["split"] is not None
             and store.migration_status()["split"]["phase"] == "PUBLISHED",
             timeout=timeout, what="split to publish")


def check_routing_consistent(store, expect_epoch=None):
    rt = store.routing()
    if expect_epoch is not None:
        check(rt["epoch"] == expect_epoch,
              f"epoch: got {rt['epoch']} expected {expect_epoch}")
    routes = rt["routes"]
    check(len(routes) >= 1, "routing must not be empty")
    check(routes[0]["lo"] is None, "first route must start at -infinity")
    check(routes[-1]["hi"] is None, "last route must end at +infinity")
    for i, (a, b) in enumerate(zip(routes, routes[1:])):
        check(a["hi"] is not None and a["hi"] == b["lo"],
              f"routes {i},{i+1} must be adjacent (no hole/overlap): "
              f"{a['hi']} vs {b['lo']}")
    for r in routes:
        check(r["state"] == "active", f"routed shard {r['shard']} not active")
    return rt


def check_visible_seq(store, model):
    st = store.migration_status()
    check(st["visible_seq"] == model.seq,
          f"visible_seq {st['visible_seq']} != model seq {model.seq}")


# ================================================================= phases
def phase_basic(srv):
    """Types, ordering, dedup, conflicts, bounded scans, token handling."""
    store = Store(srv.sock)
    model = Model()
    tenants = ["", "a", "ab", "b", "é", "中文"]
    keys = [-(2**63), -1000, -1, 0, 1, 7, 2**63 - 1]
    blob_full = bytes(range(256))
    receipts = {}

    # spread of rows exercising NULL / exact int64 / BLOB / empty values
    n = 0
    ops = []
    for tenant in tenants:
        for k in keys:
            n += 1
            text = None if n % 4 == 0 else ("" if n % 7 == 0 else f"r-{tenant}-{k}")
            integer = None if n % 4 == 1 else (2**63 - 1 if n % 5 == 0
                                               else -(2**63) if n % 5 == 1 else k)
            blob = None if n % 4 == 2 else (b"" if n % 3 == 0 else blob_full[:n % 64])
            ops.append(put(tenant, k, text=text, integer=integer, blob=blob))
            if len(ops) == 9:
                rid = f"basic-{len(receipts)}"
                receipts[rid] = (store.write(rid, ops), ops)
                check(receipts[rid][0]["commit_seq"] == model.commit(ops),
                      "commit_seq must match the serial model")
                ops = []
    if ops:
        rid = f"basic-{len(receipts)}"
        receipts[rid] = (store.write(rid, ops), ops)
        check(receipts[rid][0]["commit_seq"] == model.commit(ops), "seq alignment")

    # deletes: existing rows, missing rows (no-op), and a re-put
    ops = [delete("a", -1), delete("a", -1), delete("no/such", 12345),
           put("b", 0, text="back", integer=0, blob=b"\x00")]
    rid = f"basic-{len(receipts)}"
    receipts[rid] = (store.write(rid, ops), ops)
    check(receipts[rid][0]["commit_seq"] == model.commit(ops), "seq alignment")

    # retransmission of every request returns the identical receipt
    for rid, (rcpt, rops) in receipts.items():
        again = store.write(rid, rops)
        check(again == rcpt, f"retransmission of {rid} must return the receipt")
    check_visible_seq(store, model)

    # same request_id with a different payload is rejected
    try:
        store.write("basic-0", [put("x", 1)])
        raise Failure("conflicting request_id was accepted")
    except StoreError as e:
        check(e.code == "CONFLICT", f"expected CONFLICT, got {e.code}")
    check_visible_seq(store, model)

    # full scan in pages matches the model snapshot exactly, in total order
    tok = store.open_read()
    check(tok.read_seq == model.seq, "token must pin the latest seq")
    got = list(store.scan_all(tok, page=5))
    check_rows(got, model.scan(model.seq), "full scan")
    # ordering is (tenant utf-8 bytes, key)
    got_keys = [(r.tenant.encode("utf-8"), r.key) for r in got]
    check(got_keys == sorted(got_keys), "scan must follow the composite order")

    # bounded scans
    lo, hi = ("a", -1), ("b", 7)
    got = list(store.scan_all(tok, lo=lo, hi=hi, page=3))
    check_rows(got, model.scan(model.seq, lo=lo, hi=hi), "bounded scan")
    got = list(store.scan_all(tok, lo=("", 0), hi=("b", 0), page=4))
    check_rows(got, model.scan(model.seq, lo=("", 0), hi=("b", 0)), "bounded scan 2")

    # point reads: present, deleted, missing; NULL/BLOB/int64 fidelity
    for tenant in tenants:
        for k in keys:
            row = store.get(tok, tenant, k)
            exp = model.get(model.seq, tenant, k)
            if exp is None:
                check(row is None, f"get({tenant!r},{k}) should be missing")
            else:
                check(row is not None and row.value() == exp,
                      f"get({tenant!r},{k}): {row!r} != {exp!r}")
    check(store.get(tok, "a", -1) is None, "deleted row must stay deleted")
    row = store.get(tok, "b", 0)
    check(row is not None and row.text == "back" and row.blob == b"\x00",
          "re-put after delete must win")

    # value fidelity spot checks
    row = store.get(tok, "a", 1)
    exp = model.get(model.seq, "a", 1)
    check(row.blob == exp[2], "blob round trip")
    row = store.get(tok, "", 2**63 - 1)
    exp = model.get(model.seq, "", 2**63 - 1)
    check(row.integer == exp[1] and isinstance(row.integer, int),
          "int64 round trip")

    store.release(tok)
    try:
        store.get(tok, "a", 1)
        raise Failure("released token still works")
    except StoreError as e:
        check(e.code == "BAD_TOKEN", f"expected BAD_TOKEN, got {e.code}")
    print(f"    basic: {len(receipts)} writes, {len(model.cur)} live rows ok")


def phase_main_drill(srv):
    """The specified scenario with a paused copy window."""
    store = Store(srv.sock)
    model = Model()

    # write keys 1..6
    ops = [put(T, k, text=f"v{k}", integer=k, blob=bytes([k])) for k in range(1, 7)]
    check(store.write("drill-w1", ops)["commit_seq"] == model.commit(ops), "seq")

    # open the old scan and read the first page (1, 2)
    tok_old = store.open_read()
    old_seq = tok_old.read_seq
    page = store.scan(tok_old, limit=2)
    check_rows(page.rows, model.scan(old_seq)[:2], "old scan first page")
    check(not page.done and page.next_cursor == (T, 2), "cursor after page 1")
    cursor = page.next_cursor

    # pause the copy worker, then request the split: the migration window
    # genuinely exists while we write
    store.pause_copy(True)
    src = store.routing()["routes"][0]["shard"]
    store.split(src, (T, 4))
    st = store.migration_status()
    check(st["split"]["phase"] == "COPYING", "split must be COPYING while paused")

    # one transaction: change key 2, delete key 4, insert key 7
    ops2 = [put(T, 2, text="v2-new", integer=22, blob=b"\x02\x02"),
            delete(T, 4),
            put(T, 7, text="v7", integer=7, blob=b"\x07")]
    rcpt = store.write("drill-w2", ops2)
    check(rcpt["commit_seq"] == model.commit(ops2),
          "transaction must commit while the copy is paused")
    check(store.migration_status()["split"]["phase"] == "COPYING",
          "write happened inside the migration window")

    # a new read sees all three changes of that one commit together
    tok_new = store.open_read()
    check(tok_new.read_seq == model.seq, "new token at the new frontier")
    check(store.get(tok_new, T, 2).text == "v2-new", "new read: key 2 updated")
    check(store.get(tok_new, T, 4) is None, "new read: key 4 deleted")
    check(store.get(tok_new, T, 7) is not None, "new read: key 7 inserted")
    # the old token still sees the old commit, unmixed
    check(store.get(tok_old, T, 2).text == "v2", "old token: old value of 2")
    check(store.get(tok_old, T, 4) is not None, "old token: key 4 still there")
    check(store.get(tok_old, T, 7) is None, "old token: key 7 not visible")

    # let the copy run to the cutover
    store.pause_copy(False)
    wait_published(store)
    st = store.migration_status()
    check(st["epoch"] == 2, "exactly one epoch switch")
    check_routing_consistent(store, expect_epoch=2)
    fronts = {s["shard_id"]: s["frontier"] for s in st["shards"]}
    check(fronts[st["split"]["left"]] == fronts[st["split"]["right"]]
          == model.seq, "both targets reached the same frontier")

    # the old scan's remainder crosses the cutover and still returns the
    # old snapshot: 3, 4, 5, 6 with the old values
    rest = []
    while True:
        page = store.scan(tok_old, cursor=cursor, limit=2)
        rest.extend(page.rows)
        if page.done:
            break
        cursor = page.next_cursor
    check_rows(rest, model.scan(old_seq)[2:], "old scan remainder")
    check([k for (_, k), _ in rows_of(rest)] == [3, 4, 5, 6],
          "old scan remainder keys must be 3,4,5,6")

    # a fresh scan sees 1, 2', 3, 5, 6, 7 across both new shards
    tok_after = store.open_read()
    got = list(store.scan_all(tok_after, page=2))
    check_rows(got, model.scan(model.seq), "post-split full scan")
    check([k for (_, k), _ in rows_of(got)] == [1, 2, 3, 5, 6, 7],
          "post-split keys must be 1,2,3,5,6,7")

    # the active old token blocks reclamation of the retired source
    rep = store.reclaim()
    check(src in rep["blocked"] and src not in rep["reclaimed"],
          f"old token must block reclaim: {rep}")
    check(os.path.exists(os.path.join(srv.dir, "shards", src + ".db")),
          "source shard file must survive while a token needs it")

    # after release the source can be reclaimed
    store.release(tok_old)
    store.release(tok_new)
    store.release(tok_after)
    rep = store.reclaim()
    check(src in rep["reclaimed"], f"reclaim after release: {rep}")
    check(not os.path.exists(os.path.join(srv.dir, "shards", src + ".db")),
          "source shard file must be removed after reclaim")
    check_routing_consistent(store, expect_epoch=2)
    check_visible_seq(store, model)
    print("    main drill: paused-copy transaction, cutover, old/new scans, "
          "reclaim gating ok")


def phase_crash_after_decision(srv):
    """Kill the server between the durable commit decision and the reply."""
    store = Store(srv.sock)
    model = Model()
    ops0 = [put(T, k, text=f"seed{k}") for k in range(10)]
    check(store.write("seed", ops0)["commit_seq"] == model.commit(ops0), "seq")

    store.arm_crash("after_decision")
    ops = [put(T, 3, text="lost?"), put(T, 100, text="new"), delete(T, 4)]
    try:
        store.write("lost-write", ops)
        raise Failure("write should have lost its reply")
    except StoreError as e:
        check(e.code == "CONN", f"expected CONN, got {e.code}")
    srv.wait_dead()

    srv.start()
    store = Store(srv.sock)
    # the decision was durable: the retry gets the original receipt, the
    # write is applied exactly once, and no commit_seq is reused
    rcpt = store.write("lost-write", ops)
    check(rcpt["commit_seq"] == model.seq + 1, "retry returns the decided seq")
    model.commit(ops)
    check_visible_seq(store, model)
    tok = store.open_read()
    check(store.get(tok, T, 3).text == "lost?", "decided write must be applied")
    check(store.get(tok, T, 4) is None, "delete of the lost write applied")
    check(store.get(tok, T, 100) is not None, "insert of the lost write applied")
    check_rows(list(store.scan_all(tok, page=3)), model.scan(model.seq), "scan")
    store.release(tok)
    again = store.write("lost-write", ops)
    check(again == rcpt, "re-retransmission must return the same receipt")
    ops2 = [put(T, 200)]
    check(store.write("next", ops2)["commit_seq"] == model.commit(ops2),
          "next write gets a fresh seq")
    print("    crash after decision: retry converges to the unique receipt ok")


def _seed(store, model, rows, tag):
    """Seed `rows` keys for tenant T in chunks; deterministic values.
    Returns {request_id: (receipt, ops)} for later retransmission."""
    receipts = {}
    k = 0
    wid = 0
    while k < rows:
        ops = []
        for _ in range(min(500, rows - k)):
            k += 1
            ops.append(put(T, k, text=f"{tag}{k}", integer=k,
                           blob=hashlib.sha256(f"{tag}{k}".encode()).digest()[:8]))
        wid += 1
        rid = f"{tag}-w{wid}"
        rcpt = store.write(rid, ops)
        check(rcpt["commit_seq"] == model.commit(ops), "seed seq alignment")
        receipts[rid] = (rcpt, ops)
    return receipts


def _verify_store(store, model, what):
    check_visible_seq(store, model)
    tok = store.open_read()
    check_rows(list(store.scan_all(tok, page=499)), model.scan(model.seq), what)
    store.release(tok)


def _retransmit_seed(store, receipts):
    """Retransmit the first and last seed requests; receipts must be
    identical and nothing may be re-executed."""
    rids = list(receipts)
    for rid in (rids[0], rids[-1]):
        rcpt, ops = receipts[rid]
        again = store.write(rid, ops)
        check(again == rcpt, f"retransmission of {rid} must return the receipt")
        try:
            store.write(rid, ops + [delete(T, -1)])
            raise Failure(f"conflicting {rid} accepted")
        except StoreError as e:
            check(e.code == "CONFLICT", f"expected CONFLICT, got {e.code}")


def phase_split_crash(srv, point, rows=5000):
    """One crash round: seed, arm the crash point, split, die, recover."""
    store = Store(srv.sock)
    model = Model()
    receipts = _seed(store, model, rows, point.replace("_", "-"))
    src = store.routing()["routes"][0]["shard"]
    store.arm_crash(point)
    store.split(src, (T, rows // 2))
    srv.wait_dead()

    srv.start()
    store = Store(srv.sock)
    wait_published(store)
    check_routing_consistent(store, expect_epoch=2)
    st = store.migration_status()
    check(st["split"]["phase"] == "PUBLISHED", f"split must publish after {point}")
    fronts = {s["shard_id"]: s["frontier"] for s in st["shards"]}
    sp = st["split"]
    check(fronts[sp["left"]] == fronts[sp["right"]] == model.seq,
          "targets hold the same frontier after recovery")

    # no confirmed write lost, nothing re-executed
    _verify_store(store, model, f"scan after {point}")
    _retransmit_seed(store, receipts)

    # interleave a retransmission with a cross-shard transaction
    left_key, right_key = rows // 4, rows - rows // 4
    ops = [put(T, left_key, text="x-left"), put(T, right_key, text="x-right"),
           delete(T, rows // 2 + 1)]
    rcpt = store.write(f"xshard-{point}", ops)
    check(rcpt["commit_seq"] == model.commit(ops), "cross-shard seq")
    tok = store.open_read()
    check(store.get(tok, T, left_key).text == "x-left", "left side visible")
    check(store.get(tok, T, right_key).text == "x-right", "right side visible")
    check(store.get(tok, T, rows // 2 + 1) is None, "cross-shard delete visible")
    store.release(tok)
    again = store.write(f"xshard-{point}", ops)
    check(again == rcpt, "cross-shard retransmission returns the same receipt")
    _verify_store(store, model, f"final scan after {point}")
    print(f"    crash at {point}: recovered, split completed, dedup ok")


def phase_crash_during_reclaim(srv):
    store = Store(srv.sock)
    model = Model()
    _seed(store, model, 1500, "reclaim")
    src = store.routing()["routes"][0]["shard"]
    store.split(src, (T, 750))
    wait_published(store)
    store.arm_crash("during_reclaim")
    try:
        store.reclaim()
    except StoreError as e:
        check(e.code == "CONN", f"expected CONN, got {e.code}")
    srv.wait_dead()

    srv.start()
    store = Store(srv.sock)
    # recovery finishes (or safely redoes) the interrupted reclamation
    wait_for(lambda: store.migration_status()["split"]["phase"] == "RECLAIMED",
             what="reclaim to finish after restart")
    rep = store.reclaim()
    check(src in rep["reclaimed"] or
          all(s["shard_id"] != src or s["state"] == "reclaimed"
              for s in store.migration_status()["shards"]),
          f"source must end reclaimed: {rep}")
    check(not os.path.exists(os.path.join(srv.dir, "shards", src + ".db")),
          "source file gone after reclaim")
    _verify_store(store, model, "scan after reclaim crash")
    print("    crash during reclaim: recovery finishes it exactly once ok")


def phase_sigkill(srv):
    """A real SIGKILL mid-copy; a read token survives the restart."""
    store = Store(srv.sock)
    model = Model()
    _seed(store, model, 20000, "sigkill")
    tok_old = store.open_read()
    old_seq = tok_old.read_seq
    src = store.routing()["routes"][0]["shard"]
    store.split(src, (T, 10000))
    time.sleep(0.3)  # let the copy get going, then pull the plug
    srv.kill(signal.SIGKILL)

    srv.start()
    store = Store(srv.sock)
    wait_published(store)
    check_routing_consistent(store, expect_epoch=2)
    # the token opened before the kill still reads its snapshot
    check(tok_old.read_seq == old_seq, "token seq stable")
    got = store.get(tok_old, T, 5)
    check(got is not None and got.value() == model.get(old_seq, T, 5),
          "old token still reads its snapshot")
    check_rows(list(store.scan_all(tok_old, page=777)), model.scan(old_seq),
               "old token full scan after SIGKILL")
    store.release(tok_old)
    _verify_store(store, model, "post-SIGKILL scan")
    print("    SIGKILL mid-copy: split resumed, pre-kill token still valid ok")


def phase_validation_and_race(srv):
    store = Store(srv.sock)
    model = Model()
    _seed(store, model, 100, "valid")
    src = store.routing()["routes"][0]["shard"]
    store.split(src, (T, 4))
    wait_published(store)
    before = store.routing()

    def expect_error(code, fn, what):
        try:
            fn()
            raise Failure(f"{what}: should have been rejected")
        except StoreError as e:
            check(e.code == code, f"{what}: expected {code}, got {e.code}")

    expect_error("UNKNOWN_SHARD", lambda: store.split("s999", (T, 1)),
                 "unknown shard")
    expect_error("UNKNOWN_SHARD", lambda: store.split(src, (T, 1)),
                 "retired shard")
    left = before["routes"][0]["shard"]
    right = before["routes"][1]["shard"]
    expect_error("BAD_BOUNDARY", lambda: store.split(left, (T, 4)),
                 "boundary == shard hi")
    expect_error("BAD_BOUNDARY", lambda: store.split(left, (T, 99)),
                 "boundary outside shard")
    expect_error("BAD_BOUNDARY", lambda: store.split(right, (T, 4)),
                 "boundary == shard lo (duplicate range)")
    check(store.routing() == before, "rejected splits must not change routing")

    # two processes race one split slot: exactly one wins
    store.pause_copy(True)
    q = mp.Queue()
    procs = [mp.Process(target=_child_split, args=(srv.sock, left, (T, 2), q)),
             mp.Process(target=_child_split, args=(srv.sock, left, (T, 3), q))]
    for p in procs:
        p.start()
    results = [q.get(timeout=60) for _ in procs]
    for p in procs:
        p.join(60)
    store.pause_copy(False)
    oks = [r for r in results if r[0] == "ok"]
    errs = [r for r in results if r[0] == "err"]
    check(len(oks) == 1 and len(errs) == 1 and errs[0][1] == "SPLIT_IN_PROGRESS",
          f"race must produce exactly one winner: {results}")
    wait_published(store)
    check_routing_consistent(store, expect_epoch=3)
    _verify_store(store, model, "scan after racing splits")
    print("    validation + racing splits: rejections clean, one winner ok")


def _child_split(sock, shard, boundary, q):
    try:
        store = Store(sock)
        r = store.split(shard, boundary)
        q.put(("ok", r["split_id"]))
    except StoreError as e:
        q.put(("err", e.code))
    except Exception as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _child_reader(sock, token_id, lo, hi, expect_count, q):
    """Child process: scan a pinned token, check order + deterministic values."""
    try:
        store = Store(sock)
        from shardstore.client import ReadToken
        tok = ReadToken(store, token_id, None, None)
        n = 0
        prev = None
        for row in store.scan_all(tok, lo=lo, hi=hi, page=997):
            kk = (row.tenant.encode("utf-8"), row.key)
            if prev is not None and kk <= prev:
                q.put(("err", f"order violation at {kk}"))
                return
            prev = kk
            if row.value() != _scale_value(row.tenant, row.key):
                q.put(("err", f"value mismatch at {row.tenant}/{row.key}"))
                return
            n += 1
        store.release(tok)
        q.put(("ok", n, expect_count))
    except Exception as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _scale_value(tenant, k):
    return (f"txt:{tenant}:{k}", (k * 31) % (2**40),
            hashlib.sha256(f"{tenant}|{k}".encode()).digest()[:12])


def phase_scale(srv):
    store = Store(srv.sock)
    tenants = ["t0", "t1", "t2", "t3"]
    per = SCALE_ROWS // len(tenants)

    # runtime-generated scale data, deterministic values
    wid = 0
    for tenant in tenants:
        k = 0
        while k < per:
            ops = []
            for _ in range(min(1000, per - k)):
                ops.append(put(tenant, k, *_scale_value(tenant, k)))
                k += 1
            wid += 1
            store.write(f"scale-w{wid}", ops)
    seed_seq = store.migration_status()["visible_seq"]

    # two child processes pin read tokens before the split and scan the
    # whole range while the copy runs and new writes land
    tokens = [store.open_read() for _ in range(2)]
    check(all(t.read_seq == seed_seq for t in tokens), "reader tokens pinned")
    q = mp.Queue()
    readers = [mp.Process(target=_child_reader,
                          args=(srv.sock, tokens[0].token_id, ("t0", 0), ("t2", 0),
                                2 * per, q)),
               mp.Process(target=_child_reader,
                          args=(srv.sock, tokens[1].token_id, ("t2", 0), ("t4", 0),
                                2 * per, q))]
    for p in readers:
        p.start()

    baseline = srv.rss_kb()
    src = store.routing()["routes"][0]["shard"]
    store.split(src, ("t2", 0))

    # writes keep landing while the copy runs (new keys, both sides)
    extra = 0
    for tenant in ("t1", "t2"):
        ops = []
        for k in range(per, per + 500):
            ops.append(put(tenant, k, *_scale_value(tenant, k)))
            extra += 1
        store.write(f"scale-extra-{tenant}", ops)

    max_rss = baseline or 0
    while True:
        st = store.migration_status()
        rss = srv.rss_kb()
        if rss:
            max_rss = max(max_rss, rss)
        if st["split"]["phase"] == "PUBLISHED":
            break
        time.sleep(0.05)

    results = [q.get(timeout=300) for _ in readers]
    for p in readers:
        p.join(300)
    for r in results:
        check(r[0] == "ok", f"child reader failed: {r}")
        check(r[1] == r[2], f"child reader count {r[1]} != {r[2]}")

    # full scan after cutover: seed + extra, in order, values intact
    tok = store.open_read()
    n = 0
    prev = None
    for row in store.scan_all(tok, page=2000):
        kk = (row.tenant.encode("utf-8"), row.key)
        check(prev is None or kk > prev, "order violation after split")
        prev = kk
        check(row.value() == _scale_value(row.tenant, row.key),
              f"value mismatch at {row.tenant}/{row.key}")
        n += 1
    store.release(tok)
    check(n == SCALE_ROWS + extra, f"row count {n} != {SCALE_ROWS + extra}")

    if baseline:
        delta = max_rss - baseline
        check(delta < RSS_BUDGET_KB,
              f"copy memory growth {delta}kB exceeds budget {RSS_BUDGET_KB}kB")
        print(f"    scale: {n} rows, copy RSS growth {delta}kB "
              f"(budget {RSS_BUDGET_KB}kB), 2 child readers ok")
    else:
        print(f"    scale: {n} rows, 2 child readers ok")

    # release the pre-split tokens; the retired source becomes reclaimable
    for t in tokens:
        try:
            store.release(t)
        except StoreError:
            pass  # a child may have released it already
    rep = store.reclaim()
    check(src in rep["reclaimed"], f"scale source reclaimed: {rep}")
    check_routing_consistent(store, expect_epoch=2)


# ================================================================== main
PHASES = [
    ("basic types/order/dedup", phase_basic),
    ("main drill", phase_main_drill),
    ("crash after decision", phase_crash_after_decision),
    ("crash during copy", lambda s: phase_split_crash(s, "during_copy")),
    ("crash after copy", lambda s: phase_split_crash(s, "after_copy")),
    ("crash after catchup", lambda s: phase_split_crash(s, "after_catchup")),
    ("crash before publish", lambda s: phase_split_crash(s, "before_publish")),
    ("crash after publish", lambda s: phase_split_crash(s, "after_publish")),
    ("crash during reclaim", phase_crash_during_reclaim),
    ("SIGKILL mid-copy", phase_sigkill),
    ("validation + racing splits", phase_validation_and_race),
    ("scale + bounded memory + multiprocess", phase_scale),
]


def main():
    keep = os.environ.get("KEEP") == "1"
    only = os.environ.get("ONLY")
    base = tempfile.mkdtemp(prefix="shardstore-acc-")
    print(f"workdir: {base}")
    failed = None
    t0 = time.time()
    try:
        for name, fn in PHASES:
            if only and only not in name:
                continue
            print(f"== {name}")
            srv = ServerProc(os.path.join(base, name.replace(" ", "_")))
            start = time.time()
            try:
                srv.start()
                fn(srv)
            except Exception as e:
                failed = (name, e, srv)
                raise
            finally:
                srv.stop()
            print(f"   ok ({time.time() - start:.1f}s)")
    except Exception:
        if failed:
            name, err, srv = failed
            print(f"\nFAILED: {name}: {err}", file=sys.stderr)
            try:
                with open(srv.log, "rb") as f:
                    tail = f.read()[-4000:].decode("utf-8", "replace")
                print(f"--- server log tail ({srv.log}) ---\n{tail}",
                      file=sys.stderr)
            except OSError:
                pass
        else:
            print("\nFAILED", file=sys.stderr)
        raise SystemExit(1)
    finally:
        if not keep:
            shutil.rmtree(base, ignore_errors=True)
        else:
            print(f"kept workdir: {base}")
    print(f"\nALL CHECKS PASSED ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
