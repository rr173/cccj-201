#!/usr/bin/env python3
"""End-to-end acceptance drill for shardstore change subscriptions.

Runs the real server as a subprocess and drives it from this process plus
extra client child processes over the unix socket.  Every expectation is
derived from an independent serial commit model (a list of commits, each a
list of ops exactly as the caller submitted them); the model never calls the
subscription or cleanup implementation.

Phases:
  1. delivery     order, unit boundaries, caller op order, duplicate keys,
                  deletes of missing keys, byte/count limits, oversize commit
  2. generations  repeated pull is byte identical, dedup, ack retries, slow
                  vs fast subscribers, pause/resume, replay boundaries
  3. cleanup      boundary gating by unconfirmed subscribers, retained-range
                  errors, request_id receipts surviving payload release
  4. crashes      stop the server during pull, ack and cleanup; cursors,
                  generations, replay range and receipts must survive
  5. multi        several child processes contend for one subscription; one
                  valid generation, stale acks rejected
  6. split        subscriptions continue across an online split (cross-shard
                  commits, duplicate keys, deletes of missing keys)
  7. backfill     runtime-generated backlog paged with bounded results
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
T = "t"
BACKLOG = int(os.environ.get("SUB_BACKLOG", "400"))


class Failure(Exception):
    pass


def check(cond, msg):
    if not cond:
        raise Failure(msg)


# ------------------------------------------------------------------ model
class CommitModel:
    """Independent serial commit model: an ordered list of (request_id, ops)
    together with the running row state.  Snapshots are not used; the
    expected *change stream* is simply the original op lists."""

    def __init__(self):
        self.commits = []          # [(seq, request_id, ops)]
        self.state = {}

    def commit(self, request_id, ops):
        seq = len(self.commits) + 1
        self.commits.append((seq, request_id, [tuple(o) for o in ops]))
        for op in ops:
            if op[0] == "put":
                self.state[(op[1], op[2])] = op[3]
            else:
                self.state.pop((op[1], op[2]), None)
        return seq

    @property
    def seq(self):
        return len(self.commits)

    def stream(self, after=0, through=None):
        through = self.seq if through is None else through
        return [c for c in self.commits if after < c[0] <= through]


def check_delivery(page, expected, what):
    """A pull page must be exactly `expected`: same commits, same order,
    same caller op order, no merged or split units."""
    got = [(c.commit_seq, c.request_id, [tuple(o) for o in c.ops])
           for c in page.changes]
    if got != expected:
        n = min(len(got), len(expected))
        for i in range(n):
            if got[i] != expected[i]:
                raise Failure(f"{what}: commit #{i} differs:\n"
                              f"  got      {got[i]!r}\n"
                              f"  expected {expected[i]!r}")
        raise Failure(f"{what}: length {len(got)} != {len(expected)}")
    seqs = [c.commit_seq for c in page.changes]
    check(seqs == sorted(seqs) and len(set(seqs)) == len(seqs),
          f"{what}: commit_seq must strictly increase")


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


def expect_error(code, fn, what):
    try:
        fn()
    except StoreError as e:
        check(e.code == code, f"{what}: expected {code}, got {e.code}: {e}")
        return e
    raise Failure(f"{what}: should have been rejected with {code}")


def drain(store, name, gen, page=None, what="drain"):
    """Pull and confirm everything currently available; return the commits."""
    out = []
    while True:
        p = page() if page is not None else store.pull(name, gen)
        check_delivery(p, [(c.commit_seq, c.request_id, [tuple(o) for o in
                                                        c.ops]) for c in p],
                       f"{what} internal")
        out.extend(p.changes)
        if len(p.changes):
            store.ack(name, gen, p.next_after)
        if not p.has_more:
            return out


# ================================================================= phases
def phase_delivery(srv):
    """Order, unit boundaries, op order, duplicate keys, limits, oversize."""
    store = Store(srv.sock)
    model = CommitModel()

    model.commit("seed", [put(T, 1, text="old")])
    store.write("seed", model.commits[-1][2])

    sub = store.subscribe("deliver", 0)
    check(sub["origin_seq"] == 0 and sub["generation"] == 1, "fresh sub")
    gen = sub["generation"]

    # duplicate keys inside one commit, deletes of missing keys, type spread
    ops_a = [put(T, 10, text="a"), put(T, 10, text="b"),
             delete(T, 11), delete(T, 11), put(T, 11, blob=b"\x00\xff\x00"),
             put(T, 12, integer=-(2**63)), put(T, 13, integer=2**63 - 1)]
    store.write("c-a", ops_a)
    check(model.commit("c-a", ops_a) == 2, "model seq")
    ops_b = [delete("no-such-tenant", -1), delete(T, 2**63 - 1),
             put("zz", -1000, text="", integer=0, blob=b"")]
    store.write("c-b", ops_b)
    model.commit("c-b", ops_b)

    p = store.pull("deliver", gen, max_commits=100)
    expected = [(seq, rid, ops) for seq, rid, ops in model.stream(0)]
    check_delivery(p, expected, "first pull")
    check(p.next_after == model.seq and not p.has_more, "pulled everything")
    check(p.bytes > 0, "byte accounting reported")

    # a repeated pull before ack returns identical commits and bytes, whether
    # it goes through the same connection or a fresh client object
    p2 = store.pull("deliver", gen, max_commits=100)
    check_delivery(p2, expected, "repeated pull")
    check([c.size_bytes for c in p2] == [c.size_bytes for c in p],
          "repeated pull must report identical sizes")
    p3 = Store(srv.sock).pull("deliver", gen, max_commits=100)
    check_delivery(p3, expected, "repeated pull from a new client")
    check([(c.commit_seq, c.ops) for c in p3] == [(c.commit_seq, c.ops)
                                                  for c in p],
          "delivery must be byte-identical across client reconnects")

    # per-op order is preserved inside the unit (a, b, del, del, put, ...)
    check([o[0] for o in p.changes[1].ops] == ["put", "put", "del", "del",
                                               "put", "put", "put"],
          "caller op order must be preserved inside a commit")
    check(p.changes[1].ops[0] != p.changes[1].ops[1],
          "duplicate-key ops must not be collapsed")

    # count and byte limits break between units only
    store.ack("deliver", gen, model.seq)
    ops_c = [put(T, 20, text="x" * 200)]
    store.write("c-c", ops_c)
    check(model.commit("c-c", ops_c) == 4, "model seq c-c")
    ops_d = [put(T, 21, text="y" * 200)]
    store.write("c-d", ops_d)
    check(model.commit("c-d", ops_d) == 5, "model seq c-d")

    p = store.pull("deliver", gen, max_commits=1)
    check(len(p.changes) == 1 and p.changes[0].commit_seq == 4, "count limit")
    check(p.has_more, "more must be reported after a count-limited page")
    p_same = store.pull("deliver", gen, max_commits=1)
    check([c.commit_seq for c in p_same] == [4],
          "count-limited retry is stable")
    store.ack("deliver", gen, 4)
    p = store.pull("deliver", gen, max_commits=5, max_bytes=1)
    check([c.commit_seq for c in p.changes] == [5],
          "a single commit must be delivered whole even over the byte budget")
    check(p.over_budget and p.oversize_seq == 5,
          "over-budget delivery must be explicitly observable")
    check(p.changes[0].over_budget, "the unit is flagged over_budget")
    store.ack("deliver", gen, 5)

    # an oversize commit does not swallow the following one into one unit
    ops_e = [put(T, 22, text="q")]
    store.write("c-e", ops_e)
    check(model.commit("c-e", ops_e) == 6, "model seq c-e")
    p = store.pull("deliver", gen, max_bytes=1)
    check([c.commit_seq for c in p.changes] == [6],
          "next pull must start a new unit after the oversize one")

    # tuple ops from the client are usable as the original request again
    check(p.changes[0].ops == ops_e, "decoded ops round trip")

    # the model state still matches the store's reads
    tok = store.open_read()
    for (tenant, key), value in model.state.items():
        row = store.get(tok, tenant, key)
        check(row is not None and row.value() == value,
              f"row {tenant}/{key} must match the model")
    store.release(tok)
    print(f"    delivery: {model.seq} commits, units and op order intact")


def phase_generations(srv):
    """Repeated pull, dedup, generation takeover, pause/resume, replay."""
    store = Store(srv.sock)
    model = CommitModel()
    model.commit("g1", [put(T, 1, text="one")])
    store.write("g1", model.commits[-1][2])
    model.commit("g2", [put(T, 2, text="two")])
    store.write("g2", model.commits[-1][2])

    a = store.subscribe("gen", 0)
    gen1 = a["generation"]
    p = store.pull("gen", gen1, max_commits=1)
    check([c.commit_seq for c in p.changes] == [1], "first commit only")

    # re-registering the same name with the same origin is not a reset
    again = store.subscribe("gen", 0)
    check(again["acked_seq"] == 0 and again["origin_seq"] == 0,
          "re-registering with the same after_seq must not reset progress")
    gen2 = again["generation"]
    check(gen2 != gen1, "re-registering starts a new consumption generation")

    # the delayed old generation can no longer advance the cursor
    expect_error("STALE_GENERATION", lambda: store.ack("gen", gen1, 2),
                 "stale ack")
    expect_error("STALE_GENERATION", lambda: store.pull("gen", gen1),
                 "stale pull")

    # ack retries are idempotent and only the contiguous delivered prefix moves
    p = store.pull("gen", gen2)
    check([c.commit_seq for c in p.changes] == [1, 2], "new generation resumes")
    expect_error("BAD_ACK", lambda: store.ack("gen", gen2, 3),
                 "ack past the delivered prefix")
    check(store.subscribe_status("gen")["acked_seq"] == 0,
          "a rejected ack must not move the cursor")
    check(store.ack("gen", gen2, 2)["acked_seq"] == 2, "contiguous ack")
    check(store.ack("gen", gen2, 2)["acked_seq"] == 2, "ack retry")
    expect_error("BAD_ACK", lambda: store.ack("gen", gen2, 1),
                 "ack moving backwards")
    check(store.subscribe_status("gen")["acked_seq"] == 2,
          "backwards ack must not move the cursor")

    # a different origin for an existing name is refused outright
    expect_error("SUBSCRIPTION_EXISTS", lambda: store.subscribe("gen", 1),
                 "re-register with a different origin")
    check(store.subscribe_status("gen")["acked_seq"] == 2,
          "refused re-registration must not reset progress")

    # replay from a retained position re-delivers the same bytes, cursor intact
    rp = store.replay("gen", gen2, 0, max_commits=10)
    check_delivery(rp, [(s, r, o) for s, r, o in model.stream(0)],
                   "full replay")
    check(store.subscribe_status("gen")["acked_seq"] == 2,
          "replay must not move the cursor")
    expect_error("OUT_OF_RANGE", lambda: store.replay("gen", gen2, 99),
                 "replay past the delivered range")
    expect_error("OUT_OF_RANGE", lambda: store.replay("gen", gen2, -1),
                 "replay before the origin")
    # replay can page and stops at the delivered end without moving cursors
    first = store.replay("gen", gen2, 0, max_commits=2)
    check([c.commit_seq for c in first.changes] == [1, 2], "replay page 1")
    check(not first.has_more, "replay ends at the delivered end")
    # paging a longer replay is still exact
    model.commit("g2b", [put(T, 21, text="extra")])
    store.write("g2b", model.commits[-1][2])
    store.pull("gen", gen2)
    first = store.replay("gen", gen2, 0, max_commits=2)
    check([c.commit_seq for c in first.changes] == [1, 2], "replay page 1")
    check(first.has_more, "replay reports more")
    second = store.replay("gen", gen2, first.next_after, max_commits=2)
    check([c.commit_seq for c in second.changes] == [3], "replay page 2")
    check(not second.has_more, "replay ends at the delivered end")

    # pause stops delivery without moving either cursor; resume continues
    model.commit("g3", [put(T, 3, text="three")])
    store.write("g3", model.commits[-1][2])
    before = store.subscribe_status("gen")
    check(store.pause("gen", gen2)["paused"], "pause")
    paused = store.pull("gen", gen2)
    check(paused.changes == [] and paused.state["paused"],
          "paused subscription returns no commits")
    check(store.subscribe_status("gen")["delivered_seq"]
          == before["delivered_seq"], "pause must not advance delivery")
    check(store.subscribe_status("gen")["acked_seq"] == before["acked_seq"],
          "pause must not advance the confirmed prefix")
    backlog = store.subscribe_status("gen")["available_commits"]
    check(backlog == 2, f"backlog reported: {backlog}")
    check(store.resume("gen", gen2)["paused"] is False, "resume")
    p = store.pull("gen", gen2)
    check([c.commit_seq for c in p.changes] == [3, 4], "delivery resumes")
    store.ack("gen", gen2, 4)

    # status reports position, range and backlog; the g3 commit arrived
    # after the earlier ack, so it is still outstanding only if not pulled
    st = store.subscribe_status("gen")
    check(st["acked_seq"] == 4 and st["delivered_seq"] == 4, "position")
    check(st["backlog_commits"] == 0, "no backlog after ack")
    check(st["log_low"] == 1 and st["log_high"] == 4, "delivery range")
    print("    generations: stale acks rejected, replay, pause/resume, status")


def phase_slow_fast(srv):
    """A slow subscriber blocks cleanup; a fast one is not held back."""
    store = Store(srv.sock)
    model = CommitModel()
    slow = store.subscribe("slow", 0)
    fast = store.subscribe("fast", 0)
    sg, fg = slow["generation"], fast["generation"]

    for i in range(1, 11):
        ops = [put(T, i, text=f"v{i}")]
        store.write(f"s{i}", ops)
        model.commit(f"s{i}", ops)

    # fast consumes and confirms everything
    fp = drain(store, "fast", fg)
    check([c.commit_seq for c in fp] == list(range(1, 11)), "fast gets all")

    # slow has confirmed only through 3
    sp = store.pull("slow", sg, max_commits=10)
    check([c.commit_seq for c in sp.changes] == list(range(1, 11)),
          "slow pull")
    store.ack("slow", sg, 3)

    # cleanup may not cross the slow subscriber's unconfirmed prefix
    err = expect_error("CLEANUP_BLOCKED", lambda: store.cleanup(5),
                       "cleanup past a slow subscriber")
    check(err.detail.get("safe_up_to") == 3,
          f"cleanup must report the safe bound: {err.detail}")
    check("slow" in err.detail.get("blocked_by", []), "blocker named")
    check(store.cleanup(3)["log_low"] == 4, "cleanup at the safe bound works")
    check(store.cleanup(3)["released_commits"] == 0,
          "cleanup at an already-clean boundary is a stable no-op")

    # the fast subscriber is unaffected by the slow one
    check(store.subscribe_status("fast")["acked_seq"] == 10,
          "fast subscriber unaffected")
    opts = [put(T, 100), delete(T, 101)]
    store.write("f1", opts)
    model.commit("f1", opts)
    fp = store.pull("fast", fg)
    check([c.commit_seq for c in fp.changes] == [11],
          "fast subscriber keeps receiving")
    store.ack("fast", fg, 11)

    # slow still receives from its confirmed prefix, untouched by cleanup
    sp = store.pull("slow", sg)
    check([c.commit_seq for c in sp.changes] == list(range(4, 12)),
          "slow subscriber continues after cleanup below its prefix")
    check(len(sp.changes) == 8, "no change lost by cleanup")
    store.ack("slow", sg, 11)

    # new subscription / replay earlier than the retained range is refused
    err = expect_error("OUT_OF_RANGE", lambda: store.subscribe("late", 0),
                       "new subscription before the retained range")
    check(err.detail.get("min_after") == 3 and err.detail.get("max_after") == 11,
          f"usable range reported: {err.detail}")
    sub = store.subscribe("late", 3)
    check(sub["origin_seq"] == 3, "subscription at the retained range")
    check(sub["log_low"] == 4, "reported retained lower bound")
    print("    slow/fast: slow blocks cleanup, fast unaffected, ranges reported")


def phase_cleanup_receipts(srv):
    """Payload release must not affect request_id dedup receipts."""
    store = Store(srv.sock)
    model = CommitModel()
    sub = store.subscribe("ids", 0)
    gen = sub["generation"]

    ops1 = [put(T, 1, text="r1"), delete(T, 2)]
    r1 = store.write("req-1", ops1)
    model.commit("req-1", ops1)
    ops2 = [put(T, 3, blob=b"\x01\x02")]
    r2 = store.write("req-2", ops2)
    model.commit("req-2", ops2)

    drain(store, "ids", gen)
    rep = store.cleanup(model.seq)
    check(rep["released_commits"] == 2, f"payloads released: {rep}")
    check(rep["log_low"] == 3, "retained range moved")

    # retransmitting the original request still returns the original receipt
    check(store.write("req-1", ops1) == r1, "receipt after payload cleanup")
    check(store.write("req-2", ops2) == r2, "receipt after payload cleanup")
    # conflicting reuse is still refused
    expect_error("CONFLICT", lambda: store.write("req-1", [put(T, 9)]),
                 "conflict after payload cleanup")
    # no re-execution: visible sequence did not move
    st = store.migration_status()
    check(st["visible_seq"] == model.seq, "dedup must not re-execute")

    # a subscription that already confirmed the released commits is fine
    check(store.pull("ids", gen).changes == [], "cleanup did not hurt the sub")
    # replay into released history is refused with the available range
    err = expect_error("OUT_OF_RANGE", lambda: store.replay("ids", gen, 0),
                       "replay into released history")
    check(err.detail.get("min_after") == 2, f"range reported {err.detail}")

    # cleanup beyond the committed log end is refused
    expect_error("OUT_OF_RANGE", lambda: store.cleanup(model.seq + 1),
                 "cleanup beyond the committed end")

    # no subscribers at all: the boundary is stable and initial positions work
    ops3 = [put(T, 4)]
    store.write("req-3", ops3)
    model.commit("req-3", ops3)
    drain(store, "ids", gen)
    store.cleanup(model.seq)
    st_all = store.subscriptions()
    check(st_all["log_low"] == model.seq + 1, "stable empty-log boundary")
    fresh = store.subscribe("fresh", model.seq)
    check(fresh["origin_seq"] == model.seq, "fresh subscription at the end")
    print("    cleanup: receipts survive payload release, ranges reported")


def phase_empty_and_types(srv):
    """Empty stream, signed boundary keys, NULL/BLOB and restart-persisted
    pause.  Also checks the retained-range meaning with no subscribers."""
    store = Store(srv.sock)
    model = CommitModel()

    # entirely empty stream: subscribing at 0 is stable, pulls return nothing
    sub = store.subscribe("empty", 0)
    gen = sub["generation"]
    p = store.pull("empty", gen)
    check(p.changes == [] and p.next_after == 0 and not p.has_more,
          "empty stream pull")
    check(p.bytes == 0, "empty stream bytes")
    check(store.ack("empty", gen, 0)["acked_seq"] == 0, "empty ack at origin")
    expect_error("BAD_ACK", lambda: store.ack("empty", gen, 1),
                 "ack beyond an empty stream")
    rep = store.cleanup(0)
    check(rep["released_commits"] == 0, "cleanup of an empty log")
    check(store.subscribe_status("empty")["log_low"] == 1,
          "empty log lower bound stable")

    # signed int64 boundary keys, empty tenant, NULLs and BLOBs
    ops = [put("", -(2**63), text=None, integer=None, blob=None),
           put("", 2**63 - 1, text="max", integer=2**63 - 1, blob=b"\x00"),
           put("~", -1, text="", integer=-(2**63), blob=bytes(range(256))),
           delete("", -(2**63)),
           delete("~", 0)]
    store.write("types", ops)
    model.commit("types", ops)
    tok = store.open_read()
    got = list(store.scan_all(tok, page=2))
    check([(r.tenant, r.key) for r in got] == [("", 2**63 - 1), ("~", -1)],
          f"scan order with signed boundary keys: {[(r.tenant, r.key) for r in got]}")
    check(got[0].integer == 2**63 - 1 and got[0].blob == b"\x00",
          "max int64 / blob fidelity")
    check(got[1].integer == -(2**63) and got[1].blob == bytes(range(256)),
          "min int64 / full blob fidelity")
    store.release(tok)

    g2 = store.subscribe("typed", 0)["generation"]
    page = store.pull("typed", g2)
    check_delivery(page, [(s, r, o) for s, r, o in model.stream(0)],
                   "typed delivery")
    # the delivered delete of the min-int64 key is a real op even though the
    # row did not exist at commit time
    check(("del", "", -(2**63)) in [tuple(o) for o in page.changes[0].ops],
          "delete of a missing signed-boundary key is delivered")
    store.ack("typed", g2, model.seq)

    # pause survives a service restart
    check(store.pause("typed", g2)["paused"], "pause")
    srv.stop()
    srv.start()
    store = Store(srv.sock)
    st = store.subscribe_status("typed")
    check(st["paused"] is True and st["generation"] == g2,
          f"pause and generation survive restart: {st}")
    check(st["acked_seq"] == model.seq, "cursor survives restart")
    check(store.pull("typed", g2).changes == [], "still paused after restart")
    check(store.resume("typed", g2)["paused"] is False, "resume after restart")
    print("    empty stream, signed keys, NULL/BLOB, restart-persisted pause")


def phase_restart_around_state(srv):
    """Restart across a generation switch and around a blocked cleanup
    boundary: generations, unconfirmed prefixes and receipts must persist."""
    store = Store(srv.sock)
    model = CommitModel()
    for i in range(1, 7):
        ops = [put(T, i, text=f"s{i}")]
        store.write(f"r{i}", ops)
        model.commit(f"r{i}", ops)

    sub = store.subscribe("epoch", 0)
    old_gen = sub["generation"]
    store.pull("epoch", old_gen, max_commits=3)
    store.ack("epoch", old_gen, 2)

    # switch generations, then restart before the new generation consumes
    new_gen = store.claim("epoch")["generation"]
    check(new_gen == old_gen + 1, "claim bumps the generation")
    srv.stop()
    srv.start()
    store = Store(srv.sock)

    st = store.subscribe_status("epoch")
    check(st["generation"] == new_gen, "generation survives restart")
    check(st["acked_seq"] == 2 and st["delivered_seq"] == 3,
          f"cursors survive restart: {st}")
    expect_error("STALE_GENERATION", lambda: store.ack("epoch", old_gen, 5),
                 "old generation ack after restart")
    expect_error("STALE_GENERATION", lambda: store.pull("epoch", old_gen),
                 "old generation pull after restart")
    check(store.subscribe_status("epoch")["acked_seq"] == 2,
          "old generation cannot advance the cursor after restart")
    p = store.pull("epoch", new_gen)
    check([c.commit_seq for c in p.changes] == [3, 4, 5, 6],
          "new generation resumes from the confirmed prefix")

    # a blocked cleanup stays blocked across a restart, and the blocker is
    # still named; the safe bound is recomputed identically
    err = expect_error("CLEANUP_BLOCKED", lambda: store.cleanup(5),
                       "blocked cleanup before restart")
    safe_before = err.detail["safe_up_to"]
    srv.stop()
    srv.start()
    store = Store(srv.sock)
    err = expect_error("CLEANUP_BLOCKED", lambda: store.cleanup(5),
                       "blocked cleanup after restart")
    check(err.detail["safe_up_to"] == safe_before,
          "blocked cleanup bound is stable across restart")
    check("epoch" in err.detail["blocked_by"], "blocker named after restart")

    # confirm, restart, then the same cleanup succeeds exactly once
    store.ack("epoch", new_gen, 6)
    srv.stop()
    srv.start()
    store = Store(srv.sock)
    check(store.subscribe_status("epoch")["acked_seq"] == 6,
          "confirmation survives restart")
    rep = store.cleanup(6)
    check(rep["log_low"] == 7 and rep["released_commits"] == 6, "cleanup")
    check(store.write("r1", model.commits[0][2])["commit_seq"] == 1,
          "receipts survive every restart")
    print("    restart: generation switch, blocked boundary and receipts "
          "persist")


def phase_cleanup_contention(srv):
    """Several processes push cleanup while another consumes: no cleanup can
    cross the consumer's confirmed prefix, and the survivor continues."""
    store = Store(srv.sock)
    model = CommitModel()
    for i in range(1, 41):
        ops = [put(T, i, text=f"q{i}")]
        store.write(f"q{i}", ops)
        model.commit(f"q{i}", ops)
    sub = store.subscribe("contend", 0)
    gen = sub["generation"]
    store.pull("contend", gen, max_commits=40)
    store.ack("contend", gen, 10)

    # many processes try to clean far ahead while the consumer is mid-way
    q = mp.Queue()
    procs = [mp.Process(target=_child_cleanup, args=(srv.sock, 40, q))
             for _ in range(4)]
    for pr in procs:
        pr.start()
    results = [q.get(timeout=60) for _ in procs]
    for pr in procs:
        pr.join(60)
    check(all(r[0] == "err" and r[1] == "CLEANUP_BLOCKED" for r in results),
          f"cleanup must be blocked by the unconfirmed prefix: {results}")
    st = store.subscribe_status("contend")
    check(st["acked_seq"] == 10 and st["delivered_seq"] == 40,
          f"contention must not move any cursor: {st}")

    # a cleanup exactly at the confirmed prefix is fine and does not disturb
    # delivery of the rest
    check(store.cleanup(10)["log_low"] == 11, "cleanup at the confirmed prefix")
    p = store.pull("contend", gen)
    check([c.commit_seq for c in p.changes] == list(range(11, 41)),
          "consumer continues after a concurrent cleanup")
    store.ack("contend", gen, 40)
    check(store.cleanup(40)["log_low"] == 41, "cleanup after confirmation")

    # a fresh subscription at the new end works with old requests still deduped
    fresh = store.subscribe("contend-new", 40)
    check(fresh["origin_seq"] == 40 and fresh["generation"] == 1,
          "subscription at the moved boundary")
    check(store.write("q1", model.commits[0][2])["commit_seq"] == 1,
          "old request receipt after contention")
    expect_error("CONFLICT", lambda: store.write("q1", [put(T, 1, text="x")]),
                 "conflict after contention")
    print("    cleanup contention: blocked across processes, consumer survives")


def _child_cleanup(sock, up_to, q):
    try:
        Store(sock).cleanup(up_to)
        q.put(("ok",))
    except StoreError as e:
        q.put(("err", e.code))
    except Exception as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def phase_crash_during_pull(srv):
    """Kill the server right after a delivery cursor advance."""
    store = Store(srv.sock)
    model = CommitModel()
    for i in range(1, 6):
        ops = [put(T, i, text=f"p{i}")]
        store.write(f"p{i}", ops)
        model.commit(f"p{i}", ops)
    sub = store.subscribe("crash", 0)
    gen = sub["generation"]

    store.arm_crash("during_pull")
    try:
        store.pull("crash", gen)
        raise Failure("pull should have lost its reply")
    except StoreError as e:
        check(e.code == "CONN", f"expected CONN, got {e.code}")
    srv.wait_dead()
    srv.start()
    store = Store(srv.sock)

    # the delivery cursor either advanced or did not; a retry is still the
    # same, complete, ordered prefix
    st = store.subscribe_status("crash")
    check(st["generation"] == gen, "generation survives a restart")
    p = store.pull("crash", gen)
    check_delivery(p, [(s, r, o) for s, r, o in model.stream(0)], "after crash")
    store.ack("crash", gen, model.seq)
    check(store.subscribe_status("crash")["acked_seq"] == model.seq, "ack")
    print("    crash during pull: cursor and generation consistent")


def phase_crash_during_ack(srv):
    """Kill the server right after the confirmed prefix is written."""
    store = Store(srv.sock)
    model = CommitModel()
    for i in range(1, 4):
        ops = [put(T, i)]
        store.write(f"a{i}", ops)
        model.commit(f"a{i}", ops)
    sub = store.subscribe("ackcrash", 0)
    gen = sub["generation"]
    store.pull("ackcrash", gen)

    store.arm_crash("during_ack")
    try:
        store.ack("ackcrash", gen, 2)
        raise Failure("ack should have lost its reply")
    except StoreError as e:
        check(e.code == "CONN", f"expected CONN, got {e.code}")
    srv.wait_dead()
    srv.start()
    store = Store(srv.sock)
    st = store.subscribe_status("ackcrash")
    check(st["generation"] == gen and st["delivered_seq"] == 3,
          f"state after ack crash: {st}")
    # the ack may or may not have committed; the retry is deterministic
    check(store.ack("ackcrash", gen, 2)["acked_seq"] in (0, 2),
          "ack retry after restart")
    check(store.ack("ackcrash", gen, 2)["acked_seq"] == 2,
          "ack retry converges")
    p = store.pull("ackcrash", gen)
    check([c.commit_seq for c in p.changes] == [3], "delivery resumes after ack")
    print("    crash during ack: confirmed prefix deterministic")


def phase_crash_during_cleanup(srv):
    """Kill the server while the retained range is being released."""
    store = Store(srv.sock)
    model = CommitModel()
    for i in range(1, 5):
        ops = [put(T, i, text=f"c{i}"), delete(T, 100 + i)]
        store.write(f"k{i}", ops)
        model.commit(f"k{i}", ops)
    sub = store.subscribe("clean", 0)
    gen = sub["generation"]
    p = drain(store, "clean", gen)
    check(len(p) == 4, "drained")

    store.arm_crash("during_cleanup")
    try:
        store.cleanup(4)
        raise Failure("cleanup should have lost its reply")
    except StoreError as e:
        check(e.code == "CONN", f"expected CONN, got {e.code}")
    srv.wait_dead()
    srv.start()
    store = Store(srv.sock)

    # either the old or the new retained range is visible in full, never
    # a partially cleaned one
    st = store.subscriptions()
    check(st["log_low"] in (1, 5), f"retained range atomic: {st['log_low']}")
    check(store.subscribe_status("clean")["generation"] == gen,
          "generation survives cleanup crash")
    rep = store.cleanup(4)
    check(rep["log_low"] == 5, "cleanup converges after restart")
    check(store.write("k1", model.commits[0][2])["commit_seq"] == 1,
          "receipt survives cleanup crash")
    expect_error("CONFLICT", lambda: store.write("k1", [put(T, 999)]),
                 "conflict after cleanup crash")
    print("    crash during cleanup: retained range and receipts intact")


def _child_consumer(sock, name, after, tag, commits_per_pull, q):
    """Child process: attach, consume, confirm, report what it saw."""
    try:
        store = Store(sock)
        try:
            gen = store.subscribe(name, after)["generation"]
        except StoreError as e:
            q.put(("attach-error", tag, e.code))
            return
        seen = []
        stale = None
        while True:
            try:
                p = store.pull(name, gen, max_commits=commits_per_pull)
            except StoreError as e:
                q.put(("stale", tag, e.code))
                return
            for c in p.changes:
                seen.append((c.commit_seq, c.request_id, c.ops))
            if len(p.changes):
                try:
                    store.ack(name, gen, p.next_after)
                except StoreError as e:
                    stale = e.code
                    break
            if not p.has_more:
                break
            time.sleep(0.002)
        q.put(("ok", tag, len(seen), stale))
    except Exception as e:  # noqa: BLE001
        q.put(("err", tag, repr(e)))


def phase_multiprocess(srv):
    """Several processes attach to one subscription; only the newest
    generation may consume, and a stale process cannot advance the cursor."""
    store = Store(srv.sock)
    model = CommitModel()
    for i in range(1, 31):
        ops = [put(T, i, text=f"m{i}")]
        store.write(f"m{i}", ops)
        model.commit(f"m{i}", ops)

    # seed the subscription so the children attach rather than create
    base = store.subscribe("shared", 0)
    check(base["generation"] == 1, "seed generation")

    # attach two generations; the second one is the valid consumer
    g1 = store.subscribe("shared", 0)["generation"]
    g2 = store.subscribe("shared", 0)["generation"]
    check(g1 == 2 and g2 == 3, f"generations {g1},{g2}")

    # a delayed ack from the old generation must not touch the new cursor
    p = store.pull("shared", g2, max_commits=5)
    check([c.commit_seq for c in p.changes] == [1, 2, 3, 4, 5], "new gen pull")
    expect_error("STALE_GENERATION", lambda: store.ack("shared", g1, 30),
                 "old generation ack")
    expect_error("STALE_GENERATION", lambda: store.pull("shared", g1),
                 "old generation pull")
    check(store.subscribe_status("shared")["acked_seq"] == 0,
          "stale generation must not advance the cursor")

    # the current generation consumes the lot; ack retries from the child
    store.ack("shared", g2, 5)
    p = store.pull("shared", g2)
    check(p.next_after == 30, "current generation sees the rest")
    store.ack("shared", g2, 30)

    # many processes attach in parallel; exactly one generation is current,
    # and all older ones fail cleanly without moving the cursor
    q = mp.Queue()
    procs = []
    for i in range(4):
        pr = mp.Process(target=_child_attach_only,
                        args=(srv.sock, "shared", 0, f"p{i}", q))
        procs.append(pr)
        pr.start()
    results = [q.get(timeout=60) for _ in procs]
    for pr in procs:
        pr.join(60)
    gens = [r[1] for r in results if r[0] == "ok"]
    check(len(gens) == 4 and len(set(gens)) == 4,
          f"each attach starts a distinct generation: {results}")
    current = max(gens)
    for g in gens:
        if g == current:
            continue
        expect_error("STALE_GENERATION", lambda g=g: store.pull("shared", g),
                     f"non-current generation {g}")
    check(store.subscribe_status("shared")["generation"] == current,
          "newest generation is current")
    check(store.subscribe_status("shared")["acked_seq"] == 30,
          "cursor still at the confirmed prefix")

    # several processes consume cooperatively under the current generation
    for i in range(31, 61):
        ops = [put(T, i, text=f"n{i}")]
        store.write(f"n{i}", ops)
        model.commit(f"n{i}", ops)
    q = mp.Queue()
    procs = []
    for i in range(3):
        pr = mp.Process(target=_child_consume_gen,
                        args=(srv.sock, "shared", current, f"w{i}", q))
        procs.append(pr)
        pr.start()
    results = [q.get(timeout=120) for _ in procs]
    for pr in procs:
        pr.join(120)
    for r in results:
        check(r[0] == "ok", f"child consumer failed: {r}")
    check(store.subscribe_status("shared")["acked_seq"] == 60,
          f"cooperative consumers confirmed the lot: "
          f"{store.subscribe_status('shared')}")
    print("    multiprocess: one current generation, stale acks rejected, "
          "cooperative consumption")


def _child_attach_only(sock, name, after, tag, q):
    try:
        store = Store(sock)
        gen = store.subscribe(name, after)["generation"]
        q.put(("ok", gen, tag))
    except StoreError as e:
        q.put(("err", tag, e.code))
    except Exception as e:  # noqa: BLE001
        q.put(("err", tag, repr(e)))


def _child_consume_gen(sock, name, gen, tag, q):
    try:
        store = Store(sock)
        total = 0
        while True:
            p = store.pull(name, gen, max_commits=7)
            if p.changes:
                total += len(p.changes)
                store.ack(name, gen, p.next_after)
            if not p.has_more:
                q.put(("ok", tag, total))
                return
    except StoreError as e:
        q.put(("err", tag, e.code))
    except Exception as e:  # noqa: BLE001
        q.put(("err", tag, repr(e)))


def phase_split(srv):
    """Subscriptions stay exact across an online split, including a commit
    that lands during the migration window and a cross-shard commit."""
    store = Store(srv.sock)
    model = CommitModel()
    sub = store.subscribe("split", 0)
    gen = sub["generation"]

    ops = [put(T, k, text=f"pre{k}") for k in range(1, 9)]
    store.write("pre", ops)
    model.commit("pre", ops)

    store.pause_copy(True)
    src = store.routing()["routes"][0]["shard"]
    store.split(src, (T, 4))

    # a commit while the split is copying: keys on both sides of the boundary
    ops = [put(T, 2, text="mid-left"), delete(T, 5), put(T, 7, text="mid-right"),
           delete(T, 999)]
    store.write("mid", ops)
    model.commit("mid", ops)
    store.pause_copy(False)
    wait_for(lambda: store.migration_status()["split"] is not None
             and store.migration_status()["split"]["phase"] == "PUBLISHED",
             timeout=120, what="split publish")

    # another commit after the cutover, touching both new shards
    ops = [put(T, 3, text="post-left"), put(T, 8, text="post-right"),
           delete(T, 6), put(T, 6, text="back"), delete(T, 6)]
    store.write("post", ops)
    model.commit("post", ops)

    p = store.pull("split", gen, max_commits=100)
    check_delivery(p, [(s, r, o) for s, r, o in model.stream(0)],
                   "delivery across the split")
    store.ack("split", gen, model.seq)

    # new writes get fresh seqs and remain in order
    ops = [put(T, 1, text="newest")]
    store.write("newest", ops)
    model.commit("newest", ops)
    p = store.pull("split", gen)
    check([c.commit_seq for c in p.changes] == [model.seq],
          "post-split delivery order")
    store.ack("split", gen, model.seq)

    # the store's own state agrees with the model
    tok = store.open_read()
    for (tenant, key), value in model.state.items():
        row = store.get(tok, tenant, key)
        check(row is not None and row.value() == value,
              f"row {tenant}/{key} after split")
    check(store.get(tok, T, 6) is None, "key 6 deleted in the post commit")
    store.release(tok)
    check(store.migration_status()["epoch"] == 2, "one epoch switch")
    print("    split: cross-shard commits delivered in caller op order")


def phase_backlog(srv):
    """Runtime-generated backlog paged with bounded pull sizes and memory."""
    store = Store(srv.sock)
    model = CommitModel()
    sub = store.subscribe("backlog", 0)
    gen = sub["generation"]

    n = BACKLOG
    for i in range(1, n + 1):
        ops = [put(T, i, text=f"b{i}"), delete(T, 10**6 + i)]
        store.write(f"b{i}", ops)
        model.commit(f"b{i}", ops)
    expected = [(s, r, o) for s, r, o in model.stream(0)]

    baseline = srv.rss_kb() or 0
    max_rss = baseline
    seen = []
    pages = 0
    while True:
        p = store.pull("backlog", gen, max_commits=13, max_bytes=2048)
        pages += 1
        seen.extend((c.commit_seq, c.request_id, [tuple(o) for o in c.ops])
                    for c in p.changes)
        check(len(p.changes) <= 13, "count budget respected")
        if len(p.changes) > 1:
            check(p.bytes <= 2048, f"byte budget respected: {p.bytes}")
        if p.changes:
            store.ack("backlog", gen, p.next_after)
        rss = srv.rss_kb()
        if rss:
            max_rss = max(max_rss, rss)
        if not p.has_more:
            break
    check(seen == expected, f"backlog delivery exact over {pages} pages")
    check(pages > 1, "backlog must be paged")
    delta = max_rss - baseline
    check(delta < 200 * 1024, f"pulling {n} commits grew RSS by {delta}kB")

    # every commit seq was delivered exactly once, in order
    seqs = [s for s, _, _ in seen]
    check(seqs == list(range(1, n + 1)), "strictly increasing, no gaps")

    # replay of the whole retained backlog is also exact
    rp = []
    after = 0
    while True:
        p = store.replay("backlog", gen, after, max_commits=50)
        rp.extend((c.commit_seq, c.request_id, [tuple(o) for o in c.ops])
                  for c in p.changes)
        after = p.next_after
        if not p.has_more:
            break
    check(rp == expected, "replay of the backlog is exact")

    rep = store.cleanup(n)
    check(rep["released_commits"] == n, "backlog released")
    check(store.cleanup(n)["released_commits"] == 0, "second cleanup is a no-op")
    print(f"    backlog: {n} commits over {pages} pages, RSS growth {delta}kB")


# ================================================================== main
PHASES = [
    ("delivery units", phase_delivery),
    ("generations/pause/replay", phase_generations),
    ("slow vs fast cleanup", phase_slow_fast),
    ("cleanup and receipts", phase_cleanup_receipts),
    ("empty stream and types", phase_empty_and_types),
    ("restart around state", phase_restart_around_state),
    ("cleanup contention", phase_cleanup_contention),
    ("crash during pull", phase_crash_during_pull),
    ("crash during ack", phase_crash_during_ack),
    ("crash during cleanup", phase_crash_during_cleanup),
    ("multiprocess generations", phase_multiprocess),
    ("delivery across split", phase_split),
    ("bounded backlog", phase_backlog),
]


def main():
    keep = os.environ.get("KEEP") == "1"
    only = os.environ.get("ONLY")
    base = tempfile.mkdtemp(prefix="shardstore-sub-acc-")
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
    print(f"\nALL SUBSCRIPTION CHECKS PASSED ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
