#!/usr/bin/env python3
"""Independent acceptance drill for shardstore grouped aggregate views.

Expected results are computed by a hand-written serial model that keeps the
full logical row state per commit seq and derives the grouped values itself
(row count, non-NULL integer count, exact integer sum).  It never calls the
server's aggregate code, SQL, or any helper from shardstore except the public
client.

The real server runs as a subprocess; crashes are real os._exit hooks and
SIGKILLs, and the service is restarted to prove that view definitions,
aggregates, build progress and the queryable watermark range survive intact.

Phases:
  1. empty / NULL group / integer NULLs / negatives / >64-bit sums / stable
     order / key-range filtering
  2. cross-group updates, deletes, delete-of-missing, repeated ops on one key
     inside a single transaction, an emptied group disappearing
  3. initial build interleaved with concurrent commits and an online split;
     every available watermark matches the model; the same watermark returns
     identical groups before and after the split
  4. forced exit during the build scan, during catch-up and around the atomic
     publish; after restart there is no missed, duplicated or partially
     exposed commit
  5. an injected rebuild failure leaves the previous generation fully usable
  6. restart preserves definitions, aggregates, progress and range
  7. runtime-generated large row set: the build is batched with bounded
     server memory (RSS sampled)
"""
from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shardstore.client import Store, StoreError, put, delete  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
T = "t"
SCALE_ROWS = int(os.environ.get("VIEW_SCALE_ROWS", "30000"))
RSS_BUDGET_KB = int(os.environ.get("RSS_BUDGET_KB", str(300 * 1024)))


class Failure(Exception):
    pass


def check(cond, msg):
    if not cond:
        raise Failure(msg)


def expect_error(code, fn, what):
    try:
        fn()
    except StoreError as e:
        check(e.code == code, f"{what}: expected {code}, got {e.code}: {e}")
        return e
    raise Failure(f"{what}: should have been rejected with {code}")


# ================================================= independent model
def gorder(tenant, text):
    """The total order the contract promises: (tenant UTF-8 bytes, NULL
    before text, text bytes)."""
    return (tenant.encode("utf-8"),
            0 if text is None else 1,
            b"" if text is None else text.encode("utf-8"))


class ViewModel:
    """Serial logical-row model with a full snapshot per commit seq.

    `groups_at(w)` derives the expected grouped result from the plain dict,
    using Python's arbitrary-precision integers for the sum.
    """

    def __init__(self):
        self.cur = {}
        self.seq = 0
        self.snaps = {0: {}}

    def commit(self, ops):
        self.seq += 1
        for op in ops:
            if op[0] == "put":
                _, tenant, key, value = op
                self.cur[(tenant, key)] = tuple(value)
            else:
                _, tenant, key = op
                self.cur.pop((tenant, key), None)
        self.snaps[self.seq] = dict(self.cur)
        return self.seq

    def groups_at(self, watermark, lo=None, hi=None):
        acc = {}
        for (tenant, _key), (_text, integer, _blob) in self.snaps[watermark].items():
            key = (tenant, _text)
            ent = acc.setdefault(key, [0, 0, 0])
            ent[0] += 1
            if integer is not None:
                ent[1] += 1
                ent[2] += int(integer)
        out = []
        for (tenant, text), (rows, ncnt, total) in acc.items():
            if lo is not None and gorder(tenant, text) < gorder(*lo):
                continue
            if hi is not None and gorder(tenant, text) >= gorder(*hi):
                continue
            out.append({"tenant": tenant, "text": text, "rows": rows,
                        "int_nonnull": ncnt, "int_sum": str(total)})
        out.sort(key=lambda g: gorder(g["tenant"], g["text"]))
        return out


def check_groups(got, expected, what):
    """Compare server groups with the model, including exact big sums."""
    norm = [{"tenant": g["tenant"], "text": g["text"], "rows": int(g["rows"]),
             "int_nonnull": int(g["int_nonnull"]),
             "int_sum": str(int(g["int_sum"]))} for g in got]
    if norm != expected:
        n = min(len(norm), len(expected))
        for i in range(n):
            if norm[i] != expected[i]:
                raise Failure(f"{what}: first diff at #{i}: got {norm[i]!r}"
                              f" expected {expected[i]!r}")
        raise Failure(f"{what}: length {len(norm)} != expected {len(expected)}:"
                      f" got {norm!r} expected {expected!r}")


def check_view(store, name, model, watermark, what, lo=None, hi=None):
    q = store.view_query(name, watermark=watermark, lo=lo, hi=hi,
                         wait=True, timeout_ms=30000)
    check(q["watermark"] == watermark,
          f"{what}: answered at {q['watermark']}, asked for {watermark}")
    check_groups(q["groups"], model.groups_at(watermark, lo, hi), what)


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
                raise Failure(f"server exited early rc={self.proc.returncode};"
                              f" log: {self.log}")
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


def wait_for(fn, timeout=120, interval=0.03, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(interval)
    raise Failure(f"timeout waiting for {what}")


def wait_watermark(store, name, watermark, timeout=120):
    """Wait until the view can answer exactly at `watermark`."""
    def ready():
        try:
            q = store.view_query(name, watermark=watermark, wait=False)
            return q["watermark"] == watermark
        except StoreError as e:
            if e.code == "VIEW_NOT_READY":
                return False
            raise
    wait_for(ready, timeout=timeout, what=f"view {name} at {watermark}")


def view_hi(store, name):
    return store.view_status(name)["queryable_hi"]


def wait_published(store, timeout=180):
    wait_for(lambda: (store.migration_status()["split"] or {}).get("phase")
             == "PUBLISHED", timeout=timeout, what="split publish")


def _shard_for(store, tenant, key):
    """The routed shard whose half-open range contains (tenant, key)."""
    order = (tenant.encode("utf-8"), key)

    def bnd(b):
        return None if b is None else (b["tenant"].encode("utf-8"), int(b["key"]))
    for r in store.routing()["routes"]:
        lo, hi = bnd(r["lo"]), bnd(r["hi"])
        if (lo is None or order >= lo) and (hi is None or order < hi):
            return r["shard"]
    raise Failure(f"no shard covers {tenant!r}/{key}")


def _seed(store, model, rows, tag, integer=lambda k: k, text=lambda k: "g",
          tenants=(T,)):
    """Seed `rows` keys deterministically in small chunks (small fixtures)."""
    k = 0
    wid = 0
    while k < rows:
        ops = []
        for _ in range(min(250, rows - k)):
            k += 1
            t = tenants[k % len(tenants)]
            ops.append(put(t, k, text=text(k), integer=integer(k)))
        wid += 1
        store.write(f"{tag}-{wid}", ops)
        model.commit(ops)


# ================================================================= phases
def phase_types_and_nulls(srv):
    """Empty view, NULL text group, integer NULLs, negatives, >64-bit sums,
    stable order and filtering."""
    store = Store(srv.sock)
    model = ViewModel()

    # an empty view over an empty store
    store.view_create("empty", 0)
    wait_watermark(store, "empty", 0)
    check_groups(store.view_query("empty", watermark=0)["groups"], [],
                 "empty view must have no groups")

    # NULL text is its own group; NULL integers count as rows but not integers
    ops = [
        put(T, 1, text=None, integer=5),
        put(T, 2, text=None, integer=None),
        put(T, 3, text="a", integer=-(2 ** 63)),
        put(T, 4, text="a", integer=None),
        put(T, 5, text="a", integer=7),
        put("u", 1, text="", integer=0),
        put("u", 2, text=None, integer=-(2 ** 63)),
    ]
    store.write("types-1", ops)
    model.commit(ops)
    store.view_create("v", 0)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "types/nulls at head")

    # per watermark history: every committed seq is queryable and exact
    for w in range(0, model.seq + 1):
        check_view(store, "v", model, w, f"types at watermark {w}")

    # values beyond signed 64 bits stay exact (two INT64_MAX rows in a group)
    big = [
        put("big", 1, text="sum", integer=(2 ** 63) - 1),
        put("big", 2, text="sum", integer=(2 ** 63) - 1),
        put("big", 3, text="sum", integer=(2 ** 63) - 1),
        put("big", 4, text="neg", integer=-(2 ** 63)),
        put("big", 5, text="neg", integer=-(2 ** 63)),
        put("big", 6, text="neg", integer=1),
    ]
    store.write("types-big", big)
    model.commit(big)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "beyond-64-bit sums")
    big_groups = [g for g in store.view_query("v", watermark=model.seq)["groups"]
                  if g["tenant"] == "big"]
    sums = {g["text"]: int(g["int_sum"]) for g in big_groups}
    check(sums["sum"] == 3 * ((2 ** 63) - 1), "positive big sum must be exact")
    check(sums["neg"] == 2 * (-(2 ** 63)) + 1, "negative big sum must be exact")
    lists = store.view_query("v", watermark=model.seq)["groups"]
    keys = [gorder(g["tenant"], g["text"]) for g in lists]
    check(keys == sorted(keys), "groups must follow the stable order")
    check(all(g["tenant"] != "u" or g["text"] in (None, "")
              for g in lists), "NULL and empty text are distinct groups")

    # filtering by (tenant, text) with NULL as its own boundary
    check_view(store, "v", model, model.seq, "filter tenant a",
               lo=("a", None), hi=("b", None))
    # fake a tenant named between; bounds are pure ordering
    check_view(store, "v", model, model.seq, "filter lowercase t, NULL..b",
               lo=(T, None), hi=(T, "b"))
    check_view(store, "v", model, model.seq, "filter text range",
               lo=("a", "a"), hi=("a", None))
    print("    types: empty view, NULL group, integer NULLs, negatives,"
          " >64-bit sums, order and filters ok")


def phase_updates_and_deletes(srv):
    """Cross-group updates, deletes, delete-of-missing, repeated ops on one
    key in one transaction, and an emptied group disappearing."""
    store = Store(srv.sock)
    model = ViewModel()
    ops = [put(T, 1, text="A", integer=10), put(T, 2, text="A", integer=20),
           put(T, 3, text="B", integer=5), put(T, 4, text="B", integer=None)]
    store.write("u-1", ops)
    model.commit(ops)
    store.view_create("v", 0)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "initial groups")

    # move key 1 from group A to group B and change its integer
    ops = [put(T, 1, text="B", integer=100)]
    store.write("u-2", ops)
    model.commit(ops)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "cross-group update")

    # delete a row; deleting a missing row must not change anything
    ops = [delete(T, 2), delete(T, 99)]
    store.write("u-3", ops)
    model.commit(ops)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "delete plus delete-of-missing")
    # a transaction that only deletes missing rows leaves the aggregate equal
    before = store.view_query("v", watermark=model.seq)["groups"]
    ops = [delete(T, 500), delete(T, 501)]
    store.write("u-4", ops)
    model.commit(ops)
    check(model.snaps[model.seq] == model.snaps[model.seq - 1],
          "model: missing deletes change nothing")
    wait_watermark(store, "v", model.seq)
    after = store.view_query("v", watermark=model.seq)["groups"]
    check(before == after, "delete-of-missing changed the aggregate")
    check_view(store, "v", model, model.seq, "no-op deletes")

    # several ops on the same key inside one transaction compose to the net
    ops = [put(T, 7, text="A", integer=1), put(T, 7, text="C", integer=2),
           put(T, 7, text="C", integer=3), delete(T, 7),
           put(T, 7, text="D", integer=40)]
    store.write("u-5", ops)
    model.commit(ops)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "repeated ops on one key")
    # move then delete the same key in one transaction: net no change
    ops = [put(T, 8, text="A", integer=9)]
    store.write("u-6", ops)
    model.commit(ops)
    ops = [put(T, 8, text="B", integer=99), delete(T, 8)]
    store.write("u-7", ops)
    model.commit(ops)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "put+delete in one transaction")
    check(not any(g["tenant"] == T and g["text"] == "B" and
                  int(g["int_sum"]) == 99
                  for g in store.view_query("v", watermark=model.seq)["groups"]),
          "transient value of a key must not leak into the aggregate")

    # empty group disappears: delete both rows of group A
    ops = [delete(T, 3), delete(T, 4)]
    store.write("u-8", ops)
    model.commit(ops)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "emptied group disappears")
    check(not any(g["text"] == "A" for g in
                  store.view_query("v", watermark=model.seq)["groups"]),
          "group A must be gone")
    print("    updates: cross-group moves, deletes, no-op deletes, repeated"
          " same-key ops, emptied group gone ok")


def phase_build_interleaved_with_split(srv):
    """Build while commits keep landing and an online split runs; every
    available watermark is exact, and a watermark is identical before and
    after the split."""
    store = Store(srv.sock)
    model = ViewModel()
    _seed(store, model, 2000, "seed",
          text=lambda k: f"g{k % 4}", integer=lambda k: k - 1000)
    start = model.seq

    # begin a view pinned to an already-committed seq, then keep writing
    store.view_create("live", start)
    store.pause_copy(True)
    src = store.routing()["routes"][0]["shard"]
    store.split(src, (T, 1000))

    extra = []
    for i in range(1, 61):
        k = 2000 + i
        ops = [put(T, k, text=f"g{i % 4}", integer=i),
               delete(T, i),  # deletes a seed key
               put(T, 1000 + i, text=f"g{(i + 1) % 4}", integer=-i)]
        store.write(f"live-{i}", ops)
        model.commit(ops)
        extra.append(model.seq)
        if i == 20:
            store.pause_copy(False)
    wait_published(store)

    # every sampled watermark, including ones committed during the build and
    # split, must match the model exactly once the view has materialized it
    wait_watermark(store, "live", model.seq)
    for w in [start, start + 1] + extra[::7] + [model.seq]:
        check_view(store, "live", model, w, f"interleaved watermark {w}")

    # the same watermark is identical before and after a further split
    w = extra[len(extra) // 2]
    before = store.view_query("live", watermark=w)["groups"]
    src2 = _shard_for(store, "t", 500)
    store.split(src2, ("t", 500))
    wait_published(store)
    after = store.view_query("live", watermark=w)["groups"]
    check(before == after, "same watermark must be identical across a split")

    # a fresh view built entirely after both splits still matches the model
    store.view_create("post", 0)
    wait_watermark(store, "post", model.seq)
    check_view(store, "post", model, model.seq, "view built after splits")
    for w in [0, start, model.seq]:
        check_view(store, "post", model, w, f"post-split watermark {w}")
    print("    interleave: build across commits and two splits, identical"
          " watermarks ok")


def _crash_phase(srv, point, rows=1200, victim_start=0):
    """Force an exit at `point` during a view build, restart and verify."""
    store = Store(srv.sock)
    model = ViewModel()
    _seed(store, model, rows, f"c-{point}", text=lambda k: f"g{k % 5}",
          integer=lambda k: (k - rows // 2))
    # an already-materialized generation that must survive the crash untouched
    store.view_create("base", 0)
    wait_watermark(store, "base", model.seq)
    base_groups = store.view_query("base", watermark=model.seq)["groups"]

    # a non-zero start forces the reverse-undo scan (and its crash points)
    store.arm_crash(point)
    try:
        store.view_create("victim", victim_start)
    except StoreError as e:
        check(e.code == "CONN", f"{point}: expected CONN, got {e.code}")
    srv.wait_dead()
    srv.start()
    store = Store(srv.sock)

    # the pre-crash generation is still complete and usable
    check(store.view_query("base", watermark=model.seq)["groups"] == base_groups,
          f"{point}: the intact generation must stay usable")

    # the crashed generation either completed or is still rebuilding; once
    # queryable, every watermark matches the model with no double counting
    wait_watermark(store, "victim", model.seq)
    for w in [victim_start, model.seq // 3, model.seq]:
        if w < victim_start:
            continue
        check_view(store, "victim", model, w, f"{point} after restart w={w}")
    st = store.view_status("victim")
    check(st["active_gen"] == 1, f"{point}: active generation stable")
    check(st["queryable_lo"] == victim_start and st["queryable_hi"] >= model.seq,
          f"{point}: queryable range after recovery")

    # a commit made after recovery is folded exactly once
    ops = [put(T, rows + 1, text="g0", integer=777),
           delete(T, 1), delete(T, 2)]
    store.write(f"post-{point}", ops)
    model.commit(ops)
    wait_watermark(store, "victim", model.seq)
    check_view(store, "victim", model, model.seq, f"{point}: post-recovery write")
    check_view(store, "base", model, model.seq, f"{point}: base tails writes")
    print(f"    crash at {point}: recovered, no miss/dup, prior generation ok")


def phase_crash_during_build(srv):
    _crash_phase(srv, "during_view_build", victim_start=3)


def phase_crash_during_catchup(srv):
    _crash_phase(srv, "during_view_catchup", victim_start=0)


def phase_crash_during_fold(srv):
    _crash_phase(srv, "during_view_fold", victim_start=3)


def phase_crash_around_publish(srv):
    _crash_phase(srv, "before_view_publish", victim_start=3)


def phase_crash_after_publish(srv):
    _crash_phase(srv, "after_view_publish", victim_start=0)


def phase_crash_interleaved_split(srv):
    """Interleave a view build with concurrent commits, an online split and a
    forced exit; after restart no commit is missed, duplicated or partially
    exposed, including a cross-shard transaction."""
    store = Store(srv.sock)
    model = ViewModel()
    _seed(store, model, 5000, "icsi", text=lambda k: f"g{k % 6}",
          integer=lambda k: k - 2500)
    start = model.seq

    store.pause_copy(True)
    src = store.routing()["routes"][0]["shard"]
    store.split(src, (T, 2500))

    # a single cross-shard transaction (keys on both sides of the boundary)
    ops = [put(T, 100, text="g0", integer=11),
           put(T, 4900, text="g0", integer=22),
           delete(T, 200)]
    store.write("icsi-xshard", ops)
    xshard_seq = model.commit(ops)

    # more writes land while the split copies and the view has yet to build
    for i in range(1, 31):
        ops = [put(T, 5000 + i, text=f"g{i % 6}", integer=i), delete(T, i)]
        store.write(f"icsi-w{i}", ops)
        model.commit(ops)

    # force an exit during the view's reverse-undo scan (non-zero start)
    store.arm_crash("during_view_build")
    victim_start = max(0, start - 3)
    try:
        store.view_create("live", victim_start)
    except StoreError as e:
        check(e.code == "CONN", f"expected CONN, got {e.code}")
    srv.wait_dead()
    srv.start()
    store = Store(srv.sock)
    store.pause_copy(False)
    wait_published(store)

    wait_watermark(store, "live", model.seq)
    # the cross-shard transaction is visible whole at its own watermark
    check_view(store, "live", model, xshard_seq,
               "cross-shard transaction at its watermark")
    for w in [victim_start, start, model.seq]:
        check_view(store, "live", model, w, f"interleaved crash watermark {w}")

    # writes after recovery are folded exactly once, and the same watermark
    # is identical before and after a further split
    ops = [put(T, 6000, text="g0", integer=7), delete(T, 5001)]
    store.write("icsi-post", ops)
    model.commit(ops)
    wait_watermark(store, "live", model.seq)
    w = model.seq
    before = store.view_query("live", watermark=w)["groups"]
    src2 = _shard_for(store, T, 3000)
    store.split(src2, (T, 3000))
    wait_published(store)
    check(store.view_query("live", watermark=w)["groups"] == before,
          "same watermark identical across a split after recovery")
    check_view(store, "live", model, model.seq, "final after interleaved crash")
    print("    interleaved split + crash: cross-shard atomic, no miss/dup,"
          " stable watermarks ok")


def phase_rebuild_failure_and_restart(srv):
    """An injected rebuild failure must leave the old generation fully usable;
    a restart preserves definitions, aggregates, progress and range."""
    store = Store(srv.sock)
    model = ViewModel()
    _seed(store, model, 600, "rb", text=lambda k: f"g{k % 3}",
          integer=lambda k: k)
    store.view_create("v", 0)
    wait_watermark(store, "v", model.seq)
    good = store.view_query("v", watermark=model.seq)["groups"]
    gen_before = store.view_status("v")["active_gen"]

    # inject a build failure on the rebuild's scan; the old generation stays
    store.arm_view_failure(True)
    store.view_rebuild("v", 0)
    wait_for(lambda: any(g["phase"] == "FAILED"
                         for g in store.view_status("v")["generations"]),
             timeout=60, what="injected rebuild failure")
    st = store.view_status("v")
    check(st["active_gen"] == gen_before,
          "a failed rebuild must not change the active generation")
    check(store.view_query("v", watermark=model.seq)["groups"] == good,
          "old generation must stay fully usable after a failed rebuild")

    # writes continue and the still-active generation keeps tailing them
    ops = [put(T, 700, text="g0", integer=5), delete(T, 3)]
    store.write("rb-after", ops)
    model.commit(ops)
    wait_watermark(store, "v", model.seq)
    check_view(store, "v", model, model.seq, "active generation after failure")

    # restart: definition, aggregate, progress and range are unchanged
    status_before = store.view_status("v")
    queries = {w: store.view_query("v", watermark=w)["groups"]
               for w in (0, model.seq // 2, model.seq)}
    srv.kill(signal.SIGKILL)
    srv.start()
    store = Store(srv.sock)
    status_after = store.view_status("v")
    check(status_after["active_gen"] == status_before["active_gen"],
          "active generation survives restart")
    check(status_after["queryable_lo"] == status_before["queryable_lo"]
          and status_after["queryable_hi"] == status_before["queryable_hi"],
          "queryable range survives restart")
    check([(g["gen"], g["start_seq"], g["phase"])
           for g in status_after["generations"]]
          == [(g["gen"], g["start_seq"], g["phase"])
              for g in status_before["generations"]],
          "generation table survives restart")
    for w, groups in queries.items():
        check(store.view_query("v", watermark=w)["groups"] == groups,
              f"aggregate at {w} survives restart")

    # a successful rebuild after the restart publishes a new generation and
    # keeps answering the same values (the aggregate is derived, not additive)
    store.view_rebuild("v", 0)
    wait_for(lambda: store.view_status("v")["active_gen"] > gen_before,
             timeout=60, what="rebuild publish")
    check_view(store, "v", model, model.seq, "rebuilt generation")
    for w in (0, model.seq // 2, model.seq):
        check_groups(store.view_query("v", watermark=w)["groups"],
                     model.groups_at(w), f"rebuilt watermark {w}")
    print("    rebuild failure + restart: old generation intact, state durable"
          " and a clean rebuild publishes ok")


def phase_ready_and_errors(srv):
    """A not-yet-materialized watermark is reported, never silently answered
    from an older watermark; inputs are validated."""
    store = Store(srv.sock)
    model = ViewModel()
    ops = [put(T, k, text="g", integer=k) for k in range(1, 51)]
    store.write("e-1", ops)
    model.commit(ops)
    store.view_create("v", 0)
    wait_watermark(store, "v", model.seq)

    # asking beyond the committed end is out of range, not an old answer
    expect_error("VIEW_NOT_READY",
                 lambda: store.view_query("v", watermark=model.seq + 100,
                                          wait=False),
                 "future watermark")
    # and waiting for it times out with the explicit code
    expect_error("VIEW_NOT_READY",
                 lambda: store.view_query("v", watermark=model.seq + 100,
                                          wait=True, timeout_ms=300),
                 "future watermark (wait)")
    # an unknown view and an out-of-range start seq
    expect_error("NO_VIEW", lambda: store.view_query("nope"), "unknown view")
    expect_error("OUT_OF_RANGE", lambda: store.view_create("x", model.seq + 5),
                 "start_seq beyond the committed end")
    expect_error("VIEW_EXISTS", lambda: store.view_create("v", 0),
                 "duplicate view create")

    # waiting for a just-committed watermark blocks briefly and then answers
    ops = [put(T, 51, text="g", integer=51)]
    store.write("e-2", ops)
    model.commit(ops)
    q = store.view_query("v", watermark=model.seq, wait=True, timeout_ms=30000)
    check(q["watermark"] == model.seq, "waiting query answered at the target")
    check_groups(q["groups"], model.groups_at(model.seq), "waiting query result")
    # a watermark below a generation's start is refused at once (waiting can
    # never materialize it), not answered from some other watermark
    store2 = Store(srv.sock)
    store2.write("e-start", [put(T, 100, text="g", integer=1)])
    model.commit([put(T, 100, text="g", integer=1)])
    store2.view_create("late", model.seq)
    wait_watermark(store2, "late", model.seq)
    t0 = time.time()
    e = expect_error("VIEW_NOT_READY",
                     lambda: store2.view_query("late", watermark=model.seq - 1,
                                               wait=True, timeout_ms=30000),
                     "watermark below the start")
    check(time.time() - t0 < 5,
          "a pre-start watermark must be refused immediately, not waited on")
    check(e.detail.get("permanent") is True, "pre-start refusal is permanent")

    # the previous generation stays fully queryable during a rebuild
    prev = store2.view_query("late", watermark=model.seq)["groups"]
    store2.view_rebuild("late", model.seq)
    # immediately after starting the rebuild the old generation still answers
    check(store2.view_query("late", watermark=model.seq)["groups"] == prev,
          "old generation must answer during a rebuild")
    wait_for(lambda: store2.view_status("late")["active_gen"] >= 2,
             timeout=60, what="rebuild completes")

    print("    readiness: pre-start/unknown/duplicate errors, waiting query,"
          " rebuild keeps old generation ok")


def phase_scale_bounded_memory(srv):
    """Runtime-generated large row set: the build stays batched and its server
    memory growth is bounded, and the aggregate matches a model recomputed
    from the generated rows."""
    store = Store(srv.sock)
    model = ViewModel()
    groups = 8
    tag = "scale"

    # bulk-load in bounded chunks; the model keeps the current rows only
    wid = 0
    k = 0
    while k < SCALE_ROWS:
        ops = []
        for _ in range(min(500, SCALE_ROWS - k)):
            k += 1
            ops.append(put(T, k, text=f"g{k % groups}", integer=(k * 37) % 100003 - 50000))
        wid += 1
        store.write(f"{tag}-{wid}", ops)
        model.commit(ops)

    base_rss = srv.rss_kb()
    peak = [base_rss or 0]
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            r = srv.rss_kb()
            if r:
                peak[0] = max(peak[0], r)
            time.sleep(0.02)

    t = threading.Thread(target=sample, daemon=True)
    t.start()
    try:
        store.view_create("scale", 0)
        wait_watermark(store, "scale", model.seq, timeout=300)
    finally:
        stop.set()
        t.join(timeout=2)
    growth = peak[0] - (base_rss or peak[0])
    check(growth <= RSS_BUDGET_KB,
          f"view build RSS growth {growth}kB exceeds budget {RSS_BUDGET_KB}kB")
    check_view(store, "scale", model, model.seq, "large view at head")
    check_view(store, "scale", model, 0, "large view at start")
    # exact sums over many rows
    total = sum(int(g["int_sum"]) for g in
                store.view_query("scale", watermark=model.seq)["groups"])
    check(total == sum(int(g["int_sum"]) for g in model.groups_at(model.seq)),
          "large view total sum is exact")
    print(f"    scale: {SCALE_ROWS} rows, {groups} groups, build RSS growth"
          f" {growth}kB (budget {RSS_BUDGET_KB}kB), exact sums ok")


# ================================================================== main
PHASES = [
    ("types and nulls", phase_types_and_nulls),
    ("updates and deletes", phase_updates_and_deletes),
    ("build interleaved with split", phase_build_interleaved_with_split),
    ("crash during build", phase_crash_during_build),
    ("crash during catchup", phase_crash_during_catchup),
    ("crash during fold", phase_crash_during_fold),
    ("crash before publish", phase_crash_around_publish),
    ("crash after publish", phase_crash_after_publish),
    ("interleaved split + crash", phase_crash_interleaved_split),
    ("rebuild failure + restart", phase_rebuild_failure_and_restart),
    ("readiness and errors", phase_ready_and_errors),
    ("scale bounded memory", phase_scale_bounded_memory),
]


def main():
    keep = os.environ.get("KEEP") == "1"
    only = os.environ.get("ONLY")
    base = tempfile.mkdtemp(prefix="shardstore-view-acc-")
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
    print(f"\nALL VIEW CHECKS PASSED ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
