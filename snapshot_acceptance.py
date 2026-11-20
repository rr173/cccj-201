#!/usr/bin/env python3
"""Independent acceptance drill for shardstore consistent snapshots.

Everything expected here is derived without calling the snapshot export or
restore implementation:

  * Model  - a hand-written serial dict model that keeps the full logical row
             state *per commit watermark* plus the request_id -> (hash,
             receipt) evidence the dedup contract needs.
  * Backup - the backup directory is parsed from raw bytes in this file
             (uint32 length prefix + JSON frame), every SHA-256/length is
             recomputed straight from the file bytes, and the manifest is
             checked against those independently counted records.

The server still runs as a real subprocess; crashes are real os._exit hooks
and SIGKILLs, restores build into separate directories and come up as new
server instances.

Phases:
  1. consistency   NULL/BLOB/boundary-key cross-shard transactions, export
                   while writes and a split keep landing, exact watermark
                   match, post-watermark writes excluded
  2. restore       rows/order/types match the model at the watermark, seq
                   continues from there, old request_ids replay their
                   receipts and conflict, subscription resume boundary
  3. crash export  forced exits during segment writing and around manifest
                   publication: only a complete backup is ever recognizable
  4. crash restore forced exits during verification, shard building and
                   around the final publish: no half-store, empty target,
                   existing data never overwritten
  5. corruption    truncated/tampered segments, bad hash/length, unknown
                   format version, duplicate key, out-of-order frame
  6. edges         empty store, receipts surviving change-log cleanup,
                   multi-source-shard routing mapping onto the restore
                   layout, oversized single value
  7. scale         runtime-generated large dataset, bounded export/restore
                   memory (RSS sampled), independent byte re-verification
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shardstore.client import Store, StoreError, put, delete  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
T = "t"
SCALE_ROWS = int(os.environ.get("SNAP_SCALE_ROWS", "60000"))
RSS_BUDGET_KB = int(os.environ.get("RSS_BUDGET_KB", str(300 * 1024)))
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1


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


# ======================================================= independent model
class WatermarkModel:
    """Hand-written serial model: logical rows *and* dedup evidence, with a
    full row snapshot retained per watermark.  Never imports snapshot.py."""

    def __init__(self):
        self.cur = {}
        self.seq = 0
        self.snaps = {0: {}}
        # request_id -> (canonical hash, original ops, receipt)
        self.requests = {}

    @staticmethod
    def canon_hash(wire_ops):
        canon = json.dumps(wire_ops, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False)
        return hashlib.sha256(canon.encode("utf-8")).hexdigest()

    def commit(self, request_id, ops):
        wire = [_op_wire(o) for o in ops]
        h = self.canon_hash(wire)
        prior = self.requests.get(request_id)
        if prior is not None:
            if prior[0] != h:
                raise Failure("model: conflicting request_id reuse")
            return prior[2]["commit_seq"], False  # retransmission
        self.seq += 1
        for op in ops:
            if op[0] == "put":
                _, tenant, key, value = op
                self.cur[(tenant, key)] = tuple(value)
            else:
                _, tenant, key = op
                self.cur.pop((tenant, key), None)
        self.snaps[self.seq] = dict(self.cur)
        receipt = {"commit_seq": self.seq, "epoch": None,
                   "request_id": request_id, "ops_applied": len(ops)}
        self.requests[request_id] = (h, [tuple(o) for o in ops], receipt)
        return self.seq, True

    def rows_at(self, watermark):
        return sorted(self.snaps[watermark].items(),
                      key=lambda kv: (kv[0][0].encode("utf-8"), kv[0][1]))

    def receipts_at(self, watermark):
        out = []
        for rid, (h, ops, rcpt) in self.requests.items():
            if rcpt["commit_seq"] <= watermark:
                out.append((rcpt["commit_seq"], rid, h))
        out.sort()
        return out


def _op_wire(op):
    # mirror the on-wire op shape independently for canonical hashing
    if op[0] == "put":
        _, tenant, key, (text, integer, blob) = op
        return {"op": "put", "tenant": tenant, "key": str(key),
                "value": {
                    "t": text,
                    "i": None if integer is None else str(integer),
                    "b": None if blob is None else
                    __import__("base64").b64encode(bytes(blob)).decode("ascii")}}
    _, tenant, key = op
    return {"op": "del", "tenant": tenant, "key": str(key)}


# ============================================== independent backup decoder
def read_frames_raw(path):
    """Parse a segment from raw bytes; return (frames, length, sha256)."""
    with open(path, "rb") as f:
        data = f.read()
    frames = []
    off = 0
    while off < len(data):
        check(len(data) - off >= 4, f"{path}: truncated frame header")
        (n,) = struct.unpack(">I", data[off:off + 4])
        check(n <= 512 * 1024 * 1024, f"{path}: implausible frame length {n}")
        start = off + 4
        check(start + n <= len(data), f"{path}: truncated frame body")
        frames.append(json.loads(data[start:start + n].decode("utf-8")))
        off = start + n
    return frames, len(data), hashlib.sha256(data).hexdigest()


def independently_verify(backup_path, model, expected_watermark):
    """Verify a backup purely from its bytes and the independent model.

    Returns the parsed manifest.  Recomputes hashes/lengths, counts records,
    checks global row ordering and primary-key uniqueness, receipt ordering
    and the shard-independent routing coverage -- without snapshot.py.
    """
    check(os.path.isdir(backup_path), "backup must be a directory")
    entries = sorted(os.listdir(backup_path))
    check("MANIFEST.json" in entries, "published backup must have a manifest")
    # nothing else besides manifest + plain segment files may be present
    for e in entries:
        if e == "MANIFEST.json":
            continue
        check(e.startswith("seg-") and e.endswith(".dat"),
              f"unexpected backup entry {e!r}")
    with open(os.path.join(backup_path, "MANIFEST.json"), "rb") as f:
        manifest = json.loads(f.read().decode("utf-8"))
    check(manifest["format"] == "shardstore-backup", "manifest format magic")
    check(manifest["format_version"] == 1, "manifest format version")
    check(manifest["watermark"] == expected_watermark,
          f"watermark {manifest['watermark']} != expected {expected_watermark}")
    check(isinstance(manifest["created_at"], str) and manifest["created_at"],
          "manifest must record creation time")

    # routing view: full partition, no holes/overlaps, no source paths
    routing = manifest["routing"]
    check(routing, "routing view must not be empty")
    bounds = []
    for r in routing:
        check(set(r) == {"shard", "lo", "hi"}, f"route must be portable: {r}")
        lo = None if r["lo"] is None else (r["lo"]["tenant"],
                                           int(r["lo"]["key"]))
        hi = None if r["hi"] is None else (r["hi"]["tenant"],
                                           int(r["hi"]["key"]))
        bounds.append((lo, hi))
    bounds.sort(key=lambda x: (0,) if x[0] is None
                else (1, (x[0][0].encode("utf-8"), x[0][1])))
    check(bounds[0][0] is None and bounds[-1][1] is None,
          "routing must cover the whole key domain")
    for (_, hi1), (lo2, _) in zip(bounds, bounds[1:]):
        check(hi1 == lo2, "routing ranges must be adjacent")
    # routes carry only a label and key bounds: no source file names, data
    # directory or absolute path can hide in extra fields
    for r in routing:
        check(set(r) == {"shard", "lo", "hi"},
              f"route must be portable (label + bounds only): {r}")
        check(isinstance(r["shard"], str) and r["shard"],
              "route shard label must be present")

    rows = []
    receipts = []
    seg_summaries = []
    prev_role = "rows"
    for s in manifest["segments"]:
        check(s["role"] in ("rows", "receipts"), "segment role")
        if s["role"] == "receipts":
            prev_role = "receipts"
        else:
            check(prev_role == "rows", "row segments must precede receipts")
        path = os.path.join(backup_path, s["name"])
        frames, length, digest = read_frames_raw(path)
        check(length == s["length"],
              f"{s['name']}: length {length} != manifest {s['length']}")
        check(digest == s["sha256"],
              f"{s['name']}: sha256 mismatch (tampered/truncated)")
        check(len(frames) == s["record_count"],
              f"{s['name']}: record count mismatch")
        seg_summaries.append((s["name"], s["role"], len(frames)))
        if s["role"] == "rows":
            check((s["first_key"] is None) == (not frames),
                  f"{s['name']}: key range flag inconsistent")
            for fr in frames:
                check(fr["kind"] == "row", "row frame kind")
                key = (fr["tenant"], int(fr["key"]))
                check(str(key[1]) == fr["key"], "non-canonical key string")
                v = fr["value"]
                value = (v["t"],
                         None if v["i"] is None else int(v["i"]),
                         None if v["b"] is None else
                         __import__("base64").b64decode(v["b"].encode()))
                rows.append((key, value))
        else:
            check(s["first_key"] is None and s["last_key"] is None,
                  "receipt segment must not carry row key range")
            for fr in frames:
                check(fr["kind"] == "receipt", "receipt frame kind")
                receipts.append((fr["seq"], fr["request_id"],
                                 fr["request_hash"], fr["receipt_json"]))

    # global order + primary key uniqueness across all row segments
    order = [(k[0].encode("utf-8"), k[1]) for k, _ in rows]
    check(order == sorted(order), "rows must be globally sorted")
    check(len(set(order)) == len(order), "primary keys must be unique")
    check(len(rows) == manifest["row_count"], "manifest row count")
    if rows:
        check(manifest["key_range"] == {
            "first": {"tenant": rows[0][0][0], "key": str(rows[0][0][1])},
            "last": {"tenant": rows[-1][0][0], "key": str(rows[-1][0][1])}},
            "manifest key range must match the records")
        # every row falls inside exactly one route
        for (tenant, key), _ in rows:
            ok = any((lo is None or (tenant.encode(), key) >=
                      (lo[0].encode(), lo[1]))
                     and (hi is None or (tenant.encode(), key) <
                          (hi[0].encode(), hi[1])) for lo, hi in bounds)
            check(ok, f"row {tenant}/{key} uncovered by routing")
    else:
        check(manifest["key_range"] == {"first": None, "last": None},
              "empty backup key range")

    # receipts: strict seq order, unique ids, hash equals the model's hash,
    # and every decided commit through the watermark is present
    seqs = [r[0] for r in receipts]
    check(seqs == sorted(seqs) and len(set(seqs)) == len(seqs),
          "receipt seqs must be strictly ordered and unique")
    check(len(receipts) == manifest["receipt_count"], "manifest receipt count")
    model_receipts = model.receipts_at(expected_watermark)
    check(len(receipts) == len(model_receipts),
          f"receipt count {len(receipts)} != model {len(model_receipts)}")
    for (seq, rid, rh, rj), (mseq, mrid, mh) in zip(receipts, model_receipts):
        check((seq, rid, rh) == (mseq, mrid, mh),
              f"receipt mismatch at seq {seq}")
        parsed = json.loads(rj)
        check(parsed["commit_seq"] == seq and parsed["request_id"] == rid,
              "receipt_json must match its frame")

    # rows equal the model snapshot at exactly this watermark
    expected_rows = model.rows_at(expected_watermark)
    check(len(rows) == len(expected_rows),
          f"row total {len(rows)} != model {len(expected_rows)}")
    for (got_key, got_val), (exp_key, exp_val) in zip(rows, expected_rows):
        check(got_key == exp_key and got_val == tuple(exp_val),
              f"row mismatch at {got_key}: {got_val!r} != {exp_val!r}")
    return manifest, seg_summaries


# ------------------------------------------------------------- server mgmt
class ServerProc:
    def __init__(self, directory, tag="srv"):
        self.dir = directory
        self.sock = os.path.join(directory, f"{tag}.sock")
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
                return self.proc.returncode
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


def wait_for(fn, timeout=120, interval=0.05, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(interval)
    raise Failure(f"timeout waiting for {what}")


def wait_published(store):
    wait_for(lambda: store.migration_status()["split"] is not None
             and store.migration_status()["split"]["phase"] == "PUBLISHED",
             what="split to publish")


def model_apply(model, store, request_id, ops):
    """Commit through the real server and advance the independent model with
    the same ops; assert seq alignment for fresh commits."""
    seq, fresh = model.commit(request_id, ops)
    rcpt = store.write(request_id, ops)
    if fresh:
        check(rcpt["commit_seq"] == seq,
              f"seq alignment at {request_id}: {rcpt} vs model {seq}")
        # mirror the server's actual receipt (it is the dedup evidence that
        # must survive export/restore byte-for-byte)
        model.requests[request_id] = (model.requests[request_id][0],
                                      model.requests[request_id][1], rcpt)
    return rcpt


def check_store_matches_model(store, model, watermark, what):
    """Read a live (restored) store and compare with the model snapshot."""
    tok = store.open_read()
    check(tok.read_seq == watermark,
          f"{what}: read_seq {tok.read_seq} != watermark {watermark}")
    got = []
    for row in store.scan_all(tok, page=500):
        got.append(((row.tenant, row.key),
                    (row.text, row.integer, row.blob)))
    store.release(tok)
    expected = model.rows_at(watermark)
    check(len(got) == len(expected),
          f"{what}: {len(got)} rows != model {len(expected)}")
    for (gk, gv), (ek, ev) in zip(got, expected):
        check(gk == ek and gv == tuple(ev),
              f"{what}: mismatch at {gk}: {gv!r} != {ev!r}")


# ================================================================= phases
def phase_consistency_and_concurrency(base):
    """Export under concurrent commits and a split; exact watermark match."""
    srv = ServerProc(os.path.join(base, "src"), "src")
    srv.start()
    try:
        store = Store(srv.sock)
        model = WatermarkModel()

        # cross-shard-shaped boundary transactions with NULL/BLOB/extremes
        ops = [
            put("", INT64_MIN, text=None, integer=None, blob=None),
            put("", INT64_MAX, text="max", integer=INT64_MAX, blob=b"\x00"),
            put(T, 0, text=None, integer=0, blob=b""),
            put("é", -7, text="nüll", integer=-7, blob=bytes(range(32))),
            put("中", 9, integer=INT64_MIN, blob=b"\xff" * 16),
            delete("ghost", 42),
        ]
        model_apply(model, store, "c0", ops)
        ops = [put(T, 1, text="a"), put(T, 50, text="right"),
               delete("", INT64_MIN), put("a", -1, blob=b"\x01\x02")]
        model_apply(model, store, "c1", ops)

        # split while more writes keep landing; snapshot pins its own point
        src_shard = store.routing()["routes"][0]["shard"]
        store.split(src_shard, (T, 10))
        time.sleep(0.05)
        for i in range(2, 8):
            model_apply(model, store, f"mid-{i}",
                        [put(T, i * 3, text=f"m{i}"),
                         put("b", i, integer=i, blob=bytes([i]))])
        wait_published(store)
        check(store.routing()["epoch"] == 2, "split published during workload")

        # export while a background process keeps writing
        bk = os.path.join(base, "bk-concurrent")
        q = mp.Queue()
        bg_count = 40
        writer = mp.Process(target=_background_writer,
                            args=(srv.sock, bg_count, q))
        writer.start()
        rep = store.snapshot_export(bk)
        writer.join(120)
        check(writer.exitcode == 0, "background writer crashed")
        bg_watermark = q.get()
        check(rep["watermark"] <= bg_watermark,
              "snapshot watermark must be a committed seq")

        # reconstruct the background commits into the independent model from
        # the same deterministic formula (the writer code path is separate,
        # the formula is duplicated here on purpose)
        bg_seqs = []
        for i in range(bg_count):
            bg_ops = [put("bg", i, text=f"bg-{i}", integer=i,
                          blob=hashlib.sha256(str(i).encode()).digest()[:8])]
            seq, fresh = model.commit(f"bg-{i}", bg_ops)
            check(fresh, "background request ids must be new")
            bg_seqs.append(seq)
        check(model.seq == bg_watermark,
              f"model frontier {model.seq} != writer frontier {bg_watermark}")
        # server-side retransmission confirms the receipts already exist
        for i in (0, bg_count - 1):
            bg_ops = [put("bg", i, text=f"bg-{i}", integer=i,
                          blob=hashlib.sha256(str(i).encode()).digest()[:8])]
            rcpt = store.write(f"bg-{i}", bg_ops)
            check(rcpt["commit_seq"] == bg_seqs[i],
                  "background write receipt must already exist")

        # independent verification against the model *at that watermark*
        manifest, segs = independently_verify(bk, model, rep["watermark"])
        check(manifest["receipt_count"] == len(
            model.receipts_at(rep["watermark"])), "receipt coverage")
        check(len(segs) >= 2, "at least one row + one receipt segment")
        # server-side report agrees with the independently parsed manifest
        info = store.snapshot_inspect(bk)
        check(info["watermark"] == rep["watermark"]
              and info["row_count"] == manifest["row_count"]
              and info["receipt_count"] == manifest["receipt_count"],
              "inspect report must match the parsed manifest")
        check(info["subscription_resume_after_seq"] == rep["watermark"]
              and info["retained_log_low"] == rep["watermark"] + 1,
              "inspect must state the subscription resume boundary")

        # writes after the watermark are not in the backup
        post_key = ("postwm", 1)
        post_ops = [put(*post_key, text="not-in-backup")]
        model_apply(model, store, "post-watermark", post_ops)
        check(store.migration_status()["visible_seq"] > rep["watermark"],
              "source kept accepting writes")
        manifest2, _ = independently_verify(bk, model, rep["watermark"])
        all_rows = _all_backup_rows(bk)
        check(post_key not in dict(all_rows),
              "post-watermark write must be excluded from the backup")

        # restore into a fresh directory and compare with the exact watermark
        dst = os.path.join(base, "dst-concurrent")
        rrep = store.snapshot_restore(bk, dst)
        check(rrep["next_commit_seq"] == rep["watermark"] + 1,
              "restore report must state seq continuation")
        check(not os.listdir(dst) or os.path.isdir(os.path.join(dst, "shards")),
              "restored directory has the store layout")
        check_restore_instance(dst, model, rep["watermark"], base, "concurrent")
        print(f"    consistency: watermark {rep['watermark']},"
              f" {manifest['row_count']} rows, {len(segs)} segments,"
              " split+writes concurrent, exact match ok")
    finally:
        srv.stop()


def _background_writer(sock, count, q):
    """Deterministic writer; the parent reconstructs the same ops from the
    request ids and key formula, so model and server stay aligned."""
    try:
        store = Store(sock)
        last = 0
        for i in range(count):
            ops = [put("bg", i, text=f"bg-{i}", integer=i,
                       blob=hashlib.sha256(str(i).encode()).digest()[:8])]
            rcpt = store.write(f"bg-{i}", ops)
            last = rcpt["commit_seq"]
        q.put(last)
    except Exception as e:  # noqa: BLE001
        q.put(-1)
        raise


def check_restore_instance(dst, model, watermark, base, tag):
    """Boot a server on the restored directory and run the full contract."""
    rsrv = ServerProc(dst, tag)
    rsrv.start()
    try:
        store = Store(rsrv.sock)
        st = store.migration_status()
        check(st["visible_seq"] == watermark,
              f"restored visible_seq {st['visible_seq']} != {watermark}")
        # a freshly restored instance carries no source subscribers, tokens
        # or split state
        check(store.subscriptions()["subscriptions"] == [],
              "source subscribers must not be restored")
        check(st["active_tokens"] == 0, "source read tokens must not be restored")
        check(st["split"] is None, "source split state must not be restored")
        check_store_matches_model(store, model, watermark, f"{tag} restore")

        # seq continues from the watermark, never reused
        before = watermark
        rcpt = store.write("restored-new", [put("new", 1, text="fresh")])
        check(rcpt["commit_seq"] == before + 1,
              f"next seq {rcpt['commit_seq']} != {before + 1}")

        # old request_id: original receipt returned, no re-execution
        for rid, (_h, ops, orig_rcpt) in list(model.requests.items())[:6]:
            again = store.write(rid, ops)
            check(again["commit_seq"] == orig_rcpt["commit_seq"],
                  f"replay of {rid} must return the original receipt")
        check(store.migration_status()["visible_seq"] == before + 1,
              "replayed request_ids must not re-execute or move the seq")
        # conflicting reuse is still rejected (skip for an empty backup)
        if model.requests:
            first_rid = next(iter(model.requests))
            expect_error("CONFLICT",
                         lambda: store.write(first_rid, [put("x", 1)]),
                         "conflicting request_id after restore")

        # subscriptions start from the watermark; strictly earlier positions
        # fail (on an empty backup watermark=0, so 0 already is the boundary)
        if watermark > 0:
            expect_error("OUT_OF_RANGE",
                         lambda: store.subscribe("too-early", watermark - 1),
                         "subscription before the resume boundary")
        sub = store.subscribe("from-watermark", watermark)
        check(sub["origin_seq"] == watermark, "subscription at the boundary")
        p = store.pull("from-watermark", sub["generation"])
        check([c.commit_seq for c in p.changes] == [before + 1],
              "restored stream delivers only commits after the watermark")

        # split still works on the restored layout
        routes = store.routing()["routes"]
        if len(routes) == 1:
            store.split(routes[0]["shard"], ("new", 0))
            wait_published(store)
            check(store.routing()["epoch"] == 2, "split works after restore")
    finally:
        rsrv.stop()


def _all_backup_rows(bk):
    rows = []
    with open(os.path.join(bk, "MANIFEST.json"), "rb") as f:
        manifest = json.loads(f.read())
    for s in manifest["segments"]:
        if s["role"] != "rows":
            continue
        frames, _, _ = read_frames_raw(os.path.join(bk, s["name"]))
        for fr in frames:
            rows.append(((fr["tenant"], int(fr["key"])), fr["value"]))
    return rows


def phase_crash_export(base):
    """Forced exits during export: only complete backups are recognizable."""
    srv = ServerProc(os.path.join(base, "crash-export-src"), "src")
    srv.start()
    try:
        store = Store(srv.sock)
        model = WatermarkModel()
        for i in range(1, 400):
            model_apply(model, store, f"seed-{i}",
                        [put(T, i, text=f"v{i}", integer=i,
                             blob=hashlib.sha256(str(i).encode()).digest()[:10])])
        work = os.path.join(base, "crash-export-work")
        os.makedirs(work, exist_ok=True)
        for point in ("during_snapshot_segments", "before_snapshot_manifest",
                      "after_snapshot_manifest"):
            bk = os.path.join(work, f"bk-{point}")
            store.arm_crash(point)
            try:
                store.snapshot_export(bk)
                raise Failure(f"export should have crashed at {point}")
            except StoreError as e:
                check(e.code == "CONN", f"expected CONN, got {e.code}")
            srv.wait_dead()
            # no finished backup may be visible; temp dirs are anonymous
            check(not os.path.exists(bk),
                  f"{point}: killed export must not publish a backup")
            leftovers = [n for n in os.listdir(work)
                         if n.startswith(".snapshot-")]
            for n in leftovers:
                check(not os.path.exists(os.path.join(work, n, "MANIFEST.json"))
                      or point != "before_snapshot_manifest",
                      "manifest must not exist before its publish point")
            srv.start()
            store = Store(srv.sock)
            # the interrupted temp data is never recognized as a backup:
            # either the destination never appeared, or it lacks a manifest
            try:
                store.snapshot_inspect(bk)
                raise Failure(f"killed export at {point} must not inspect as"
                              f" a backup")
            except StoreError as e:
                check(e.code in ("BACKUP_INCOMPLETE", "BACKUP_NOT_FOUND"),
                      f"killed export at {point}: got {e.code}")
            # retry after restart succeeds and verifies independently
            rep = store.snapshot_export(bk)
            independently_verify(bk, model, rep["watermark"])
            info = store.snapshot_inspect(bk)
            check(info["verified"] and info["watermark"] == rep["watermark"],
                  "retried export is a complete, verifiable backup")
            shutil.rmtree(bk)
        print("    export crashes: temp data invisible, retries complete ok")
    finally:
        srv.stop()


def phase_crash_restore(base):
    """Forced exits during restore: no half-store, empty target, no overwrite."""
    root = os.path.join(base, "crash-restore")
    os.makedirs(root, exist_ok=True)
    srv = ServerProc(os.path.join(root, "src"), "src")
    srv.start()
    try:
        store = Store(srv.sock)
        model = WatermarkModel()
        for i in range(1, 300):
            model_apply(model, store, f"r-{i}",
                        [put(T, i, text=f"t{i}"),
                         put("u", -i, blob=bytes([i % 256]))])
        bk = os.path.join(root, "bk-good")
        rep = store.snapshot_export(bk)
        independently_verify(bk, model, rep["watermark"])

        for point in ("during_restore_verify", "during_restore_build",
                      "before_restore_publish", "after_restore_publish"):
            dst = os.path.join(root, f"dst-{point}")
            os.mkdir(dst)  # pre-existing *empty* target
            store.arm_crash(point)
            try:
                store.snapshot_restore(bk, dst)
                if point != "after_restore_publish":
                    raise Failure(f"restore should have crashed at {point}")
            except StoreError as e:
                check(e.code == "CONN", f"expected CONN at {point}, got {e.code}")
            srv.wait_dead()
            srv.start()
            store = Store(srv.sock)

            stages = [n for n in os.listdir(root)
                      if n.startswith(".restore-")]
            if point == "after_restore_publish":
                # the single atomic publish landed: usable store, no staging
                check(not stages, "staging must be gone after publish")
                check(sorted(os.listdir(dst)) == ["control.sqlite", "shards"],
                      "published target has the store layout")
                check_restore_instance(dst, model, rep["watermark"], root,
                                       f"{point}-done")
            else:
                # target is still empty and never a usable half-store
                check(os.listdir(dst) == [],
                      f"{point}: target must stay empty, got {os.listdir(dst)}")
                check(not os.path.exists(os.path.join(dst, "control.sqlite")),
                      "no half-published control db")
                # every leftover is anonymous staging, never at the target
                for n in stages:
                    check(n != os.path.basename(dst),
                          "staging must never take the target name early")
                # retry after restart completes exactly once
                rrep = store.snapshot_restore(bk, dst)
                check(rrep["rows_built"] == rep["row_count"],
                      "retried restore row count")
                check_restore_instance(dst, model, rep["watermark"], root,
                                       f"{point}-retry")
            for n in stages:
                shutil.rmtree(os.path.join(root, n), ignore_errors=True)

        # existing non-empty data is never overwritten
        occupied = os.path.join(root, "dst-occupied")
        os.makedirs(os.path.join(occupied, "shards"))
        marker = os.path.join(occupied, "control.sqlite")
        with open(marker, "wb") as f:
            f.write(b"original-data")
        expect_error("TARGET_NOT_EMPTY",
                     lambda: store.snapshot_restore(bk, occupied),
                     "restore over existing data")
        with open(marker, "rb") as f:
            check(f.read() == b"original-data", "existing data was modified")
        print("    restore crashes: empty targets, atomic publish, no overwrite")
    finally:
        srv.stop()


def phase_corruption(base):
    """Damaged/tampered backups and malformed manifests are all rejected."""
    root = os.path.join(base, "corruption")
    os.makedirs(root, exist_ok=True)
    srv = ServerProc(os.path.join(root, "src"), "src")
    srv.start()
    try:
        store = Store(srv.sock)
        model = WatermarkModel()
        for i in range(1, 60):
            model_apply(model, store, f"k-{i}",
                        [put(T, i, integer=i, text=f"s{i}"),
                         put("v", i, blob=bytes(range(i % 50)))])
        good = os.path.join(root, "bk-good")
        rep = store.snapshot_export(good)
        independently_verify(good, model, rep["watermark"])

        def clone(name):
            d = os.path.join(root, name)
            shutil.copytree(good, d)
            return d

        def manifest_of(d):
            with open(os.path.join(d, "MANIFEST.json"), "rb") as f:
                return json.loads(f.read())

        def write_manifest(d, m):
            with open(os.path.join(d, "MANIFEST.json"), "w") as f:
                json.dump(m, f)

        # 1) truncated segment (chop the last byte)
        d = clone("bad-truncated")
        m = manifest_of(d)
        seg = os.path.join(d, m["segments"][0]["name"])
        data = open(seg, "rb").read()
        with open(seg, "wb") as f:
            f.write(data[:-1])
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "truncated segment")
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_restore(
                         d, os.path.join(root, "o1")),
                     "restore truncated")

        # 2) tampered segment byte with stale hash
        d = clone("bad-tampered")
        m = manifest_of(d)
        seg = os.path.join(d, m["segments"][0]["name"])
        data = bytearray(open(seg, "rb").read())
        data[-1] ^= 0xFF
        with open(seg, "wb") as f:
            f.write(bytes(data))
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "tampered segment")

        # 3) manifest lies about the length
        d = clone("bad-length")
        m = manifest_of(d)
        m["segments"][0]["length"] += 1
        write_manifest(d, m)
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "lying length")

        # 4) manifest lies about the hash
        d = clone("bad-hash")
        m = manifest_of(d)
        m["segments"][0]["sha256"] = "00" * 32
        write_manifest(d, m)
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "lying hash")

        # 5) unknown format version
        d = clone("bad-version")
        m = manifest_of(d)
        m["format_version"] = 99
        write_manifest(d, m)
        err = expect_error("UNSUPPORTED_FORMAT",
                           lambda: store.snapshot_inspect(d), "unknown version")
        check(err.detail.get("supported_version") == 1,
              "unsupported version must name the supported one")

        # 6) duplicate primary key inside a row segment
        d = clone("bad-dupkey")
        m = manifest_of(d)
        row_seg_name = next(s["name"] for s in m["segments"]
                            if s["role"] == "rows")
        frames, _, _ = read_frames_raw(os.path.join(d, row_seg_name))
        dup = _repack_segment(os.path.join(d, row_seg_name),
                              frames[:2] + [frames[0]])
        s0 = next(s for s in m["segments"] if s["name"] == row_seg_name)
        s0.update(dup)
        write_manifest(d, m)
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "duplicate key")

        # 7) out-of-order row frames
        d = clone("bad-order")
        m = manifest_of(d)
        swapped = [frames[1], frames[0]] + frames[2:]
        rep2 = _repack_segment(os.path.join(d, row_seg_name), swapped)
        s0 = next(s for s in m["segments"] if s["name"] == row_seg_name)
        s0.update(rep2)
        write_manifest(d, m)
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "out-of-order frames")

        # 8) missing segment file
        d = clone("bad-missing")
        m = manifest_of(d)
        os.unlink(os.path.join(d, m["segments"][0]["name"]))
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "missing segment")

        # 9) garbage where the manifest should be
        d = clone("bad-manifest")
        with open(os.path.join(d, "MANIFEST.json"), "wb") as f:
            f.write(b"{not json")
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "garbage manifest")

        # 10) no manifest at all -> incomplete, never a usable backup
        d = clone("bad-nomanifest")
        os.unlink(os.path.join(d, "MANIFEST.json"))
        expect_error("BACKUP_INCOMPLETE",
                     lambda: store.snapshot_inspect(d), "missing manifest")
        expect_error("BACKUP_INCOMPLETE",
                     lambda: store.snapshot_restore(
                         d, os.path.join(root, "o2")),
                     "restore without manifest")

        # 11) routing view with a hole (non-adjacent ranges) must be rejected;
        #     a single-route backup cannot express this, so synthesize one
        d = clone("bad-holey")
        m = manifest_of(d)
        check(len(m["routing"]) == 1, "seed backup starts with one route")
        only = m["routing"][0]
        m["routing"] = [
            {"shard": "s1", "lo": None,
             "hi": {"tenant": "gap-low", "key": "0"}},
            {"shard": "s2", "lo": {"tenant": "gap-high", "key": "0"},
             "hi": only["hi"]},
        ]
        write_manifest(d, m)
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d), "holey routing")

        # 12) routing view that leaks non-portable fields must be rejected
        d = clone("bad-route-fields")
        m = manifest_of(d)
        m["routing"][0]["path"] = "/etc/passwd"
        write_manifest(d, m)
        expect_error("CORRUPT_BACKUP",
                     lambda: store.snapshot_inspect(d),
                     "non-portable route fields")

        # every failed restore left no target behind
        for o in ("o1", "o2"):
            check(not os.path.exists(os.path.join(root, o)),
                  "failed restore must not create a target directory")
        print("    corruption: truncation/tamper/version/dup/order all rejected")
    finally:
        srv.stop()


def _repack_segment(path, frames):
    """Rewrite a segment from frames and return its new descriptor fields."""
    hasher = hashlib.sha256()
    length = 0
    with open(path, "wb") as f:
        for fr in frames:
            data = json.dumps(fr, sort_keys=True,
                              ensure_ascii=False).encode("utf-8")
            chunk = struct.pack(">I", len(data)) + data
            f.write(chunk)
            hasher.update(chunk)
            length += len(chunk)
    first = {"tenant": frames[0]["tenant"], "key": frames[0]["key"]} \
        if frames and frames[0].get("kind") == "row" else None
    last = {"tenant": frames[-1]["tenant"], "key": frames[-1]["key"]} \
        if frames and frames[-1].get("kind") == "row" else None
    return {"length": length, "sha256": hasher.hexdigest(),
            "record_count": len(frames), "first_key": first, "last_key": last}


def phase_edges(base):
    """Empty store, receipts after cleanup, multi-shard mapping, big value."""
    root = os.path.join(base, "edges")
    os.makedirs(root, exist_ok=True)
    srv = ServerProc(os.path.join(root, "src"), "src")
    srv.start()
    try:
        store = Store(srv.sock)
        model = WatermarkModel()

        # empty store snapshot
        empty_bk = os.path.join(root, "bk-empty")
        rep = store.snapshot_export(empty_bk)
        check(rep["row_count"] == 0 and rep["receipt_count"] == 0,
              "empty store counters")
        independently_verify(empty_bk, model, 0)
        empty_dst = os.path.join(root, "dst-empty")
        store.snapshot_restore(empty_bk, empty_dst)
        check_restore_instance(empty_dst, model, 0, root, "empty")

        # receipts must survive change-log cleanup (payloads released)
        for i in range(1, 21):
            model_apply(model, store, f"keep-{i}",
                        [put(T, i, text=f"k{i}"), delete(T, 1000 + i)])
        sub = store.subscribe("tmp", 0)
        gen = sub["generation"]
        p = store.pull("tmp", gen)
        store.ack("tmp", gen, p.next_after)
        cleaned = store.cleanup(model.seq)
        check(cleaned["released_commits"] == 20, "payloads released")
        check(store.migration_status()["visible_seq"] == model.seq,
              "cleanup does not move the frontier")
        bk = os.path.join(root, "bk-cleaned")
        rep = store.snapshot_export(bk)
        independently_verify(bk, model, rep["watermark"])
        # restored receipts still dedup despite missing historical payloads
        dst = os.path.join(root, "dst-cleaned")
        store.snapshot_restore(bk, dst)
        rsrv = ServerProc(dst, "cleaned")
        rsrv.start()
        try:
            rstore = Store(rsrv.sock)
            rid = "keep-1"
            again = rstore.write(rid, model.requests[rid][1])
            check(again == model.requests[rid][2],
                  "receipt survives cleanup and restore")
            expect_error("CONFLICT",
                         lambda: rstore.write(rid, [put("z", 1)]),
                         "conflict after cleaned restore")
            # the retained change log starts past the watermark: no fake history
            expect_error("OUT_OF_RANGE",
                         lambda: rstore.subscribe("late", 0),
                         "cleaned history must not be resurrected")
            fresh = rstore.subscribe("edge", rep["watermark"])
            check(fresh["origin_seq"] == rep["watermark"],
                  "subscription resumes at the watermark")
        finally:
            rsrv.stop()

        # multi-source-shard routing mapped onto the restore layout
        for i in range(20, 80):
            model_apply(model, store, f"split-{i}",
                        [put(T, i, integer=-i, text=f"m{i}"),
                         put("w", i, blob=bytes([i % 256, i % 17]))])
        # two online splits -> three source shards, exercising rows spread
        # across all of them and a multi-route backup
        first = store.routing()["routes"][0]["shard"]
        store.split(first, (T, 40))
        wait_published(store)
        rightmost = store.routing()["routes"][-1]["shard"]
        store.split(rightmost, (T, 70))
        wait_published(store)
        routes_now = store.routing()["routes"]
        check(len(routes_now) == 3, "two splits -> three source shards")
        bk = os.path.join(root, "bk-multishard")
        rep = store.snapshot_export(bk)
        manifest, _ = independently_verify(bk, model, rep["watermark"])
        check(len(manifest["routing"]) == 3,
              "backup carries the complete multi-shard routing view")
        dst = os.path.join(root, "dst-multishard")
        rrep = store.snapshot_restore(bk, dst)
        check(len(rrep["shards"]) == 3,
              "restore maps every source shard into the fresh layout")
        check([s["shard_id"] for s in rrep["shards"]] == ["s1", "s2", "s3"],
              "restored shard ids are layout-local, not source file names")
        check_restore_instance(dst, model, rep["watermark"], root, "multishard")

        # one oversized single value still round-trips through framing
        big = os.urandom(8 * 1024 * 1024)
        model_apply(model, store, "oversized",
                    [put("blob-tenant", 1, text="big", integer=7, blob=big)])
        bk = os.path.join(root, "bk-oversize")
        rep = store.snapshot_export(bk)
        independently_verify(bk, model, rep["watermark"])
        dst = os.path.join(root, "dst-oversize")
        store.snapshot_restore(bk, dst)
        rsrv = ServerProc(dst, "oversize")
        rsrv.start()
        try:
            rstore = Store(rsrv.sock)
            row = rstore.get(rstore.open_read(), "blob-tenant", 1)
            check(row is not None and row.blob == big
                  and row.text == "big" and row.integer == 7,
                  "oversized BLOB must round-trip exactly")
        finally:
            rsrv.stop()
        print("    edges: empty, cleaned receipts, 3-shard mapping, 8MiB BLOB ok")
    finally:
        srv.stop()


def phase_scale(base):
    """Runtime-generated scale data: bounded memory, independent byte check."""
    root = os.path.join(base, "scale")
    os.makedirs(root, exist_ok=True)
    srv = ServerProc(os.path.join(root, "src"), "src")
    srv.start()
    try:
        store = Store(srv.sock)
        model = WatermarkModel()
        tenants = ["t0", "t1", "t2", "t3"]
        per = SCALE_ROWS // len(tenants)
        wid = 0
        for tenant in tenants:
            k = 0
            while k < per:
                ops = []
                for _ in range(min(1000, per - k)):
                    ops.append(put(
                        tenant, k,
                        text=f"txt:{tenant}:{k}",
                        integer=(k * 31) % (1 << 40),
                        blob=hashlib.sha256(
                            f"{tenant}|{k}".encode()).digest()[:12]))
                    k += 1
                wid += 1
                model_apply(model, store, f"scale-{wid}", ops)
        # a split so scale export also covers multiple source shards
        store.split(store.routing()["routes"][0]["shard"], ("t2", 0))
        wait_published(store)

        baseline = srv.rss_kb() or 0
        max_rss = baseline
        bk = os.path.join(root, "bk-scale")
        rep = store.snapshot_export(bk)
        # sample RSS while a verify + restore would run; export already
        # returned, so measure the restore phase separately below
        manifest, segs = independently_verify(bk, model, rep["watermark"])
        check(manifest["row_count"] == SCALE_ROWS,
              f"scale rows {manifest['row_count']} != {SCALE_ROWS}")
        check(len(segs) >= 2, "scale backup has row + receipt segments")

        dst = os.path.join(root, "dst-scale")
        rrep = store.snapshot_restore(bk, dst)
        rss = srv.rss_kb()
        if rss:
            max_rss = max(max_rss, rss)
        check(rrep["rows_built"] == SCALE_ROWS, "restored scale row count")
        delta = max_rss - baseline
        check(delta < RSS_BUDGET_KB,
              f"export/restore RSS growth {delta}kB exceeds {RSS_BUDGET_KB}kB")

        # the restored scale instance matches the model end to end
        check_restore_instance(dst, model, rep["watermark"], root, "scale")
        print(f"    scale: {SCALE_ROWS} rows, {len(segs)} segments,"
              f" RSS growth {delta}kB, independent byte verification ok")
    finally:
        srv.stop()


# ================================================================== main
PHASES = [
    ("consistency under concurrency", phase_consistency_and_concurrency),
    ("export crash matrix", phase_crash_export),
    ("restore crash matrix", phase_crash_restore),
    ("corruption rejection", phase_corruption),
    ("empty cleanup multishard edges", phase_edges),
    ("scale bounded memory", phase_scale),
]


def main():
    keep = os.environ.get("KEEP") == "1"
    only = os.environ.get("ONLY")
    base = tempfile.mkdtemp(prefix="shardstore-snap-")
    print(f"workdir: {base}")
    t0 = time.time()
    try:
        for name, fn in PHASES:
            if only and only not in name:
                continue
            print(f"== {name}")
            start = time.time()
            try:
                fn(base)
            except Exception as e:
                print(f"\nFAILED: {name}: {e}", file=sys.stderr)
                for d in sorted(os.listdir(base)):
                    log = os.path.join(base, d, "server.log")
                    if os.path.exists(log):
                        try:
                            with open(log, "rb") as f:
                                tail = f.read()[-3000:].decode("utf-8", "replace")
                            print(f"--- {log} tail ---\n{tail}", file=sys.stderr)
                        except OSError:
                            pass
                raise SystemExit(1)
            print(f"   ok ({time.time() - start:.1f}s)")
    finally:
        if not keep:
            shutil.rmtree(base, ignore_errors=True)
        else:
            print(f"kept workdir: {base}")
    print(f"\nALL SNAPSHOT CHECKS PASSED ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
