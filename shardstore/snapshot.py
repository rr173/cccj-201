"""Portable, consistent snapshot export/restore for shardstore.

A backup is a self-contained directory that never mentions the source data
directory, source shard file names or absolute paths::

    <backup>/
      MANIFEST.json     root of trust: watermark, format version, counts,
                        shard-independent key ranges and routing view, and
                        for every data segment its byte length and SHA-256
      seg-000001.dat    length-prefixed JSON frame streams (rows, then
      seg-000002.dat    idempotency receipts); content-addressed by the
      ...               hashes in the manifest

Frames are ``uint32 big-endian length + JSON bytes`` (the same framing idea
as the socket protocol; int64 keys travel as decimal strings, BLOBs as
base64).  Two record kinds exist:

  row     -> {"kind":"row","tenant":..,"key":"<i64>",
              "value":{"t":..,"i":..,"b":..}}
              globally ordered by (tenant UTF-8 bytes, signed key), unique
  receipt -> {"kind":"receipt","seq":N,"request_id":..,"request_hash":..,
              "receipt_json":..}
              the original dedup receipt; historical change payloads are
              deliberately *not* part of a backup

Everything here is streamed: writers keep one open segment plus a hash
state, readers parse one frame at a time and verification holds only the
previous key plus counters -- never the whole database or receipt table.

The module is usable offline (see the CLI in ``main``) and has no dependency
on a running server; the server-side export driver sources rows/receipts and
feeds them to the streaming writers in this module.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import struct
import sys

from shardstore import common

SNAPSHOT_FORMAT = "shardstore-backup"
SNAPSHOT_VERSION = 1
MANIFEST_NAME = "MANIFEST.json"
# rotate a segment between records once it reaches this size; a single
# oversized record still lands whole in its segment
SEGMENT_TARGET_BYTES = 64 * 1024 * 1024
RESTORE_BATCH = 500


def segment_target_bytes():
    """Rotation threshold, overridable for tests via SHARDSTORE_SEG_TARGET.

    The override is read on every call so a running server need not be
    rebuilt to exercise the multi-segment path.  Production deployments
    leave the variable unset and get the fixed 64 MiB default.
    """
    raw = os.environ.get("SHARDSTORE_SEG_TARGET")
    if not raw:
        return SEGMENT_TARGET_BYTES
    try:
        value = int(raw)
    except ValueError:
        return SEGMENT_TARGET_BYTES
    return value if value >= 1024 else SEGMENT_TARGET_BYTES
# a corrupt length prefix must never make us allocate gigabytes
MAX_FRAME_BYTES = common.MAX_MSG


class SnapshotError(Exception):
    def __init__(self, code, message, detail=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}


# ------------------------------------------------------------- key encoding
def enc_bound(b):
    """None | (tenant, key) -> None | wire bound (key as decimal string)."""
    if b is None:
        return None
    return {"tenant": b[0], "key": str(b[1])}


def dec_bound(b):
    if b is None:
        return None
    return (b["tenant"], int(b["key"]))


def enc_key(tenant, key):
    return {"tenant": tenant, "key": str(key)}


def dec_key(k):
    return (k["tenant"], int(k["key"]))


def _order_key(tenant, key):
    return (tenant.encode("utf-8"), key)


# ------------------------------------------------------------- segment io
class SegmentWriter:
    """Append-only frame writer that hashes exactly the bytes it lands."""

    def __init__(self, path, name):
        self.name = name
        self.path = path
        self.f = open(path, "wb")
        self.hasher = hashlib.sha256()
        self.length = 0
        self.record_count = 0
        self.first_key = None
        self.last_key = None

    def write_frame(self, obj):
        data = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
        if len(data) > MAX_FRAME_BYTES:
            raise SnapshotError("CORRUPT_BACKUP", "frame larger than the format limit")
        chunk = struct.pack(">I", len(data)) + data
        self.f.write(chunk)
        self.hasher.update(chunk)
        self.length += len(chunk)
        self.record_count += 1

    def write_row(self, tenant, key, value):
        frame = {"kind": "row", "tenant": tenant, "key": str(key),
                 "value": common.value_to_wire(value)}
        if self.first_key is None:
            self.first_key = enc_key(tenant, key)
        self.last_key = enc_key(tenant, key)
        self.write_frame(frame)

    def write_receipt(self, seq, request_id, request_hash, receipt_json):
        self.write_frame({"kind": "receipt", "seq": seq,
                          "request_id": request_id,
                          "request_hash": request_hash,
                          "receipt_json": receipt_json})

    def close(self):
        self.f.flush()
        os.fsync(self.f.fileno())
        self.f.close()

    def descriptor(self, role):
        return {
            "name": self.name, "role": role, "length": self.length,
            "sha256": self.hasher.hexdigest(),
            "record_count": self.record_count,
            "first_key": self.first_key, "last_key": self.last_key,
        }


def iter_frames(path):
    """Yield parsed frames one at a time.  A truncated/cut segment raises."""
    with open(path, "rb") as f:
        while True:
            hdr = f.read(4)
            if not hdr:
                return
            if len(hdr) != 4:
                raise SnapshotError("CORRUPT_BACKUP",
                                    f"truncated frame header in {os.path.basename(path)}")
            (n,) = struct.unpack(">I", hdr)
            if n > MAX_FRAME_BYTES:
                raise SnapshotError("CORRUPT_BACKUP",
                                    f"frame length {n} exceeds the format limit"
                                    f" in {os.path.basename(path)}")
            payload = f.read(n)
            if len(payload) != n:
                raise SnapshotError("CORRUPT_BACKUP",
                                    f"truncated frame body in {os.path.basename(path)}")
            try:
                yield json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise SnapshotError(
                    "CORRUPT_BACKUP",
                    f"undecodable frame in {os.path.basename(path)}: {e}") from e


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_manifest_atomic(staging_dir, manifest):
    """Manifest appears inside staging as one atomic rename, durably."""
    final = os.path.join(staging_dir, MANIFEST_NAME)
    tmp = os.path.join(staging_dir, MANIFEST_NAME + ".tmp")
    data = (json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False)
            + "\n").encode("utf-8")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, final)
    _fsync_dir(staging_dir)


# ------------------------------------------------------------- validation
def _require(cond, message, detail=None):
    if not cond:
        raise SnapshotError("CORRUPT_BACKUP", message, detail)


def _validate_manifest_shape(m):
    _require(isinstance(m, dict), "manifest must be a JSON object")
    if m.get("format") != SNAPSHOT_FORMAT:
        raise SnapshotError("CORRUPT_BACKUP",
                            f"not a {SNAPSHOT_FORMAT} backup: {m.get('format')!r}")
    ver = m.get("format_version")
    if ver != SNAPSHOT_VERSION:
        raise SnapshotError(
            "UNSUPPORTED_FORMAT",
            f"backup format version {ver!r} is supported by this build"
            f" (known version: {SNAPSHOT_VERSION})",
            {"format_version": ver, "supported_version": SNAPSHOT_VERSION})
    for k in ("watermark", "row_count", "receipt_count", "routing",
              "segments", "key_range"):
        _require(k in m, f"manifest missing {k!r}")
    _require(isinstance(m["watermark"], int) and m["watermark"] >= 0,
             "watermark must be a non-negative integer")
    _require(isinstance(m["row_count"], int) and m["row_count"] >= 0,
             "row_count must be a non-negative integer")
    _require(isinstance(m["receipt_count"], int)
             and m["receipt_count"] >= 0,
             "receipt_count must be a non-negative integer")
    _require(isinstance(m["routing"], list) and m["routing"],
             "routing view must be a non-empty list")
    _require(isinstance(m["segments"], list) and m["segments"],
             "segments must be a non-empty list")


def _validate_routing(routing):
    """The shard-independent routing view must partition the whole key
    domain: sorted half-open ranges, no holes, no overlaps, touching.  Each
    route carries exactly a label and lo/hi key bounds, so no source path or
    shard file name can hide in extra fields."""
    bounds = []
    for i, r in enumerate(routing):
        _require(isinstance(r, dict) and set(r) == {"shard", "lo", "hi"},
                 f"route {i} must carry exactly shard/lo/hi, got {r!r}")
        _require(isinstance(r["shard"], str) and r["shard"],
                 f"route {i} shard label must be a non-empty string")
        _require("/" not in r["shard"] and os.sep not in r["shard"],
                 f"route {i} shard label must be a plain name, not a path")
        lo, hi = dec_bound(r["lo"]), dec_bound(r["hi"])
        if lo is not None:
            common.check_tenant(lo[0])
            common.check_i64(lo[1], "route lo key")
        if hi is not None:
            common.check_tenant(hi[0])
            common.check_i64(hi[1], "route hi key")
        if lo is not None and hi is not None:
            _require(common.bound_ck(lo) < common.bound_ck(hi),
                     f"route {i} is empty or reversed: {r}")
        bounds.append((lo, hi))
    bounds.sort(key=lambda x: (0,) if x[0] is None
                else (1, common.bound_ck(x[0])))
    _require(bounds[0][0] is None, "first route must start at -infinity")
    _require(bounds[-1][1] is None, "last route must end at +infinity")
    for (lo1, hi1), (lo2, hi2) in zip(bounds, bounds[1:]):
        _require(hi1 is not None and hi1 == lo2,
                 "routing view must be adjacent without holes or overlaps")
    return bounds


def _validate_row_frame(frame, prev_order):
    _require(isinstance(frame, dict) and frame.get("kind") == "row",
              f"expected a row frame, got {frame.get('kind') if isinstance(frame, dict) else frame!r}")
    tenant = frame.get("tenant")
    common.check_tenant(tenant)
    raw_key = frame.get("key")
    _require(isinstance(raw_key, str), "row key must be a decimal string")
    try:
        key = int(raw_key)
    except ValueError:
        raise SnapshotError("CORRUPT_BACKUP", f"bad row key {raw_key!r}")
    common.check_i64(key, "row key")
    # also rejects non-canonical decimal strings such as "01"
    _require(str(key) == raw_key, f"non-canonical row key {raw_key!r}")
    value = common.value_from_wire(frame.get("value"))
    order = _order_key(tenant, key)
    _require(prev_order is None or order > prev_order,
             "row frames must be strictly ordered with unique primary keys")
    return tenant, key, value, order


def _validate_receipt_frame(frame, watermark, prev_seq, seen_ids):
    _require(isinstance(frame, dict) and frame.get("kind") == "receipt",
              f"expected a receipt frame, got {frame.get('kind') if isinstance(frame, dict) else frame!r}")
    seq = frame.get("seq")
    _require(isinstance(seq, int) and not isinstance(seq, bool)
             and 1 <= seq <= watermark,
             f"receipt seq {seq!r} outside 1..{watermark}")
    _require(prev_seq is None or seq > prev_seq,
             "receipt frames must be strictly ordered by seq")
    rid = frame.get("request_id")
    _require(isinstance(rid, str) and rid, "request_id must be a non-empty string")
    if seen_ids is not None:
        _require(rid not in seen_ids,
                 f"duplicate request_id in receipts: {rid!r}")
        seen_ids.add(rid)
    rh = frame.get("request_hash")
    _require(isinstance(rh, str) and len(rh) == 64
             and all(c in "0123456789abcdef" for c in rh),
             "request_hash must be a 64-char lowercase hex SHA-256")
    rj = frame.get("receipt_json")
    _require(isinstance(rj, str), "receipt_json must be a JSON string")
    try:
        receipt = json.loads(rj)
    except json.JSONDecodeError:
        raise SnapshotError("CORRUPT_BACKUP", "receipt_json is not valid JSON")
    _require(isinstance(receipt, dict)
             and receipt.get("commit_seq") == seq
             and receipt.get("request_id") == rid,
             "stored receipt must match its commit seq and request_id")
    return seq


def verify_backup(backup_path):
    """Full offline verification, streamed.

    Checks the manifest root, format version, every segment's exact length
    and SHA-256 (recomputed from the bytes on disk), frame shapes, the global
    row ordering and primary-key uniqueness, the receipt ordering/uniqueness
    and the shard-independent routing coverage.  Returns
    ``(manifest, report)``; raises :class:`SnapshotError` on any defect.
    """
    if not os.path.exists(backup_path):
        raise SnapshotError("BACKUP_NOT_FOUND",
                            f"no backup at {backup_path!r}")
    if not os.path.isdir(backup_path):
        raise SnapshotError("BACKUP_INCOMPLETE",
                            f"backup path is not a directory: {backup_path!r}")
    manifest_path = os.path.join(backup_path, MANIFEST_NAME)
    if not os.path.isfile(manifest_path):
        raise SnapshotError("BACKUP_INCOMPLETE",
                            f"backup {backup_path!r} has no {MANIFEST_NAME};"
                            f" interrupted temporary data is not a backup")
    try:
        with open(manifest_path, "rb") as f:
            manifest = json.loads(f.read().decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise SnapshotError("CORRUPT_BACKUP",
                            f"manifest is not readable JSON: {e}") from e
    _validate_manifest_shape(manifest)
    watermark = manifest["watermark"]
    routes = _validate_routing(manifest["routing"])

    segments = manifest["segments"]
    names = set()
    roles_seen = []
    for i, s in enumerate(segments):
        _require(isinstance(s, dict), f"segment descriptor {i} must be an object")
        name = s.get("name")
        _require(isinstance(name, str) and name and "/" not in name
                 and os.sep not in name and name not in (".", "..")
                 and not name.startswith("."),
                 f"segment name {name!r} is not a safe plain file name")
        _require(name not in names, f"duplicate segment name {name!r}")
        names.add(name)
        _require(s.get("role") in ("rows", "receipts"),
                 f"segment {name} has unknown role {s.get('role')!r}")
        roles_seen.append(s["role"])
        _require(isinstance(s.get("length"), int) and s["length"] >= 0,
                 f"segment {name} length must be a non-negative integer")
        _require(isinstance(s.get("sha256"), str) and len(s["sha256"]) == 64
                 and all(c in "0123456789abcdef" for c in s["sha256"]),
                 f"segment {name} sha256 must be 64 lowercase hex chars")
        _require(isinstance(s.get("record_count"), int)
                 and s["record_count"] >= 0,
                 f"segment {name} record_count must be a non-negative integer")
        if s["role"] == "rows":
            # row segments describe their shard-independent key range
            if s["record_count"]:
                _require(s.get("first_key") is not None
                         and s.get("last_key") is not None,
                         f"non-empty row segment {name} must declare its key range")
            else:
                _require(s.get("first_key") is None
                         and s.get("last_key") is None,
                         f"empty row segment {name} must not declare a key range")
        else:
            # receipts are seq-addressed, not key-addressed
            _require(s.get("first_key") is None and s.get("last_key") is None,
                     f"receipt segment {name} must not declare a row key range")
    # rows first, receipts after, at least one segment of each role
    _require("rows" in roles_seen and "receipts" in roles_seen,
             "backup must contain row and receipt segments")
    joined = "".join("r" if x == "rows" else "c" for x in roles_seen)
    _require(joined == "r" * joined.count("r") + "c" * joined.count("c"),
             "receipt segments must follow all row segments")

    total_rows = 0
    total_receipts = 0
    prev_order = None
    global_first = None
    global_last = None
    prev_seq = None

    for s in segments:
        seg_path = os.path.join(backup_path, s["name"])
        if not os.path.isfile(seg_path):
            raise SnapshotError("CORRUPT_BACKUP",
                                f"declared segment missing: {s['name']}",
                                {"segment": s["name"]})
        hasher = hashlib.sha256()
        actual_len = 0
        count = 0
        first_key = None
        last_key = None
        for frame in iter_frames(seg_path):
            # length/hash are over the exact bytes on disk; iter_frames
            # re-reads the framing itself
            if s["role"] == "rows":
                tenant, key, _value, order = _validate_row_frame(frame, prev_order)
                prev_order = order
                if first_key is None:
                    first_key = enc_key(tenant, key)
                last_key = enc_key(tenant, key)
                if global_first is None:
                    global_first = enc_key(tenant, key)
                global_last = enc_key(tenant, key)
            else:
                # streaming check: ordering and seq range only; request_id
                # uniqueness is enforced by the restored table's UNIQUE
                # constraint during the build, so no full-id set is held
                prev_seq = _validate_receipt_frame(
                    frame, watermark, prev_seq, None)
            count += 1
        with open(seg_path, "rb") as f:
            while True:
                chunk = f.read(1 << 20)
                if not chunk:
                    break
                hasher.update(chunk)
                actual_len += len(chunk)
        if actual_len != s["length"]:
            raise SnapshotError(
                "CORRUPT_BACKUP",
                f"segment {s['name']} length {actual_len} != manifest {s['length']}",
                {"segment": s["name"], "actual_length": actual_len,
                 "manifest_length": s["length"]})
        if hasher.hexdigest() != s["sha256"]:
            raise SnapshotError(
                "CORRUPT_BACKUP",
                f"segment {s['name']} SHA-256 mismatch: the bytes were"
                f" truncated or tampered with",
                {"segment": s["name"],
                 "actual_sha256": hasher.hexdigest(),
                 "manifest_sha256": s["sha256"]})
        if count != s["record_count"]:
            raise SnapshotError(
                "CORRUPT_BACKUP",
                f"segment {s['name']} record count {count} != manifest"
                f" {s['record_count']}",
                {"segment": s["name"]})
        if s["role"] == "rows" and (
                first_key != s.get("first_key")
                or last_key != s.get("last_key")):
            raise SnapshotError(
                "CORRUPT_BACKUP",
                f"segment {s['name']} declared key range does not match its"
                f" records",
                {"segment": s["name"]})
        if s["role"] == "rows":
            total_rows += count
        else:
            total_receipts += count

    if total_rows != manifest["row_count"]:
        raise SnapshotError("CORRUPT_BACKUP",
                            f"row count {total_rows} != manifest {manifest['row_count']}")
    if total_receipts != manifest["receipt_count"]:
        raise SnapshotError("CORRUPT_BACKUP",
                            f"receipt count {total_receipts} != manifest"
                            f" {manifest['receipt_count']}")
    if manifest.get("key_range") != {"first": global_first, "last": global_last}:
        raise SnapshotError(
            "CORRUPT_BACKUP",
            f"manifest key range {manifest.get('key_range')} does not match"
            f" the records {global_first}..{global_last}")
    # every row key must fall inside exactly one route of the routing view.
    # routes are sorted and adjacent, so the bisect mapping used at restore
    # time must place every key on a route whose half-open range contains it.
    if global_first is not None:
        lo_order = _order_key(*dec_key(global_first))
        hi_order = _order_key(*dec_key(global_last))
        first_lo, first_hi = routes[0]
        last_lo, last_hi = routes[-1]
        _require(first_lo is None or lo_order >= common.bound_ck(first_lo),
                 "first row key is below the routing view")
        _require(last_hi is None or hi_order < common.bound_ck(last_hi),
                 "last row key is above the routing view")
        # re-walk the row frames and assert per-key coverage; this stays
        # streamed (one frame at a time) and catches a tampered routing view
        order_cursor = None
        for s in segments:
            if s["role"] != "rows":
                continue
            for frame in iter_frames(os.path.join(backup_path, s["name"])):
                tenant, key, _v, order_cursor = _validate_row_frame(
                    frame, order_cursor)
                idx = _route_for(routes, order_cursor)
                lo, hi = routes[idx]
                _require(
                    (lo is None or order_cursor >= common.bound_ck(lo))
                    and (hi is None or order_cursor < common.bound_ck(hi)),
                    f"row {tenant!r}/{key} is not covered by the routing view")

    report = {
        "verified": True,
        "backup_path": os.path.abspath(backup_path),
        "format": SNAPSHOT_FORMAT,
        "format_version": SNAPSHOT_VERSION,
        "watermark": watermark,
        "epoch_at_export": manifest.get("epoch_at_export"),
        "row_count": total_rows,
        "receipt_count": total_receipts,
        "key_range": manifest["key_range"],
        "route_count": len(routes),
        "routing": manifest["routing"],
        "segments": [{k: s[k] for k in
                      ("name", "role", "length", "sha256", "record_count",
                       "first_key", "last_key")} for s in segments],
        # the recovered instance's change stream starts strictly after this;
        # source subscribers and unacked positions are not portable
        "subscription_resume_after_seq": watermark,
        "retained_log_low": watermark + 1,
        "created_at": manifest.get("created_at"),
    }
    return manifest, report


# ------------------------------------------------------------- restore
def _check_absolute(path, what):
    if not isinstance(path, str) or not path:
        raise SnapshotError("BAD_REQUEST", f"{what} must be a non-empty path")
    if not os.path.isabs(path):
        raise SnapshotError("BAD_REQUEST",
                            f"{what} must be an absolute path, got {path!r}")


def _route_for(routes, order):
    """Map a key's comparable form to its route index (routes are sorted)."""
    import bisect
    lows = [None if lo is None else common.bound_ck(lo) for lo, _ in routes]
    # largest index whose low bound is <= order; None (-inf) sorts first
    keys = [(0,) if lo is None else (1, lo) for lo in lows]
    idx = bisect.bisect_right(keys, (1, order)) - 1
    if idx < 0:
        raise SnapshotError("CORRUPT_BACKUP", f"no route covers key {order}")
    lo, hi = routes[idx]
    if lo is not None and order < common.bound_ck(lo):
        raise SnapshotError("CORRUPT_BACKUP", f"key {order} below route {idx}")
    if hi is not None and order >= common.bound_ck(hi):
        raise SnapshotError("CORRUPT_BACKUP", f"key {order} above route {idx}")
    return idx


def restore_backup(backup_path, target_dir, crash_cb=None, batch_size=RESTORE_BATCH):
    """Verify a backup and build a fresh, bootable store in an *empty* target.

    All validation (manifest, format, lengths, hashes, ordering, primary-key
    uniqueness) completes before any target shard is built; the built data
    directory is published with a single atomic rename, so a failure or a
    killed process never leaves a usable half-store behind and never
    overwrites existing data.
    """
    _check_absolute(backup_path, "backup path")
    _check_absolute(target_dir, "target directory")
    if crash_cb is None:
        def crash_cb(_point):
            return None

    # 1) full verification first; nothing is written yet
    manifest, report = verify_backup(backup_path)
    watermark = manifest["watermark"]
    routes = _validate_routing(manifest["routing"])
    crash_cb("during_restore_verify")

    # 2) the target must be missing or an empty directory; decide the
    #    publish strategy before creating anything
    if os.path.lexists(target_dir):
        if not os.path.isdir(target_dir) or os.listdir(target_dir):
            raise SnapshotError(
                "TARGET_NOT_EMPTY",
                f"restore target {target_dir!r} exists and is not an empty"
                f" directory; existing data is never overwritten")
    parent = os.path.dirname(os.path.abspath(target_dir))
    if not os.path.isdir(parent):
        raise SnapshotError("BAD_REQUEST",
                            f"parent directory of the target does not exist: {parent!r}")
    staging = os.path.join(parent, ".restore-" + secrets.token_hex(12))
    if os.path.exists(staging):  # astronomically unlikely, refuse anyway
        raise SnapshotError("RESTORE_FAILED", "staging path collision; retry")
    os.mkdir(staging)
    stage_shards = os.path.join(staging, "shards")
    os.mkdir(stage_shards)

    # imported lazily: the server module owns the on-disk schema and the
    # restored directory must open through its normal recovery path
    from shardstore import server as server_mod

    control_path = os.path.join(staging, "control.sqlite")
    published = False
    try:
        # 3) build control metadata: one fresh epoch, fresh shard ids, no
        #    source subscribers, tokens or split state
        shard_ids = [f"s{i + 1}" for i in range(len(routes))]
        cc = sqlite3_connect(control_path)
        try:
            cc.executescript(server_mod.SCHEMA_CONTROL)
            cc.execute("INSERT INTO meta(k,v) VALUES ('visible_seq',?)",
                       (str(watermark),))
            cc.execute("INSERT INTO meta(k,v) VALUES ('epoch','1')")
            cc.execute("INSERT INTO meta(k,v) VALUES ('shard_counter',?)",
                       (str(len(shard_ids)),))
            for sid in shard_ids:
                cc.execute("INSERT INTO shards(shard_id,state) VALUES (?,'active')",
                           (sid,))
            for sid, (lo, hi) in zip(shard_ids, routes):
                cc.execute(
                    "INSERT INTO epochs(epoch,shard_id,lo_t,lo_k,hi_t,hi_k)"
                    " VALUES (1,?,?,?,?,?)",
                    (sid, lo[0] if lo else None, lo[1] if lo else None,
                     hi[0] if hi else None, hi[1] if hi else None))
            cc.commit()
        finally:
            cc.close()

        # 4) create the shard files and stream rows into them in key order
        conns = {}
        pending = {sid: [] for sid in shard_ids}
        try:
            for sid in shard_ids:
                p = os.path.join(stage_shards, sid + ".db")
                c = sqlite3_connect(p)
                c.executescript(server_mod.SCHEMA_SHARD)
                c.commit()
                conns[sid] = c
            rows_seen = 0
            prev_order = None

            def flush(sid):
                rows = pending[sid]
                if not rows:
                    return
                conns[sid].executemany(
                    "INSERT OR REPLACE INTO kv(tenant,k,seq,op,vt,vi,vb)"
                    " VALUES (?,?,?,'put',?,?,?)",
                    [(t, k, watermark, v[0], v[1], v[2]) for (t, k, v) in rows])
                conns[sid].commit()
                pending[sid] = []

            for s in manifest["segments"]:
                if s["role"] != "rows":
                    continue
                for frame in iter_frames(os.path.join(backup_path, s["name"])):
                    tenant, key, value, order = _validate_row_frame(
                        frame, prev_order)
                    prev_order = order
                    idx = _route_for(routes, order)
                    sid = shard_ids[idx]
                    pending[sid].append((tenant, key, value))
                    rows_seen += 1
                    if len(pending[sid]) >= batch_size:
                        flush(sid)
                    if rows_seen % batch_size == 0:
                        crash_cb("during_restore_build")
            for sid in shard_ids:
                flush(sid)
            for sid, c in conns.items():
                c.execute(
                    "INSERT INTO frontier(id,seq) VALUES (1,?) "
                    "ON CONFLICT(id) DO UPDATE SET seq=MAX(seq, excluded.seq)",
                    (watermark,))
                c.commit()
        finally:
            for c in conns.values():
                c.close()

        # 5) stream receipts into the commit table; payloads stay released
        #    (payload_live=0, deliver_json NULL): only dedup evidence is
        #    restored, never historical change payloads
        cc = sqlite3_connect(control_path)
        batch = []
        try:
            for s in manifest["segments"]:
                if s["role"] != "receipts":
                    continue
                prev = None
                for frame in iter_frames(os.path.join(backup_path, s["name"])):
                    prev = _validate_receipt_frame(frame, watermark, prev, None)
                    batch.append((frame["seq"], frame["request_id"],
                                  frame["request_hash"], frame["receipt_json"]))
                    if len(batch) >= batch_size:
                        _insert_receipts(cc, batch)
                        batch = []
                        crash_cb("during_restore_build")
            if batch:
                _insert_receipts(cc, batch)
            cc.commit()
        finally:
            cc.close()

        # 6) cross-check the built store against the backup, still streaming
        _cross_check(backup_path, manifest, routes, shard_ids, stage_shards,
                     watermark, batch_size)
        _checkpoint_and_truncate(control_path, stage_shards)

        crash_cb("before_restore_publish")
        # 7) one atomic publish; replacing an empty directory is atomic.
        #    Once this returns the target is a complete, usable store, so a
        #    failure afterwards must never trigger staging cleanup on it.
        os.replace(staging, target_dir)
        published = True
        _fsync_dir(parent)
        crash_cb("after_restore_publish")
    except BaseException:
        # any failure before the publish must leave no half-store behind and
        # the target (if it existed, empty) untouched.  After a successful
        # publish the staging path no longer exists and the target is final.
        if not published:
            import shutil
            shutil.rmtree(staging, ignore_errors=True)
        raise

    report.update({
        "data_dir": os.path.abspath(target_dir),
        "shards": [{"shard_id": sid, "lo": enc_bound(lo), "hi": enc_bound(hi)}
                   for sid, (lo, hi) in zip(shard_ids, routes)],
        "rows_built": manifest["row_count"],
        "receipts_built": manifest["receipt_count"],
        "next_commit_seq": watermark + 1,
        "subscription_resume_after_seq": watermark,
        "retained_log_low": watermark + 1,
    })
    return report


def sqlite3_connect(path):
    import sqlite3
    c = sqlite3.connect(path, timeout=60)
    c.execute("PRAGMA synchronous=FULL")
    c.execute("PRAGMA busy_timeout=60000")
    return c


def _insert_receipts(cc, batch):
    import sqlite3
    try:
        cc.executemany(
            "INSERT INTO commits(seq,request_id,request_hash,receipt_json,"
            " apply_json,bump_json,applied,deliver_json,payload_live)"
            " VALUES (?,?,?,?,'{}','[]',1,NULL,0)",
            batch)
        cc.commit()
    except sqlite3.IntegrityError as e:
        # UNIQUE(seq) / UNIQUE(request_id): a backup claiming distinct
        # receipts cannot be restored as a usable dedup table
        raise SnapshotError(
            "CORRUPT_BACKUP",
            f"duplicate commit seq or request_id while restoring receipts: {e}",
            {"first_request_id": batch[0][1] if batch else None}) from e


def _checkpoint_and_truncate(control_path, stage_shards):
    """Fold WAL files back so the published directory is self-contained."""
    import sqlite3
    paths = [control_path]
    for name in sorted(os.listdir(stage_shards)):
        if name.endswith(".db"):
            paths.append(os.path.join(stage_shards, name))
    for path in paths:
        c = sqlite3.connect(path)
        try:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            c.commit()
        finally:
            c.close()


def _cross_check(backup_path, manifest, routes, shard_ids, stage_shards,
                 watermark, batch_size):
    """Read the built shards back and compare them with every backup frame."""
    conns = {sid: sqlite3_connect(os.path.join(stage_shards, sid + ".db"))
             for sid in shard_ids}
    try:
        pending = {sid: [] for sid in shard_ids}

        def check_batch(sid):
            rows = pending[sid]
            if not rows:
                return
            keys = [(t, k) for (t, k, _v) in rows]
            qmarks = ",".join("(?,?)" for _ in keys)
            got = conns[sid].execute(
                f"SELECT tenant,k,op,vt,vi,vb FROM kv WHERE (tenant,k) IN"
                f" ({qmarks})", [x for pair in keys for x in pair]).fetchall()
            indexed = {(r[0], r[1]): r for r in got}
            for (t, k, v) in rows:
                r = indexed.get((t, k))
                _require(r is not None, f"restored row missing: {t!r}/{k}")
                blob = r[5]
                blob = bytes(blob) if blob is not None else None
                _require(r[2] == "put" and (r[3], r[4], blob) == v,
                         f"restored row value mismatch at {t!r}/{k}")
            pending[sid] = []

        total = 0
        for s in manifest["segments"]:
            if s["role"] != "rows":
                continue
            prev = None
            for frame in iter_frames(os.path.join(backup_path, s["name"])):
                tenant, key, value, order = _validate_row_frame(frame, prev)
                prev = order
                sid = shard_ids[_route_for(routes, order)]
                pending[sid].append((tenant, key, value))
                total += 1
                if len(pending[sid]) >= batch_size:
                    check_batch(sid)
        for sid in shard_ids:
            check_batch(sid)
        _require(total == manifest["row_count"],
                 "cross-check row count disagrees with the manifest")
        for sid, c in conns.items():
            (n,) = c.execute("SELECT COUNT(*) FROM kv").fetchone()
            (f,) = c.execute("SELECT seq FROM frontier WHERE id=1").fetchone()
            _require(f == watermark, f"shard {sid} frontier {f} != {watermark}")
        # primary-key uniqueness is enforced by the kv PRIMARY KEY, but make
        # the guarantee explicit on the built bytes too
        for sid, c in conns.items():
            (n,) = c.execute(
                "SELECT COUNT(*) FROM (SELECT tenant,k FROM kv GROUP BY tenant,k)") \
                .fetchone()
            (total_n,) = c.execute("SELECT COUNT(*) FROM kv").fetchone()
            _require(n == total_n, f"duplicate primary key inside shard {sid}")
    finally:
        for c in conns.values():
            c.close()

    cc = sqlite3_connect(os.path.join(os.path.dirname(stage_shards),
                                      "control.sqlite"))
    try:
        (n,) = cc.execute("SELECT COUNT(*) FROM commits").fetchone()
        _require(n == manifest["receipt_count"],
                 f"restored receipt count {n} != {manifest['receipt_count']}")
        (v,) = cc.execute("SELECT v FROM meta WHERE k='visible_seq'").fetchone()
        _require(int(v) == watermark, "restored visible_seq disagrees")
        live = cc.execute(
            "SELECT COUNT(*) FROM commits WHERE payload_live=1").fetchone()[0]
        _require(live == 0, "restored store must not expose historical payloads")
        _require(cc.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0] == 0,
                 "restored store must not carry source subscribers")
        _require(cc.execute("SELECT COUNT(*) FROM tokens").fetchone()[0] == 0,
                 "restored store must not carry source read tokens")
        _require(cc.execute("SELECT COUNT(*) FROM splits").fetchone()[0] == 0,
                 "restored store must not carry source split state")
    finally:
        cc.close()


# ------------------------------------------------------------- CLI
def main(argv=None):
    ap = argparse.ArgumentParser(
        description="offline shardstore snapshot inspection and restore")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_i = sub.add_parser("inspect", help="verify a backup and print its manifest")
    p_i.add_argument("backup")
    p_r = sub.add_parser("restore", help="restore a backup into an empty directory")
    p_r.add_argument("backup")
    p_r.add_argument("target")
    p_r.add_argument("--batch-size", type=int, default=RESTORE_BATCH)
    args = ap.parse_args(argv)
    try:
        if args.cmd == "inspect":
            _manifest, report = verify_backup(args.backup)
        else:
            report = restore_backup(args.backup, args.target,
                                    batch_size=args.batch_size)
    except SnapshotError as e:
        print(json.dumps({"ok": False, "error": {"code": e.code,
                                                 "message": e.message,
                                                 "detail": e.detail}},
                         ensure_ascii=False, indent=2))
        return 2
    print(json.dumps({"ok": True, "result": report}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
