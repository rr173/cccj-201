"""Shared pieces for shardstore: wire framing, canonical encodings, key order.

The wire protocol is length-prefixed JSON over a unix socket.  JSON numbers
cannot represent every int64 exactly, so 64-bit integers travel as decimal
strings and BLOBs as base64.  NULL / exact int64 / BLOB types survive the
round trip untouched.

The total order over composite keys is (tenant by UTF-8 bytes, then key as
a signed 64-bit integer).  Keys are always compared as the pair; they are
never concatenated into a string.
"""
from __future__ import annotations

import base64
import hashlib
import json
import struct

INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
MAX_MSG = 512 * 1024 * 1024


class ProtoError(Exception):
    """Malformed request/response at the protocol level."""


def b64e(b) -> str:
    return base64.b64encode(bytes(b)).decode("ascii")


def b64d(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))


def check_tenant(t) -> None:
    if not isinstance(t, str):
        raise ProtoError("tenant must be a string")
    t.encode("utf-8")  # raises on lone surrogates


def check_i64(v, what="integer") -> None:
    if isinstance(v, bool) or not isinstance(v, int):
        raise ProtoError(f"{what} must be an int, got {type(v).__name__}")
    if v < INT64_MIN or v > INT64_MAX:
        raise ProtoError(f"{what} out of signed 64-bit range")


# ---------------------------------------------------------------- values
# A row value is a triple (text, integer, blob); every component may be
# None (SQL NULL).  Wire form: {"t": str|None, "i": str|None, "b": b64|None}.

def value_to_wire(value):
    text, integer, blob = value
    if text is not None and not isinstance(text, str):
        raise ProtoError("text component must be str or None")
    if integer is not None:
        check_i64(integer, "integer component")
    if blob is not None and not isinstance(blob, (bytes, bytearray, memoryview)):
        raise ProtoError("blob component must be bytes or None")
    return {
        "t": text,
        "i": None if integer is None else str(integer),
        "b": None if blob is None else b64e(blob),
    }


def value_from_wire(v):
    if not isinstance(v, dict):
        raise ProtoError("value must be an object")
    t = v.get("t")
    i = v.get("i")
    b = v.get("b")
    if t is not None and not isinstance(t, str):
        raise ProtoError("bad text component")
    if i is not None and not isinstance(i, str):
        raise ProtoError("bad integer component")
    if b is not None and not isinstance(b, str):
        raise ProtoError("bad blob component")
    return (t, None if i is None else int(i), None if b is None else b64d(b))


# ---------------------------------------------------------------- ops
# Internal op forms:
#   ("put", tenant, key, (text, integer, blob))
#   ("del", tenant, key)

def op_to_wire(op):
    kind = op[0]
    if kind == "put":
        _, tenant, key, value = op
        check_tenant(tenant)
        check_i64(key, "key")
        return {"op": "put", "tenant": tenant, "key": str(key),
                "value": value_to_wire(value)}
    if kind == "del":
        _, tenant, key = op
        check_tenant(tenant)
        check_i64(key, "key")
        return {"op": "del", "tenant": tenant, "key": str(key)}
    raise ProtoError(f"unknown op kind {kind!r}")


def op_from_wire(o):
    if not isinstance(o, dict):
        raise ProtoError("op must be an object")
    kind = o.get("op")
    tenant = o.get("tenant")
    key = o.get("key")
    check_tenant(tenant)
    if not isinstance(key, str):
        raise ProtoError("key must be a decimal string")
    key = int(key)
    check_i64(key, "key")
    if kind == "put":
        return ("put", tenant, key, value_from_wire(o.get("value")))
    if kind == "del":
        return ("del", tenant, key)
    raise ProtoError(f"unknown op {kind!r}")


def request_hash(wire_ops) -> str:
    """Canonical hash of the full request payload (for dedup)."""
    canon = json.dumps(wire_ops, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- framing

def send_msg(sock, obj) -> None:
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    if len(data) > MAX_MSG:
        raise ProtoError("message too large")
    sock.sendall(struct.pack(">I", len(data)) + data)


def _recv_exact(sock, n) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(n - len(buf), 1 << 20))
        if not chunk:
            raise EOFError("connection closed")
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock):
    hdr = _recv_exact(sock, 4)
    (n,) = struct.unpack(">I", hdr)
    if n > MAX_MSG:
        raise ProtoError("message too large")
    return json.loads(_recv_exact(sock, n).decode("utf-8"))


# ------------------------------------------------- composite key order

def ck(tenant: str, key: int):
    """Comparable form of a full key: (tenant utf-8 bytes, key)."""
    return (tenant.encode("utf-8"), key)


def bound_ck(bound):
    """bound: None | (tenant, key)  ->  None | comparable tuple."""
    if bound is None:
        return None
    return ck(bound[0], bound[1])
