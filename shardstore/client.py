"""Client library for shardstore.  Safe to use from multiple processes."""
from __future__ import annotations

import socket

from shardstore import common


class StoreError(Exception):
    def __init__(self, code, message, detail=None):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.detail = detail or {}


class Row:
    """A stored row: primary key (tenant, key) plus the three nullable
    columns (text, integer, blob)."""
    __slots__ = ("tenant", "key", "text", "integer", "blob")

    def __init__(self, tenant, key, text, integer, blob):
        self.tenant = tenant
        self.key = key
        self.text = text
        self.integer = integer
        self.blob = blob

    def value(self):
        return (self.text, self.integer, self.blob)

    def __eq__(self, other):
        return (isinstance(other, Row)
                and (self.tenant, self.key, self.text, self.integer, self.blob)
                == (other.tenant, other.key, other.text, other.integer, other.blob))

    def __repr__(self):
        return (f"Row({self.tenant!r}, {self.key!r}, text={self.text!r}, "
                f"integer={self.integer!r}, blob={self.blob!r})")


class ScanPage:
    __slots__ = ("rows", "next_cursor", "done")

    def __init__(self, rows, next_cursor, done):
        self.rows = rows
        self.next_cursor = next_cursor
        self.done = done


class Change:
    """One committed write, delivered whole.

    `commit_seq` is the global commit sequence; `ops` is the original caller
    request in caller order (put/delete including deletes of missing keys),
    already decoded to the same tuples `write()` accepts.  `watermark` is the
    stable logical position of the request in the stream and `size_bytes` is
    the canonical payload size.  A commit is never split across deliveries:
    a single commit above the byte budget is still delivered in one piece and
    flagged with `over_budget=True`."""
    __slots__ = ("commit_seq", "request_id", "epoch", "watermark", "ops",
                 "size_bytes", "over_budget")

    def __init__(self, commit_seq, request_id, epoch, watermark, ops,
                 size_bytes, over_budget):
        self.commit_seq = commit_seq
        self.request_id = request_id
        self.epoch = epoch
        self.watermark = watermark
        self.ops = ops
        self.size_bytes = size_bytes
        self.over_budget = over_budget

    def __eq__(self, other):
        return isinstance(other, Change) and (
            self.commit_seq, self.request_id, self.epoch, self.watermark,
            self.ops, self.size_bytes) == (
            other.commit_seq, other.request_id, other.epoch, other.watermark,
            other.ops, other.size_bytes)

    def __repr__(self):
        return (f"Change(seq={self.commit_seq}, request_id={self.request_id!r},"
                f" ops={self.ops!r})")


class PullPage:
    """The result of a pull/replay.  Iterate it directly for the commits."""
    __slots__ = ("changes", "state", "next_after", "has_more", "bytes",
                 "over_budget", "oversize_seq")

    def __init__(self, r):
        self.changes = [Change(c["commit_seq"], c["request_id"], c["epoch"],
                               c.get("watermark"),
                               [common.op_from_wire(o) for o in c["ops"]],
                               c["size_bytes"], c["over_budget"])
                        for c in r.get("items", [])]
        self.state = r
        self.next_after = r["next_after"]
        self.has_more = r["has_more"]
        self.bytes = r.get("bytes", 0)
        self.over_budget = r.get("over_budget", False)
        self.oversize_seq = r.get("oversize_seq")

    def __iter__(self):
        return iter(self.changes)

    def __len__(self):
        return len(self.changes)

    def __repr__(self):
        return (f"PullPage({len(self.changes)} commits, next_after="
                f"{self.next_after}, has_more={self.has_more})")


def put(tenant, key, text=None, integer=None, blob=None):
    """A put op: sets all three columns (any may be None = SQL NULL)."""
    return ("put", tenant, key, (text, integer, blob))


def delete(tenant, key):
    """A delete op; deleting a missing row is a no-op."""
    return ("del", tenant, key)


class ReadToken:
    """A pinned read point: one committed seq plus one routing epoch.
    Must be released explicitly when done."""
    __slots__ = ("_store", "token_id", "read_seq", "epoch")

    def __init__(self, store, token_id, read_seq, epoch):
        self._store = store
        self.token_id = token_id
        self.read_seq = read_seq
        self.epoch = epoch

    def get(self, tenant, key):
        return self._store.get(self, tenant, key)

    def scan(self, lo=None, hi=None, cursor=None, limit=1000):
        return self._store.scan(self, lo=lo, hi=hi, cursor=cursor, limit=limit)

    def scan_all(self, lo=None, hi=None, page=1000):
        return self._store.scan_all(self, lo=lo, hi=hi, page=page)

    def release(self):
        return self._store.release(self)

    def __repr__(self):
        return f"ReadToken(seq={self.read_seq}, epoch={self.epoch})"


class Store:
    def __init__(self, sock_path, timeout=60.0):
        self.sock_path = sock_path
        self.timeout = timeout

    def _rpc(self, op, **args):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        try:
            s.connect(self.sock_path)
            common.send_msg(s, {"op": op, "args": args})
            resp = common.recv_msg(s)
        except (OSError, EOFError) as e:
            raise StoreError("CONN", f"connection failed: {e}") from e
        finally:
            s.close()
        if not resp.get("ok"):
            err = resp.get("error") or {}
            raise StoreError(err.get("code", "INTERNAL"), err.get("message", ""),
                             err.get("detail"))
        return resp.get("result")

    # ---------------------------------------------------------- writes
    def write(self, request_id, ops):
        """Commit a batch of put/delete ops in the given order.  Returns a
        receipt with the global commit_seq.  Retransmitting the same
        request_id with the same ops returns the original receipt."""
        wire_ops = [common.op_to_wire(o) for o in ops]
        return self._rpc("write", request_id=request_id, ops=wire_ops)

    # ---------------------------------------------------------- reads
    def open_read(self):
        r = self._rpc("open_read")
        return ReadToken(self, r["token"], r["read_seq"], r["epoch"])

    def get(self, token, tenant, key):
        r = self._rpc("get", token=token.token_id, tenant=tenant, key=str(key))
        if not r["found"]:
            return None
        t, i, b = common.value_from_wire(r["value"])
        return Row(tenant, key, t, i, b)

    def scan(self, token, lo=None, hi=None, cursor=None, limit=1000):
        """One page of a range scan over [lo, hi); pass the returned cursor
        back to get the next page.  Every page uses the token's read point."""
        def enc(b):
            return None if b is None else {"tenant": b[0], "key": str(b[1])}
        r = self._rpc("scan", token=token.token_id, lo=enc(lo), hi=enc(hi),
                      cursor=enc(cursor), limit=limit)
        rows = []
        for item in r["rows"]:
            t, i, b = common.value_from_wire(item["value"])
            rows.append(Row(item["tenant"], int(item["key"]), t, i, b))
        nc = r["next_cursor"]
        cursor_out = None if nc is None else (nc["tenant"], int(nc["key"]))
        return ScanPage(rows=rows, next_cursor=cursor_out, done=r["done"])

    def scan_all(self, token, lo=None, hi=None, page=1000):
        cursor = None
        while True:
            p = self.scan(token, lo=lo, hi=hi, cursor=cursor, limit=page)
            yield from p.rows
            if p.done:
                return
            cursor = p.next_cursor

    def release(self, token):
        return self._rpc("release", token=token.token_id)

    # ---------------------------------------------------------- subscriptions
    def subscribe(self, name, after_seq):
        """Register a named subscription that starts after committed seq
        `after_seq` (strictly: the first delivered commit is after_seq + 1).

        A new name gets generation 1.  Re-registering the same name with the
        same after_seq keeps the cursor and confirmed prefix but takes a new
        consumption generation (older generations are rejected).  A different
        after_seq raises SUBSCRIPTION_EXISTS -- progress is never silently
        reset.  The returned `generation` is what pull/ack need."""
        return self._rpc("subscribe", name=name, after_seq=int(after_seq))

    def claim(self, name):
        """Take over consumption of `name`, invalidating every older
        generation: their delayed acks/pulls are rejected.  The cursor and
        confirmed prefix are kept."""
        return self._rpc("claim", name=name)

    def pull(self, name, generation, max_commits=100, max_bytes=1 << 20):
        """Fetch up to max_commits commits / max_bytes of changes, in strictly
        increasing commit_seq order, starting after the confirmed prefix.
        A single commit above max_bytes is delivered whole and flagged."""
        return PullPage(self._rpc("pull", name=name, generation=generation,
                                  max_commits=int(max_commits),
                                  max_bytes=int(max_bytes)))

    def ack(self, name, generation, through_seq):
        """Confirm that the contiguous prefix through `through_seq` has been
        fully processed.  Retries are idempotent; backwards or non-contiguous
        acknowledgements are refused without moving the cursor."""
        return self._rpc("ack", name=name, generation=generation,
                         through_seq=int(through_seq))

    def pause(self, name, generation):
        return self._rpc("set_paused", name=name, paused=True,
                         generation=generation)

    def resume(self, name, generation):
        return self._rpc("set_paused", name=name, paused=False,
                         generation=generation)

    def replay(self, name, generation, after_seq, max_commits=100,
               max_bytes=1 << 20):
        """Re-deliver already delivered commits from a retained position.
        Never moves the cursor; refuses positions before the retained range
        (with the usable range) or beyond what was delivered."""
        return PullPage(self._rpc("replay", name=name, generation=generation,
                                  after_seq=int(after_seq),
                                  max_commits=int(max_commits),
                                  max_bytes=int(max_bytes)))

    def subscribe_status(self, name):
        return self._rpc("subscription_status", name=name)

    def subscriptions(self):
        return self._rpc("subscriptions")

    def cleanup(self, up_to):
        """Release subscription change payloads through commit seq `up_to`.
        Refuses to pass an unconfirmed subscriber or crash-recovery need;
        request_id dedup receipts are kept, so retransmissions still return
        the original receipt."""
        return self._rpc("cleanup", up_to=int(up_to))

    # ---------------------------------------------------------- admin
    def split(self, shard_id, boundary):
        """Split shard_id at the interior composite boundary key."""
        return self._rpc("split", shard_id=shard_id,
                         boundary={"tenant": boundary[0],
                                   "key": str(boundary[1])})

    def migration_status(self):
        return self._rpc("migration_status")

    def routing(self):
        return self._rpc("routing")

    def reclaim(self):
        return self._rpc("reclaim")

    def shutdown(self):
        return self._rpc("shutdown")

    # ---------------------------------------------------------- test hooks
    def pause_copy(self, on):
        return self._rpc("test_pause_copy", on=bool(on))

    def arm_crash(self, point):
        return self._rpc("test_arm_crash", point=point)
