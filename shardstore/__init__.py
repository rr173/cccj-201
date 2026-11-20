"""shardstore: a local sharded MVCC row store with online resharding.

Data and control records live in SQLite files; the server speaks a small
length-prefixed JSON protocol over a unix socket so multiple processes can
share one store.
"""

__version__ = "0.2.0"
