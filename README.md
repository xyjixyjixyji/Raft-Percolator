# Distributed Systems in Rust

An implementation of Raft with upper layer service and Percolator distributed transaction.

The percolator API and KvRaft API are separated. If you want to combine them all together, it cannot be more simple to replace the percolator's BigTable manipulations to KvRaft's Clerk APIs. It is **not** done now for clarity.

## Tests
- Raft layer
`make test_2 2>/dev/null`

- KV layer
`make test_3 2>/dev/null`

- Percolator layer
`make test_percolator 2>/dev/null`
