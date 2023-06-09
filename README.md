# snapshot-isolation (WIP)
[![Go](https://github.com/SarthakMakhija/snapshot-isolation/actions/workflows/build.yml/badge.svg)](https://github.com/SarthakMakhija/snapshot-isolation/actions/workflows/build.yml)

The idea is to provide an implementation of **snapshot transaction isolation** and **serialized snapshot isolation** in a tiny MVCC based in-memory store.

The focus is only on the **I** part of the **ACID** transactions. The following ideas will be implemented:
- [X] MVCC based Skiplist with:
  - [X] Put
  - [X] Get
- [ ] Transaction implementation with snapshot isolation
- [ ] Transaction implementation with serialized snapshot isolation

# Snapshot isolation
To implement snapshot isolation, databases (and KV stores) maintain multiple versions of the data. Each
transaction may read different data based on the start timestamp of the transaction. To implement snapshot isolation, each
transaction is given a `begin timestamp` during the start and a `commit timestamp` during the commit. A transaction can
only read the data (or the keys) where the `commit timestamp of the data` < transaction's `begin timestamp`.

Snapshot isolation prevents **write-write** conflict. Two transaction can conflict on:
- **Spatial overlap**: both the transactions write to the same data (or the key)
- **Temporal overlap**: both the transactions overlap in time

Snapshot isolation prevents **dirty read**, **fuzzy read**, **phantom read** and **lost update** anomalies. 
However, it can result in **write skew**. 

*More details shall be covered in the blog.*

# Serialized snapshot isolation
To implement serialized snapshot isolation, databases (and KV stores) maintain multiple versions of the data. Each
transaction may read different data based on the start timestamp of the transaction. To implement serialized snapshot isolation, each
transaction is given a `begin timestamp` during the start and a `commit timestamp` during the commit. A transaction can
only read the data (or the keys) where the `commit timestamp of the data` < transaction's `begin timestamp`.

Serialized snapshot isolation prevents **read-write** conflict. Two transaction can conflict on:
- **RW-spatial overlap**: a transaction writes to the data that the other transaction reads
- **Temporal overlap**: both the transactions overlap in time

A transaction will have to abort if its read set is modified by other concurrent transaction.

Serialized snapshot isolation prevents **dirty read**, **fuzzy read**, **phantom read**, **lost update** and **write skew** anomalies.

*More details shall be covered in the blog.*