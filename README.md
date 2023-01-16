# SimpleWAL

write-ahead logging (WAL) is a family of techniques for providing atomicity and durability (two of the ACID properties) in database systems.[1] A write ahead log is an append-only auxiliary disk-resident structure used for crash and transaction recovery. The changes are first recorded in the log, which must be written to stable storage, before the changes are written to the database.

The main functionality of a write-ahead log can be summarized as:

    Allow the page cache to buffer updates to disk-resident pages while ensuring durability semantics in the larger context of a database system.
    Persist all operations on disk until the cached copies of pages affected by these operations are synchronized on disk. Every operation that modifies the database state has to be logged on disk before the contents on the associated pages can be modified
    Allow lost in-memory changes to be reconstructed from the operation log in case of a crash.

SimpleWAL is a simple implementation of the WAL specifically for Key Value databases.
