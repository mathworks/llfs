# Multi-Page Transactions

One of the most important features of `llfs::Volume` and `llfs::PageCacheJob` is the ability to write multiple pages with ACID guarantees.  This document describes the LLFS transactional commit protocol and presents the rationale for its correctness.

## Transaction Phases (Simple, No Pipelining)

1. Prune: Trace reference counts from `PageCacheJob` "roots" to cull non-referenced new pages from the job
2. Prepare: Write a slot to the Volume WAL containing the PageIds for all new pages, wait for flush
3. Write Pages: Write all new pages in parallel to their respective `llfs::PageDevice`; wait for confirmation.  NOTE: at this point, the transaction can be considered durable (with caveat; see below), meaning it is guaranteed to be recovered if the rest of the protocol fails to execute
4. Update Ref Counts: Append a new slot to the WAL of the `PageAllocator` for each device (in parallel) that updates the reference counts of both new pages and currently existing pages; wait for flush from all `PageAllocator` `LogDevice`s.  NOTE: at this point, it is safe to execute this protocol for another transaction that depends on the changes in this one without sacrificing linearizability (strong sequential consistency) between the two.
5. Recycle Dead Pages: Based on information from the `PageAllocator` updates, queue all pages that are now garbage collectable (ref count == 1) in the `Volume`'s `PageRecycler` WAL
6. Drop Deleted Pages: Delete all pages in the job that have already been recycled/garbage-collected (i.e., pages with ref_count == 0) in parallel using their respective `PageDevice`
7. Commit: Write a final slot in the `Volume` WAL to confirm that the transaction is complete!
