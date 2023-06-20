# LLFS - Low Level File System

<!--[[_TOC_]]-->

LLFS is a library that provides low-level, portable building blocks for implementing external data structures on top of block- and log-based storage devices.  It aims to strike a middle ground between storage abstraction layers that are low-overhead but high effort, such as raw block devices, and feature-rich but lower efficiency options such as a full POSIX filesystem or relational (SQL) database.

LLFS offers the following features:

- Log-based storage (append-only):
  - In-memory
  - File-based (requires host filesystem)
  - io_uring-based (can run on flat files or directly on raw block devices)
- Block(Page)-based storage (random access):
  - In-memory
  - File-based (requires host filesystem)
  - io_uring-based (can run on flat files or directly on raw block devices)
- High-performance thread-safe page cache with transactional (atomic) updates
- Crash-safe, transactional block allocation and garbage collection (ref-counted)
- Zero-copy data serialization/parsing
- Bloom filters
- CRC-based data integrity checking
- Multi-device storage pool management

LLFS uses an **append-only** model for writing data.  Data is addressed by byte offset (in the case of `llfs::LogDevice`) and by `llfs::PageId` (in the case of `llfs::PageDevice`).  Once written, data can never be updated in place.  This means that a given offset within an `llfs::LogDevice` or a given `llfs::PageId` in a `llfs::PageDevice` will always refer to the same bytes.  Both `llfs::LogDevice` and `llfs::PageDevice` have a fixed capacity, and neither can grow forever; this means eventually the resources they consume must be reclaimed.  For `llfs::LogDevice`, this happens by trimming the start of the log to advance the minimum valid offset, erasing whatever data used to be stored before the trim point and reclaiming the storage resources that data consumed.  In the case of `llfs::PageDevice`, `llfs::PageId`s are reference-counted and released when their count drops to zero.  `llfs::PageId` embeds a write-generation counter so that re-using the same sector of a real storage device for multiple writes over time produces unique `llfs::PageId`s for each generation of data. 

**NOTE: LLFS is currently considered experimental and should only be used for prototyping, development, and non-critical applications.  When the project reaches stable status, the release major version will be incremented from 0 to 1.**

## llfs::Volume

The core abstraction for LLFS is `llfs::Volume`.  This class implements a bounded log of structured records which can contain references to pages of data.  The size of a `Volume` is fixed at creation time.  Once it fills up, a `Volume` must be `trim`-ed before more records can be appended.  Typically this is achieved via a checkpointing strategy that does some combination of refreshing older data later in the log, discarding obsolete records, and moving data out of the log into referenced pages.

`llfs::Volume` sits on top of `llfs::LogDevice`, `llfs::PageCache` , and `llfs::PageRecycler`. 

All updates to a volume are transactional.  Simple updates, i.e. ones not referencing external pages, can be achieved by appending a single record to the log via `Volume::append`.  More complex updates that involve external pages, which may contain cross-references amongst themselves, are achieved via the `llfs::PageCacheJob` mechanism.

A job represents a single atomic update to a volume.  An application creates a new job by calling `Volume::new_job`.  The job is tied to that `Volume`; LLFS does not currently provide support for multi-`Volume` transactions, though various protocols can be implemented to achieve this at the application layer.

## llfs::LogDevice

`llfs::LogDevice` is an abstract base class that represents an append-only, fixed-size storage area with a logical "active region" which moves monotonically through an unbounded range of byte offsets from the start of the log.  A good way to think of this is as a "sliding window:" the size limit (capacity) of a `LogDevice` is the maximum size (in bytes) of the active region of a "virtual log" whose maximum offset is unbounded.

The contents of a `LogDevice` are organized into sequential records, which can vary in size.  The log records are addressed by byte offset from the beginning of the log.  Thus not all log offsets actually point to the beginning of a valid record in a given log.  Applications must be careful to read records at valid offsets.  An offset interval that contains a single record within a log is referred to as a "slot" in LLFS.  There must be no logical gaps between subsequent records; any unused space at the end of one record is considered a part of that record.

The state of a `LogDevice` is captured by three variables: `trim_pos`, `commit_pos`, and `flush_pos`.  These are all scalar integers whose unit is byte offset from the beginning of the log (which is defined to be `0`).  They are mutually constrained by the following invariants:

* `trim_pos` &le; `flush_pos` &le; `commit_pos`
* `commit_pos` - `trim_pos` &le; _log-capacity_

When data is appended to a `LogDevice`, it is written to `commit_pos`, which is then atomically increased (via the `commit` method) to include the size of the appended data.  This data may include one or more records.  `LogDevice` implementations are responsible for guaranteeing that `commit` is atomic.  `flush_pos` represents the upper bound offset of data that has been durably written to the backing storage of the `LogDevice`.  Thus when `commit_pos` is advanced beyond `flush_pos`, the `LogDevice` must asynchronously flush the data between the two offsets from main memory to durable storage.  Once such writes have been confirmed, `LogDevice` will notify the application by updating `flush_pos`.  `LogDevice` includes `await_`-style methods for receiving notification of such updates so that application doesn't have to poll.

Note that different `LogDevice` implementations are allowed to offer different durability guarantees.  For example, it is perfectly fine to have a `MemoryLogDevice` that guarantees durability only for the duration of the process containing the device instance.  When `flush_pos` catches up with `commit_pos`, the `LogDevice` is signalling that its _maximum durability guarantee_ has been reached.  Currently there is no support in the `LogDevice` abstraction for multiple durability levels beyond the two implied by `commit_pos` (data is in memory) and `flush_pos` (maximum durability reached).

The distinction between these two levels of durability is reflected in the `LogDevice::Reader` interface.  A `Reader` object is bound to a specific durability level when it is created via `LogDevice::new_reader`.  The different levels are captured by `enum struct llfs::LogReadMode` (ordered by weakest to strongest):

- `kInconsistent` - the data observed by readers may or may not have been committed yet
- `kSpeculative` - the data observed by readers is guaranteed to be committed, but not necessarily flushed
- `kDurable` - the data observed by readers is guaranteed to be flushed, which implies it is also committed

`LogDevice` implementations are required to be thread-safe (the methods of a `LogDevice` object must be safe to access concurrently).  The instances of `llfs::LogDevice::Reader` and `llfs::LogDevice::Writer` returned by a `LogDevice` object are not required to be thread-safe, but are required to be thread-compatible (distinct objects are safe to access concurrently).

Space can be freed up in a `LogDevice` using the `trim` method, which updates the `trim_pos` pointer.  This is instantaneous to the application; no I/O is needed, and the reclaimed space may be used immediately by the application after the log is trimmed.  However, applications must be careful not to trim the log while there are any references to data in between the old and new values of `trim_pos`.  To make this easier to manage, LLFS provides `llfs::SlotReadLock`.  

## llfs::PageCache

The `llfs::PageCache` implements read- and write-through caching of pages from multiple `llfs::PageDevice` instances, each of which has an associated `llfs::PageAllocator` and `llfs::PageRecycler` to allocate physical pages as needed and release pages when their reference count goes to zero.  The liveness and garbage collection model for LLFS is per-page reference counting (with _no_ cycle detection), so applications using LLFS must take care to implement non-cyclic data structures.  

Each page is uniquely identified by a `llfs::PageId` value.  A `PageId` encodes (bit-wise) a device index (which is relative to the context of a `PageCache`), a physical page index (which is used by the various `PageDevice` implementations to read/write data), and a generation number.  The generation number means that each time a specific set of bytes is written to a physical location on a storage device, it is associated with a unique `PageId`.  This makes cache coherence for pages indexed by `PageId` much easier, as it is impossible for the "current" data bound to a given `PageId` to ever change.

### llfs::PageCacheJob

As mentioned in the section about `llfs::Volume`, `llfs::PageCacheJob` is the basic transaction mechanism provided by LLFS.  PageCache jobs provide all of the ACID properties:

- Atomic: all of the side-effects of a job are observed (outside of the job), or none of them
- Consistent: once a job has been committed, all observers will see its side effects
- Isolated: new pages can be read within a job, but not outside it; jobs can be chained together speculatively to create a pipeline of transactions that can see the side-effects of previous jobs in the chain, even if those jobs aren't full committed yet (and therefore aren't visible externally)
- Durable: once committed, all the data updates in the job are guaranteed to be written at the maximum durability level of the underyling log and page devices

There is one caveat: currently `PageCacheJob`s do not support full read isolation.  This means that changes made external to a job after its creation are visible inside the job.  However, because of the append-only data model of LLFS, this is not a very serious restriction.  This is because log records and data pages don't ever change once they are written; rather, new log records and pages become visible and old ones are trimmed/dropped over time.  The record at a certain log offset is immutable, as is the data bound to a given `PageId`.  Therefore, if an application wishes to implement full read isolation within a `PageCacheJob`, it can easily do so by simply not reading past a certain log offset.

`PageCacheJob` supports pipelining by chaining jobs together.  When a job sets another job as it's "base job," it gains access to all the new pages created by the base job even before they are fully flushed to durable storage.  This allows better utilization of storage hardware because an application doesn't have to wait until job data is fully flushed before it prepares the next batch of updates, even in the presence of data dependencies.  LLFS guarantees that jobs chained together in this way are observed by the outside world in the correct order.  Even if the application crashes, a later (speculative) job will not be observed unless the entire chain of base jobs has been fully committed.

## llfs::PageDevice

`llfs::PageDevice` is an abstract base class that represents a collection of pages of a single size.  Because `llfs::PageCache` may contain multiple `llfs::PageDevice` instances, each with its own page size, an application built on top of LLFS is not limited to a single page size, but rather may choose variously sized pages to optimize for different use cases.  Currently page size must be a power of 2 greater than or equal to 512. 

## llfs::PageAllocator

`llfs::PageAllocator` is built on top of `llfs::LogDevice` and offers a simple dynamic page allocation facility for `llfs::PageDevice` instances.  Instances of `PageAllocator` and `PageDevice` are 1:1.  `PageAllocator` is primarily responsible for maintaining the reference count and generation number of each physical page in a device.  

Because `PageAllocator` must do this in a crash-safe way, it needs to guarantee "exactly-once" updates for reference counts.  (For example, it would be bad if ref counts are incremented/decremented for a set of pages and then the program unexpectedly terminates before this fact can be recorded elsewhere, say in a `llfs::Volume` log record; then the program restarts and attempts to retry the ref-count updates, thus double-applying them ... oops!)  

The mechanism offered by `PageAllocator` to facilitate this is a "client attachment."  Each concurrent user of a `PageAllocator` must first attach itself, providing a UUID and a monotonically increasing initial "slot" offset.  The slot offset is defined by the client/caller; it can mean anything the client wants, provided it is monotonically increasing.  Each time a ref count update is submitted to a `PageAllocator`, it must include the UUID of an attached client, and a slot number that is greater than the last seen slot for that client.  `PageAllocator` will guarantee that only ref count updates with a slot offset strictly greater than the "current" slot upper bound for a given client will be applied.  Thus in the example use case above, when the program restarts and attempts to retry the ref-count update, the `PageAllocator` will see that the ref-count updates have already been applied because the slot number is up-to-date, and will acknowledge the update without double-applying.

When a `PageAllocator` is created, it is configured with a fixed-size pool of "attachments," effectively limiting the number of concurrent clients for a given device.

Newly allocated pages start out with a ref count value of 2.  Pages are considered garbage-collectable when ref count decreases to 1.  This is because when a page is no longer in use, it must still be read by the `llfs::PageRecycler`, as described below, to trace all outgoing page references within the "dead" page, so that their ref counts can be decremented as well.

## llfs::PageRecycler

 The job of `llfs::PageRecycler` is to simplify the process of freeing a physical page so it can be re-allocated.  Because pages can contain references to other pages (which affect the ref-count of the referenced page), when a page is recycled, the ref counts of any pages that it references must be decremented.  This process must continue recursively so that all "dead" pages are correctly reclaimed and no resources are leaked.

While it is possible for applications do to this directly using the `PageDevice` and `PageAllocator` APIs directly, it is somewhat non-trivial to do it correctly in a crash-safe manner.  `llfs::PageRecycler` stores its state in a `llfs::LogDevice`.  The state is comprised of a queue of `PageId` values identifying pages that are ready for recycling, and a "stack" for a depth-first traversal of recursively referenced pages.  `PageRecycler` always processes the deepest level of the stack first, to limit the total space requirements.  Thus the maximum reference depth is configured for a given `PageRecycler` at creation time.

`PageRecycler` also imposes a creation-time-configurable limit on the maximum "branching factor" of page refs (i.e., the maximum number of out-refs per page), and the depth of a reference chain.  Because these limits are highly dependent on the sorts of data structures and page sizes that must be accomodated by the `PageRecycler`, each `llfs::Volume` is given its own `PageRecycler`.  This allows different `Volume` instances to configure these parameters optimally for the type of data stored by that volume.

# How To Build

Requirements:

- gcc (11.2 or later)
- gnu make
- cmake (3.16 or later)
- ninja (1.11 or later)
- conan >=1.60, <2.0 (sorry, conan 2 support is coming!)
- Linux kernel 5.11 or later (for io_uring support)

## Configure Conan

```shell
conan profile update 'settings.compiler.libcxx=libstdc++11' default
conan profile update 'settings.compiler.cppstd=20' default
conan profile update 'env.CXXFLAGS=-fno-omit-frame-pointer' default
conan profile update 'env.CFLAGS=-fno-omit-frame-pointer' default
conan profile update 'env.LDFLAGS=-fno-omit-frame-pointer' default

conan remote add gitlab https://gitlab.com/api/v4/packages/conan
```

## Run Makefile

```shell
make
```

This is a shortcut for the more verbose command:

```shell
make install build test
```

Summary of Makefile targets:

- `install`
    - Like `conan install`; installs Conan package dependencies on the local system
- `build`
    - Like `conan build`; builds `libllfs.a`, `llfs_Test` (the unit test), and `llfs` (minimal CLI utility; WIP)
    - Requires:
        - `install`
- `test`
    - Runs unit tests
    - Requires:
        - `install`
        - `build`
- `export-pkg`
    - Exports LLFS Conan package (from existing build) to the local cache, for consumption by downstream projects
    - Requires:
        - `install`
        - `build`
    - Recommended:
        - `test`
- `create`
    - Creates LLFS Conan package using a clean build; _does not run tests_
- `clean`
    - Removes the build directory
- `clean-pkg`
    - Removes the current Conan package version from the local cache

# Test Configuration

## Docker and IO_URING

When running the test target for this project, you may run into an
EPERM error in the IoRing.Test.  To fix this, you must disable seccomp
(which docker configures in such a way as to prevent io_uring from
operating correctly).  Create the file `/etc/docker/seccomp.json` with
contents:

```json
{
    "defaultAction": "SCMP_ACT_ALLOW"
}
```

Then add the following properties to `/etc/docker/daemon.json`:

```json
    "seccomp-profile": "/etc/docker/seccomp.json",
    "selinux-enabled": false
```

Finally, restart the docker daemon:

```shell
sudo systemctl restart docker
```

## Docker and MEMLOCK ulimit

Test failures can occur when running under Docker with an attempt to create an `llfs::IoRing` object failing with "not enough memory" errors.  This is usually because the container's (or system's) MEMLOCK ulimit is set too low.  To fix this, the `--ulimit memlock=-1:-1` option may be added (to docker CLI invocations), or if you don't have control over the specific `docker run` command (e.g., if you're running Docker via the `gitlab-runner` daemon to process CI/CD pipelines), it will work on Ubuntu-based systems to modify the `docker.service` file (you can find it under `/etc`; e.g., `/etc/systemd/system/multi-user.target.wants/docker.service`); add the flag `--default-ulimit memlock=...` to the `ExecStart` setting:

```
ExecStart=/usr/bin/dockerd --default-ulimit memlock=819200000:819200000 -H fd:// --containerd=/run/containerd/containerd.sock
```

Then restart the docker daemon:

```
sudo systemctl restart docker
```

# License, Copyrights

Part of the LLFS Project, under Apache License v2.0.

See https://www.apache.org/licenses/LICENSE-2.0 for license information.

SPDX short identifier: Apache-2.0

&copy; 2022-2023, The MathWorks, Inc.
