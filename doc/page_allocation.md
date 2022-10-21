# Page Allocation

<!--[[_TOC_]]-->

## Summary 

LLFS tracks which pages within a `PageDevice` are in-use by storing a
reference count per page.  These counts are maintained by the
`PageAllocator` class/storage object, which persists its state to a
`LogDevice`.  To guarantee that reference count updates are always
consistent even in the face of crashes, "clients" wishing to interact
with a `PageAllocator` must attach to that allocator.  A given
`PageAllocator` is configured with a maximum number of attachments at
creation time.

**_IMPORTANT_**: In general, application code should allocate and
release pages via the `llfs::Volume` and `llfs::PageCacheJob` classes,
as these drastically simplify correct usage of the underlying
mechanisms.  The information here is provided in order to document the
internals of page allocation in LLFS.

## PageAllocator Interface

The `PageAllocator` interface may be broken down into the following
groups of related functionality:

- New Page Allocation
- Transactional Reference Count Updates
- Client Attachment Management
- PageAllocator Life Cycle Management
- Diagnostics

### New Page Allocation

When an application wishes to store a new page of data, it uses the
`PageAllocator::allocate_page` member function
('llfs/page_allocator.hpp'):

```c++
// Remove a page from the free pool but don't increment its refcount yet.
//
StatusOr<PageId> allocate_page(batt::WaitForResource wait_for_resource);
```

As the comment describes, this method will return a never-before-used
`PageId` that is backed by a free 'physical' page in the underlying
`PageDevice`.  The application will then go to the `PageDevice`
interface and call `prepare` to obtain a buffer into which the new
page data can be written ('llfs/page_device.hpp'):

```c++
virtual StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) = 0;
```

The application code should then pack the desired data into the
`PageBuffer`, write its intention to durably store the new page to a
`LogDevice` it controls, write the page data to the device using
`PageDevice::write`, and ONLY THEN update the ref counts for new pages
using `PageAllocator::update_page_ref_counts`.  This protocol is
described in detail in the "Multi-Page Transactions" document.

If for some reason the application wishes to back out of writing this
page (and durably taking it out of the free pool for the device), it
should call `PageAllocator::deallocate_page` and _not_ call
`PageDevice::write` for the given `PageId`.

### Transactional Reference Count Updates

Page ref counts are updated via `PageAllocator::update_page_ref_counts`:

```c++
template <typename PageRefCountSeq, typename GarbageCollectFn = DoNothing>
StatusOr<slot_offset_type> update_page_ref_counts(
    const boost::uuids::uuid& user_id, slot_offset_type user_slot,
    PageRefCountSeq&& ref_count_updates,
    GarbageCollectFn&& garbage_collect_fn = GarbageCollectFn{});
```

`user_id` identifies the client requesting the update.  This client
must currently be attached to the `PageAllocator`.  See "Client
Attachment Management" for more details.

`user_slot` specifies the relative ordering of ref count updates from
the specified client.  The contract between `PageAllocator` and client
for this value is as follows:

1. The `ref_count_updates` and `garbage_collect_fn` values passed to
   `PageAllocator` with for a given `user_id`/`user_slot` pair MUST be
   equivalent.  A client must NOT pass different/conflicting
   information for a given `user_slot`.
2. The `PageAllocator` is responsible for persisting ref count updates
   at a given `user_slot` _exactly once_.  If it sees a `user_slot`
   that it has already processed, then `update_page_ref_counts` MUST
   return `OkStatus` without writing any new data to its log.  (In
   this case, `garbage_collect_fn` is still invoked as it would have
   been the first time.)

When a new page is allocated, its initial reference count is 2.
When/if a future update changes this value to 1, the page is
considered "garbage-collectable."  PageIds with this status are passed
to the caller-supplied `garbage_collect_fn` after the updates are
written to the `PageAllocator` log.  This gives the application a
chance to recursively decrement the ref-counts of any pages referenced
by the garbage-collectable page before it is permanently deleted (via
`PageDevice::drop`).  This process is usually handled by an instance
of `PageRecycler`.

NOTE: When a page is ready to be permanently deleted via
`PageDevice::drop` (setting its ref count to 0 and allowing the
backing "physical" page in the device to be re-used), the ref count
MUST be updated using the special value `llfs::kRefCount_1_to_0`
(llfs/page_ref_count.hpp').  This is the only circumstance under which
this value should be used to update a ref count.  This acts as a
sanity check on application code, so that a page is never accidently
dropped without running the garbage collection algorithm to reclaim
indirect resources.

When `update_page_ref_counts` returns, the `PageAllocator` guarantees
that all future calls will see the updates from that call, but it
doesn't guarantee that the ref count updates are durably flushed to
the backing `LogDevice`.  To wait until the update has reached maximum
durability level, clients should call `Status
PageAllocator::sync(slot_offset_type min_slot)` with the slot offset
value returned by `update_page_ref_counts` (or a greater slot number).
This allows multiple `PageAllocator` logs to flush in the background
in parallel for transactions that span multiple `PageDevice`
instances.

### Client Attachment Management

Client attachment is managed via the following `PageAllocator` member
functions:

```c++
// Create an attachment used to detect duplicate change requests.
//
StatusOr<slot_offset_type> attach_user(const boost::uuids::uuid& user_id,
                                       slot_offset_type user_slot);

// Remove a previously created attachment.
//
StatusOr<slot_offset_type> detach_user(const boost::uuids::uuid& user_id,
                                       slot_offset_type user_slot);

```

Each of these functions returns the slot offset within the
`PageAllocator`'s log that records the attachment event.  Clients can
use `PageAllocator::sync` to guarantee that the attachment (or
detachment) has been durably flushed.

A client may attach and detach itself as many times as it wishes, but
it must be very careful when doing this.  Generally, a client should
never detach itself from a `PageAllocator` until it is sure that all
its previous ref count updates have been durably flushed to the log.
There is no way for a `PageAllocator` to tell whether an ordered
update has already been applied after the client that generated the
update has detached from the allocator.

### PageAllocator Life Cycle Management

The life cycle of a `PageAllocator` object at runtime is comprised of the following phases:

1. Recovery
2. Normal Use
3. Halted
4. Joined

#### Recovery

A `PageAllocator` object is created via the static `recover` member function:

```c++
static StatusOr<std::unique_ptr<PageAllocator>> recover(
    const PageAllocatorRuntimeOptions& options, const PageIdFactory& page_ids,
    LogDeviceFactory& log_device_factory);
```

This function will attempt to open the log using the passed
`llfs::LogDeviceFactory`, read its contents into memory to recover the
state of the allocator, and then create/return a `PageAllocator`
object.

#### Normal Use

This phase begins the moment that `recover` returns, up until the
moment when `PageAllocator::halt` is invoked or the `PageAllocator`
object goes out of scope (whichever comes first).

Most of the remaining functions (including all functions related to
page allocation, client attachment, and ref count updates) are invoked
during this lifecycle phase.  During this phase, the `PageAllocator`
may run one or more background tasks.

#### Halted

This phase begins when `PageAllocator::halt` (or
`PageAllocator::~PageAllocator`, which implicitly calls `halt()`) is
called.  It ends asynchronously at some unspecified moment after which
`PageAllocator::join()` will return without blocking.

Invoking `halt()` gives the allocator the command to shut everything
down and clean up after itself.  At this point any background tasks
are told to stop at their earliest convenience.

If `halt()` is called while other operations are pending concurrently
on other tasks/threads, those operations may be interrupted with an
error status.  Remember that in general, allocator events are not
guaranteed to be durable until `PageAllocator::sync` has returned with
non-error status for a given slot offset.

#### Joined

This phase begins once all background tasks managed by the
`PageAllocator` have terminated.

NOTE: `join()` does not imply `halt()`; if `join()` is called without
calling `halt()`, the application may deadlock!

`join()` and `halt()` are safe to call concurrently on the same
`PageAllocator` instance.  `~PageAllocator` automatically calls
`halt()` and then `join()`.

### Diagnostics

Various statistics and metrics are recorded by PageAllocator; see
'llfs/page_allocator.hpp' for more details.

## PageAllocator Internals

### The PageAllocator State Machine

### Log Compaction/Trimming

### PageAllocator First-Time Initialization

### PageAllocator Recovery Algorithm
