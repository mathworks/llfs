# Lock-Free Page Cache Design Proposal

## Problem

The current page cache index is implemented using a set of page-size-specific instances of the `llfs::Cache<K,V>` class.  This class is thread-safe but performs poorly under contention for two reasons:

  1. The mapping from PageId to buffered page data is maintained via a single `std::unordered_map` protected by a single mutex
  2. The LRU eviction policy is implemented using a per-`llfs::Cache` object linked list of pages in LRU order, which is also protected by the same mutex

Note that number 2 essentially turns every access (which should update the notion of recency-of-use somehow) into a write, meaning that we can't even use a reader/writer locking strategy to optimize read-heavy workloads.

## Proposed Solution

### High Level

We address the two problems above in different ways, each of which can be implemented separately (though the solution to concurrent hash table lookups is somewhat pointless without also solving the LRU maintenance problem as well).

Instead of maintaining LRU order via a linked list of cache slots, we will instead update a logical time stamp per slot on each access to that slot.  Eviction will be implemented by randomly selecting some constant _k_ number of candidate slots to evict, then attempting to evict the one with the oldest (smallest-valued) logical time stamp.  This trades off some accuracy for a (potentially) dramatic increase in scalability and performance.


The concurrent hash table access problem will be solved by implementing a new lock-free index system, where an array of 64-bit atomic integers will be maintained for each `PageDevice`.  These will index into a pool of cache slot objects; there will be one pool per page size, and multiple PageDevice instances may share a single pool.  This design takes advantage of the fact that a given page is kept alive only while there are any references to that page, with the underlying storage resources of a page being reused by multiple page ids over time.  This means that while the total space of `PageId` values is quite large (and the active set at any given time is sparse within this space), the number of _active_ (i.e. readable) pages at a given time is much smaller, and is dense due to the fact that page ids map to a physical storage location.

### Details

#### Background: CacheSlot

The `llfs::CacheSlot<K,V>` class holds a key/value pair in the current implementation.  `CacheSlot` instances are essentially the values in the hash-table index used in the current `Cache` implementation.  There are two ways for some part of the system to obtain a reference to a `CacheSlot`: 

1. Perform a lookup in the `Cache` object that owns the slot
2. Promote an atomic "weak" reference to a strong (pinned) reference

Method 2 allows applications that use LLFS to implement lookup strategies that are optimized to that use case, in order to reduce contention on the `Cache` hash table.  For example, an application might implement a tree-like structure where one page contains references to other pages.  The `PageView` implementation for this page layout might contain a number of `llfs::AtomicCacheSlotRef` objects (one for each outgoing page reference), each of which is lazily initialized to point at the cache slot containing the referenced page the first time the reference is followed.  On subsequent traversals of the link from the referring page to the referent, the application can attempt first to "pin" `llfs::AtomicCacheSlotRef` for that link.  This will succeed iff the slot still contains the target page.  If that slot has been evicted due to cache pressure, the application will fall back on the default load path for the page, which will allocate a new cache slot and update the hash table in `Cache`. 

**_Note: The potential concurrent scalability offered by this design is currently thwarted by the need to maintain an LRU list whenever a page is accessed; even when weak slot references are successfully pinned, we fail to avoid locking the single shared mutex that protects the entire Cache state._**

`llfs::CacheSlot` has a field, `std::atomic<u64> state_`, which is responsible for storing the current state of the slot and coordinating transitions between the different states.  The states of a `CacheSlot` are:

  - Invalid (the initial state; the slot _can_ return to this state repeatedly in its lifespan)
  - Valid + Filled
  - Valid + Filled + Pinned
  - Valid + Cleared

These are mapped onto the state integer as follows:

```
┌───────┬───────┬────────┬────────────────┬────────┬────────────────┐
│Valid? │Unused │Overflow│DecreasePinCount│Overflow│IncreasePinCount│
│(1 bit)│(1 bit)│(1 bit) │   (30 bits)    │(1 bit) │   (30 bits)    │
└───────┴───────┴────────┴────────────────┴────────┴────────────────┘
```

The `Valid?` bit determines whether the slot is in the Invalid state (`Valid? == 0`) or one of the Valid states.

The current "pin count" is defined as `IncreasePinCount - DecreasePinCount`; it is the number of active "pins" on the cache slot.  A slot may not be evicted or modified when it is pinned.  The slot is considered "pinned" when the pin count is not zero (negative pin count is illegal; the invariant is that `IncreasePinCount >= DecreasePinCount` at all times).

After updating either of the pin counts, if we observe that the corresponding overflow bit is set, we simply do an atomic `fetch_and` operation to set it back to zero.  This allows us to use atomic `fetch_add` operations instead of CAS to update the pin count.  For example:

```c++
  // Increment the pin count. 
  //
  const u64 observed_state = this->state_.fetch_add(kIncreasePinDelta);
  const u64 new_state = observed_state + kIncreasePinDelta;

  if (new_state & kIncreasePinOverflow) {
    this->state_.fetch_and(~kIncreasePinOverflow);
  }
```

The key and value data for a slot are stored in the following data members:

```c++
  Optional<K> key_;
  std::shared_ptr<V> value_;
```

If the `key_` field is set to `None` _and_ the slot is in a valid state (`Valid?` bit is set) then the state is Valid + Cleared.  If `key_` is non-`None`, then the slot is in a Filled state (and `value_` is presumed to be non-null, the current value bound to key).  Thus Cleared and Filled are mutually exclusive.

The state transitions for CacheSlot are shown below:

```
                                    ┌─────────┐
                              ┌─────│ Invalid │──────────┐
                       fill() │     └─────────┘          │ clear()
                              │          ▲               │
                              │          │               │
                              │          │evict()        │
                              ▼          │               ▼
                     ┌────────────────┐  │      ┌─────────────────┐
              ┌──────│ Valid + Filled │──┴──────│ Valid + Cleared │
              │      └────────────────┘         └─────────────────┘
              │               ▲
acquire_pin():│               │
    0 -> 1    │               │release_pin():
              │               │    1 -> 0
              │               │
              │  ┌─────────────────────────┐
              └─▶│ Valid + Filled + Pinned │
                 └─────────────────────────┘
```

It is only legal to read the key or value of a slot if it is in a Valid state.  If the slot enters the Invalid state because a thread is able to successfully call evict(), then that thread has exclusive access to the key and value fields.  This allows the either clear() or fill() to be called to change the key/value fields.  For this reason, readers must be careful to prevent a slot from transitioning states while they are reading the key and/or value fields.  Thus a reader must first "pin" the slot by incrementing the pin count; it can then check to see whether the pin succeeded (is the `Valid?` bit set?).  If successful, the reader can proceed to read the key and value, confident there is no data race.  Otherwise, it must restore the pin count to its prior value by incrementing the `DecreasePinCount` value.  This design allows the use of a single atomic `fetch_add` instruction to pin a slot (happy path), instead of a more expensive CAS instruction.

This state machine mechanism essentially implements a non-blocking reader/writer locking system for a single cache slot.  To reiterate: if the pin count is non-zero, this means there are read locks held against the slot; it therefore must not be changed.  Conversely, if the slot is in the Invalid state, this means there is a unique (exclusive) write lock held against the slot; whoever called `evict()` to force the slot into this state (or whoever constructed the slot object) is free to modify the slot key and value without fear of a data race.


#### LRU Maintenance

A new field, `std::atomic<i64> latest_use_`, will be added to the class `llfs::CacheSlot<K,V>`.  This will be updated with a new logical time stamp (LTS) whenever that slot is accessed from the cache.  The LTS values will be provided by a new singleton class `llfs::LRUClock`.  The LRU clock will work by maintaining thread-local `i64` counters which are added to a global linked list the first time the thread-local object is created (on a new thread); the counter will later be removed from the global linked list in the destructor for the counter class (`llfs::LRUClock::LocalCounter`).  The global linked list of counters will be protected by a mutex.  When the `LRUClock` singleton is first initialized, it will create a background thread (`std::thread`) that will periodically synchronize the thread-local counters via the following procedure:

  1. Lock the list mutex
  2. Iterate over all the `LocalCounter` objects, saving the maximum value in a field of `LRUClock`
  3. Do a second pass over all the `LocalCounter`s, this time clamping their value to at least the maximum observed in step 2

This will keep the thread-local LTS counters from drifting too far from each other over time.  The `LRUClock` background thread will sleep for an average of 500 microseconds, with random jitter, in between each synchronization, so as not to impose much overhead on the rest of the system (this is something we can easily tune later if it turns out not to be a good choice).

The `LRUClock` class will provide the following interface:

```c++
  /** \brief Returns the current thread's local counter.
   */
  static i64 read_local() noexcept;

  /** \brief Increments the current thread's local counter, returning the old value.
   */
  static i64 advance_local() noexcept;

  /** \brief Returns the last observed maximum count (over all thread-local values); this may be
   * slightly out of date, as it is only updated by the background sync thread.
   */
  static i64 read_global() noexcept;
```

Note that the maximum observed LTS value from step 2 above is saved in between rounds of synchronization so that LTS values continue to move forward even if all threads with a local counter happen to go away at some point.

#### Lock-Free Page Cache Index

##### Proposed Design

We propose the following changes to the existing design:

1. Remove the generality of `Cache<K, V>` and `CacheSlot<K, V>`, replacing these with concrete classes that explicitly name `llfs::PageId` as the key type, and `batt::Latch<std::shared_ptr<const llfs::PageView>>` as the value type.
2. Replace `CacheSlot<K, V>` with `llfs::PageCacheSlot`, as described below
3. Replace `Cache<K, V>` with two types that separate the concerns currently both handled inside `Cache<K, V>`:
   1. `llfs::PageDeviceCache` implements a per-`PageDevice` physical-page-index to cache slot index (using an array of atomic `u64` values)
   2. `llfs::PageCacheSlot::Pool` implements a shared pool of cache slots; one pool can be shared among many `PageDeviceCache` objects
4. Simplify the design of the `PageCacheSlot` state update mechanism; we don't really need two counters for increase and decrease of pin count, and we can also avoid heap-allocating the `Latch` object in favor of using `Optional<Latch<std::shared_ptr<const PageView>>>`.
