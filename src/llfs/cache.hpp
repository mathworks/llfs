//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CACHE_HPP
#define LLFS_CACHE_HPP

#include <llfs/logging.hpp>

#include <llfs/int_types.hpp>
#include <llfs/metrics.hpp>
#include <llfs/optional.hpp>
#include <llfs/slice.hpp>
#include <llfs/status_code.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/cpu_align.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/operators.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <atomic>
#include <functional>
#include <memory>

namespace llfs {

using CacheLRUHook = boost::intrusive::list_base_hook<boost::intrusive::tag<struct CacheLRUTag>>;

using LogicalTimeStamp = u64;

template <typename K, typename V>
class CacheSlot;

template <typename K, typename V>
class AttachedCacheSlot;

template <typename K, typename V>
class PinnedCacheSlot;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename K, typename V>
class Cache : public boost::intrusive_ref_counter<Cache<K, V>>
{
 public:
  struct Metrics {
    CountMetric<u64> max_slots{0};
    CountMetric<u64> indexed_slots{0};
    CountMetric<u64> query_count{0};
    CountMetric<u64> hit_count{0};
    CountMetric<u64> stale_count{0};
    CountMetric<u64> alloc_count{0};
    CountMetric<u64> evict_count{0};
    CountMetric<u64> insert_count{0};
    CountMetric<u64> erase_count{0};
    CountMetric<u64> full_count{0};
  };

  using Slot = AttachedCacheSlot<K, V>;
  using PinnedSlot = PinnedCacheSlot<K, V>;

  static boost::intrusive_ptr<Cache> make_new(usize n_slots, const std::string& name)
  {
    return boost::intrusive_ptr<Cache>{new Cache{n_slots, name}};
  }

 private:
  explicit Cache(usize n_slots, const std::string& name) noexcept
      : n_slots_{n_slots}
      , name_{name}
      , slots_{new batt::CpuCacheLineIsolated<Slot>[n_slots]}
  {
    initialize_status_codes();

    LLFS_VLOG(1) << "Cache(n_slots=" << n_slots << ")";
    auto locked = this->free_pool_.lock();
    for (auto& isolated_slot : as_slice(this->slots_.get(), this->n_slots_)) {
      isolated_slot->set_cache_ptr(this);
      locked->push_back(*isolated_slot.get());
    }

    this->metrics_.max_slots.set(n_slots);

    const auto metric_name = [this](std::string_view property) {
      return batt::to_string("Cache_", this->name_, "_", property);
    };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), this->metrics_.n)

    ADD_METRIC_(max_slots);
    ADD_METRIC_(indexed_slots);
    ADD_METRIC_(query_count);
    ADD_METRIC_(hit_count);
    ADD_METRIC_(stale_count);
    ADD_METRIC_(alloc_count);
    ADD_METRIC_(evict_count);
    ADD_METRIC_(insert_count);
    ADD_METRIC_(erase_count);
    ADD_METRIC_(full_count);

#undef ADD_METRIC_
  }

 public:
  ~Cache() noexcept
  {
    global_metric_registry()
        .remove(this->metrics_.max_slots)
        .remove(this->metrics_.indexed_slots)
        .remove(this->metrics_.query_count)
        .remove(this->metrics_.hit_count)
        .remove(this->metrics_.stale_count)
        .remove(this->metrics_.alloc_count)
        .remove(this->metrics_.evict_count)
        .remove(this->metrics_.insert_count)
        .remove(this->metrics_.erase_count)
        .remove(this->metrics_.full_count);

    LLFS_VLOG(1) << "Cache::~Cache()";
  }

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  // Attempt to locate `key` in the cache.  If not found, attempt to allocate a slot (either from
  // the free pool or by evicting an unpinned slot in LRU order) and fill it with the value returned
  // by `factory`.  If the key is not found and a slot could not be allocated/evicted, return an
  // empty PinnedSlot; else return a PinnedSlot that points ot the cached value.
  //
  // `factory` may be invoked with one or more internal locks held; therefore it MUST NOT invoke any
  // methods of this Cache object, directly or indirectly.
  //
  batt::StatusOr<PinnedSlot> find_or_insert(const K& key,
                                            std::function<std::shared_ptr<V>()>&& factory)
  {
    this->metrics_.query_count.fetch_add(1);

    auto locked_index = this->index_.lock();

    auto iter = locked_index->find(key);
    if (iter != locked_index->end()) {
      Slot* slot = this->slots_[iter->second].get();

      PinnedSlot pinned = slot->acquire_pin(key);
      if (pinned) {
        this->metrics_.hit_count.fetch_add(1);
        return pinned;
      }
      this->metrics_.stale_count.fetch_add(1);

      // If the pin failed, we should erase this slot from the index (which is evidently
      // out-of-date).
      //
      locked_index->erase(iter);
      this->metrics_.indexed_slots.set(locked_index->size());
    }

    // Not found in the index.  Try grabbing a slot from the free pool.
    //
    {
      auto locked_pool = this->free_pool_.lock();
      if (!locked_pool->empty()) {
        this->metrics_.alloc_count.fetch_add(1);
        Slot& free_slot = locked_pool->front();
        locked_pool->pop_front();
        return this->fill_slot_and_insert(locked_index, free_slot, key, std::move(factory));
      }
    }

    // No free slots; we must try to evict the least recently used one.
    //
    Slot* lru_slot = this->evict_lru();
    if (!lru_slot) {
      this->metrics_.full_count.fetch_add(1);

      // TODO [tastolfi 2022-09-19] Add a batt::WaitForResource parameter to this function so that
      // instead of returning this status, we wait until there is a cache slot available.
      //
      return batt::Status{StatusCode::kCacheSlotsFull};
    } else {
      this->metrics_.evict_count.fetch_add(1);
    }

    // Erase the old key from the index, since this slot has been evicted.
    //
    locked_index->erase(lru_slot->key());

    return this->fill_slot_and_insert(locked_index, *lru_slot, key, std::move(factory));
  }

  // Forcibly remove the given key from the cache, if present.  Returns true if the key was found,
  // false otherwise.
  //

  bool erase(const K& key)
  {
    auto locked_index = this->index_.lock();

    auto iter = locked_index->find(key);
    if (iter == locked_index->end()) {
      return false;
    }
    this->metrics_.erase_count.fetch_add(1);

    // Make a best-effort attempt to clear out the slot; it's ok if this fails, since erasing the
    // key from the index will prevent any new pins from being added, and eventually the slot's pin
    // count will drain to 0, allowing it to be evicted organically.
    //
    Slot* slot = this->slots_[iter->second].get();
    this->evict_and_clear_slot(slot);

    locked_index->erase(iter);
    this->metrics_.indexed_slots.set(locked_index->size());

    return true;
  }

 private:
  using LRUList = boost::intrusive::list<Slot, boost::intrusive::base_hook<CacheLRUHook>>;

  // Iterate through slots in LRU order, trying to evict one.  The worst case number of times
  // through this loop is the number of threads actively pinning a slot right now.
  //
  Slot* evict_lru()
  {
    auto locked_lru = this->lru_.lock();

    for (Slot& lru_slot : *locked_lru) {
      if (lru_slot.evict()) {
        if (lru_slot.CacheLRUHook::is_linked()) {
          locked_lru->erase(locked_lru->iterator_to(lru_slot));
          BATT_CHECK(!lru_slot.CacheLRUHook::is_linked());
        }
        return &lru_slot;
      }
    }
    return nullptr;
  }

  // Attempt to evict a specific slot; if this succeeds, clear the slot and move it to the LRU-end
  // of the LRU list (to allow it to be filled next).
  //
  bool evict_and_clear_slot(Slot* slot)
  {
    auto locked_lru = this->lru_.lock();

    if (!slot->evict()) {
      return false;
    }
    this->metrics_.evict_count.fetch_add(1);

    if (slot->CacheLRUHook::is_linked()) {
      locked_lru->erase(locked_lru->iterator_to(*slot));
    }
    locked_lru->push_front(*slot);
    slot->clear();

    return true;
  }

  PinnedSlot fill_slot_and_insert(
      typename batt::Mutex<std::unordered_map<K, usize>>::Lock& locked_index, Slot& dst_slot,
      const K& key, std::function<std::shared_ptr<V>()>&& factory)
  {
    BATT_CHECK(!dst_slot.is_valid());

    dst_slot.fill(key, factory());
    PinnedSlot pinned = dst_slot.acquire_pin(key);

    BATT_CHECK(pinned);

    locked_index->emplace(key,
                          std::distance(this->slots_.get(),
                                        batt::CpuCacheLineIsolated<Slot>::pointer_from(&dst_slot)));

    this->metrics_.insert_count.fetch_add(1);
    this->metrics_.indexed_slots.set(locked_index->size());

    return pinned;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <typename, typename>
  friend class AttachedCacheSlot;

  void notify_pinned(Slot* slot, u64 expected_state)
  {
    auto locked_lru = this->lru_.lock();
    if (slot->get_state() == expected_state && slot->CacheLRUHook::is_linked()) {
      locked_lru->erase(locked_lru->iterator_to(*slot));
    }
  }

  void notify_unpinned(Slot* slot, u64 expected_state)
  {
    auto locked_lru = this->lru_.lock();
    if (slot->get_state() == expected_state) {
      if (slot->CacheLRUHook::is_linked()) {
        locked_lru->erase(locked_lru->iterator_to(*slot));
      }
      // Front is least recently used; back is most recently used.
      //
      if (slot->get_obsolete_hint()) {
        locked_lru->push_front(*slot);
      } else {
        locked_lru->push_back(*slot);
      }
      BATT_CHECK(slot->CacheLRUHook::is_linked());
    }
  }
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  const usize n_slots_;
  const std::string name_;
  std::unique_ptr<batt::CpuCacheLineIsolated<Slot>[]> slots_;
  batt::Mutex<std::unordered_map<K, usize>> index_;
  Metrics metrics_;
  batt::Mutex<LRUList> free_pool_;
  batt::Mutex<LRUList> lru_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// A container for a single key/value pair in a Cache.
//
// CacheSlot objects are always in one of four states:
//  - Invalid (initial)
//  - Valid + Filled
//  - Valid + Filled + Pinned
//  - Valid + Cleared
//
template <typename K, typename V>
class CacheSlot
{
 public:
  static constexpr unsigned kPinCountBits = 30;
  static constexpr u64 kIncreasePinDelta = 1;
  static constexpr u64 kIncreasePinOverflow = u64{1} << 30;
  static constexpr u64 kDecreasePinDelta = u64{1} << 31;
  static constexpr u64 kDecreasePinOverflow = u64{1} << 61;
  static constexpr u64 kPinCountMask = kIncreasePinOverflow - 1;
  static constexpr u64 kMaxPinCount = u64{1} << (kPinCountBits - 1);
  static constexpr u64 kValidMask = u64{1} << 63;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u32 get_pin_acquire_count(u64 state)
  {
    return u32(state & kPinCountMask);
  }
  static constexpr u32 get_pin_release_count(u64 state)
  {
    return u32((state >> 31) & kPinCountMask);
  }

  static constexpr u32 get_pin_count(u64 state)
  {
    const u32 acquire_count = get_pin_acquire_count(state);
    const u32 release_count = get_pin_release_count(state);

    BATT_CHECK_LE(acquire_count - release_count, kMaxPinCount);

    return acquire_count - release_count;
  }

  static constexpr bool is_pinned(u64 state)
  {
    return get_pin_acquire_count(state) != get_pin_release_count(state);
  }

  static constexpr bool is_valid(u64 state)
  {
    return (state & kValidMask) != 0;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  CacheSlot(const CacheSlot&) = delete;
  CacheSlot& operator=(const CacheSlot&) = delete;

  virtual ~CacheSlot() = default;

  CacheSlot() = default;

  // Returns the current key held in the slot, if valid; if the slot is invalid, behavior is
  // undefined.
  //
  const K& key() const
  {
    return *this->key_;
  }

  // Returns the current value held in the slot, if valid; if the slot is invalid, behavior is
  // undefined.
  //
  V* value() const noexcept
  {
    return this->value_.get();
  }

  // Returns true iff the slot is in a valid state.
  //
  bool is_valid() const noexcept
  {
    return this->is_valid(this->state_.load(std::memory_order_acquire));
  }

  // Returns true iff the slot is in a pinned state.
  //
  bool is_pinned() const noexcept
  {
    return this->is_pinned(this->state_.load(std::memory_order_acquire));
  }

  // Returns the current pin count of the slot; if this is 0, the slot is not pinned.
  //
  u32 pin_count() const noexcept
  {
    return this->get_pin_count(this->state_.load(std::memory_order_acquire));
  }

  // Return the current (weak/non-pinning) reference count.
  //
  u64 ref_count() const noexcept
  {
    return this->ref_count_.load(std::memory_order_acquire);
  }

  // Add a (weak/non-pinning) reference to the slot.  Used to avoid premature destruction of the
  // cache.
  //
  void add_ref() noexcept
  {
    const auto observed_count = this->ref_count_.fetch_add(1, std::memory_order_relaxed);
    if (observed_count == 0) {
      this->notify_first_ref_acquired();
    }
  }

  // Remove a (weak/non-pinning) reference from the slot.  Used to avoid premature destruction of
  // the cache.
  //
  void remove_ref() noexcept
  {
    const auto observed_count = this->ref_count_.fetch_sub(1, std::memory_order_release);
    BATT_CHECK_GT(observed_count, 0);
    if (observed_count == 1) {
      (void)this->ref_count_.load(std::memory_order_acquire);
      this->notify_last_ref_released();
    }
  }

  // Conditionally pins the slot so it can't be evicted.  This operation is conditioned on `key`
  // matching the currently stored key in the slot.  If the slot is in an invalid state or the key
  // doesn't match, the operation will fail and an empty/invalid value is returned.
  //
  // A slot is removed from the cache's LRU list when its pin count goes from 0 -> 1, and placed
  // back at the "most recently used" end of the LRU list when the pin count goes from 1 -> 0.
  //
  PinnedCacheSlot<K, V> acquire_pin(const K& key)
  {
    const auto observed_state =
        this->state_.fetch_add(kIncreasePinDelta, std::memory_order_acquire);
    const auto new_state = observed_state + kIncreasePinDelta;
    const bool newly_pinned = !this->is_pinned(observed_state);

    if (new_state & kIncreasePinOverflow) {
      this->state_.fetch_and(~kIncreasePinOverflow);
    }

    BATT_CHECK(this->is_pinned(new_state));

    BATT_SUPPRESS("-Wmaybe-uninitialized")

    // We must always do this, even if the pin fails, so that we don't have an unmatched
    // `remove_ref` in `release_pin` below.
    //
    if (newly_pinned) {
      this->add_ref();
    }

    // Because the pin_count > 1 (because of the fetch_add above) and the slot is valid (because
    // generation is not odd), it is safe to read the key.  If the generation and/or key don't
    // match, release the ref and return failure.
    //
    if (!this->is_valid(observed_state) || !this->key_ || *this->key_ != key) {
      this->release_pin();
      return {};
    }

    // Unlink from the LRU list when going from pin_count 0 -> 1.
    //
    if (newly_pinned) {
      this->notify_pinned(new_state);
    }

    BATT_UNSUPPRESS()

    return PinnedCacheSlot<K, V>{this};
  }

  // Called when creating a copy of PinnedCacheSlot, i.e. only when the pin count is going from n ->
  // n+1, where n > 0.
  //
  void extend_pin() noexcept
  {
    const auto observed_state =
        this->state_.fetch_add(kIncreasePinDelta, std::memory_order_relaxed);
    const auto new_state = observed_state + kIncreasePinDelta;

    if (new_state & kIncreasePinOverflow) {
      this->state_.fetch_and(~kIncreasePinOverflow);
    }

    BATT_CHECK(this->is_pinned(observed_state))
        << "This method should never be called in cases where the current pin count might be 0; "
           "use pin() "
           "instead.";
  }

  // Decrease the pin count by 1.  If this unpins the slot, then also remove a single weak ref and
  // place this slot at the MRU end of the LRU list.
  //
  void release_pin() noexcept
  {
    const auto observed_state =
        this->state_.fetch_add(kDecreasePinDelta, std::memory_order_release);

    BATT_CHECK(this->is_pinned(observed_state))
        << "Each call to release_pin should have a previous call to "
           "acquire_pin, so we should always observe a prior pinned state. "
        << BATT_INSPECT(observed_state);

    const auto new_state = observed_state + kDecreasePinDelta;
    const bool newly_unpinned = !this->is_pinned(new_state);

    if (new_state & kDecreasePinOverflow) {
      this->state_.fetch_and(~kDecreasePinOverflow);
    }

    if (newly_unpinned) {
      // Load the state with `acquire` order to create a full memory barrier.
      //
      (void)this->state_.load(std::memory_order_acquire);

      this->notify_unpinned(new_state);
      this->remove_ref();
    }
  }

  // If this slot is not pinned and it is not evicted, atomically increment the generation counter
  // and return true; else return false.
  //
  // If evict() succeeds (returns true), then the slot is in an "invalid" state.
  //
  bool evict()
  {
    // Use a CAS loop here to guarantee an atomic transition from filled + valid + unpinned state to
    // invalid.
    //
    auto observed_state = this->state_.load(std::memory_order_acquire);
    for (;;) {
      if (this->is_pinned(observed_state) || !this->is_valid(observed_state)) {
        return false;
      }

      // Clear the valid bit from the state mask.
      //
      const auto target_state = observed_state & ~kValidMask;
      if (this->state_.compare_exchange_weak(observed_state, target_state)) {
        BATT_CHECK(!this->is_valid());
        return true;
      }
    }
  }

  // Set the key and value for this slot, then atomically increment the generation counter.  The
  // generation counter must be odd (indicating the slot has been evicted) prior to calling this
  // function.
  //
  // May only be called when the slot is in an invalid state.
  //
  void fill(K key, std::shared_ptr<V>&& value)
  {
    BATT_CHECK(!this->is_valid());

    this->key_.emplace(key);
    this->value_ = std::move(value);
    this->obsolete_hint_ = false;
    this->set_valid();
  }

  // Set the key and value of the slot to empty/null.  This causes the slot to leave the invalid
  // state, but all attempts to pin will fail until it is filled.
  //
  void clear()
  {
    BATT_CHECK(!this->is_valid());

    this->key_ = None;
    this->value_ = nullptr;
    this->obsolete_hint_ = false;
    this->set_valid();
  }

  // Get the current state word.  This is used to make sure we don't reorder LRU list operations
  // when transitioning from pinned to unpinned (and vice versa).
  //
  u64 get_state() const
  {
    return this->state_.load(std::memory_order_acquire);
  }

  // Give a hint to the cache as to whether this slot is likely to be needed again in the future. If
  // obsolete_hint is true, then when the slot is unpinned, it will be placed in the next-to-evict
  // LRU position.
  //
  void set_obsolete_hint(bool hint)
  {
    this->obsolete_hint_.store(hint);
  }

  bool get_obsolete_hint() const
  {
    return this->obsolete_hint_.load();
  }

 private:
  // Must be provided by derived classes; invoked when the pin count goes from 0 -> 1.
  //
  virtual void notify_pinned(u64 expected_state) = 0;

  // Invoked when the pin count goes from 1 -> 0.
  //
  virtual void notify_unpinned(u64 expected_state) = 0;

  // Invoked when the ref count goes from 0 -> 1.
  //
  virtual void notify_first_ref_acquired() = 0;

  // Invoked when the ref count goes from 1 -> 0.
  //
  virtual void notify_last_ref_released() = 0;

  // Increment the generation; Panic if this does *not* result in an even generation number.
  //
  void set_valid()
  {
    const auto observed_state = this->state_.fetch_or(kValidMask, std::memory_order_release);
    BATT_CHECK(!this->is_valid(observed_state)) << "Must go from an invalid state to valid!";
  }

  Optional<K> key_;
  std::shared_ptr<V> value_;
  std::atomic<u64> state_{0};
  std::atomic<u64> ref_count_{0};
  std::atomic<bool> obsolete_hint_{false};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// A CacheSlot in the context of a Cache.  Contains a back-pointer to the cache.  The overall cache
// slot object type is split into the hierarchy of CacheSlot and AttachedCacheSlot to limit the
// impact of declaring AttachedCacheSlot a friend of Cache.
//
template <typename K, typename V>
class AttachedCacheSlot
    : public CacheSlot<K, V>
    , public CacheLRUHook
{
 public:
  using CacheSlot<K, V>::CacheSlot;

  void set_cache_ptr(Cache<K, V>* cache)
  {
    this->cache_ = cache;
  }

  Cache<K, V>* get_cache_ptr() const
  {
    return this->cache_;
  }

 private:
  void notify_pinned(u64 expected_state) override
  {
    this->cache_->notify_pinned(this, expected_state);
  }

  void notify_unpinned(u64 expected_state) override
  {
    this->cache_->notify_unpinned(this, expected_state);
  }

  void notify_first_ref_acquired() override
  {
    intrusive_ptr_add_ref(this->cache_);
  }

  void notify_last_ref_released() override
  {
    intrusive_ptr_release(this->cache_);
  }

  Cache<K, V>* cache_ = nullptr;
};

namespace detail {

template <typename K, typename V>
inline CacheSlot<K, V>* increment_weak_ref(CacheSlot<K, V>* slot)
{
  if (slot) {
    slot->add_ref();
  }
  return slot;
}

template <typename K, typename V>
inline void decrement_weak_ref(CacheSlot<K, V>* slot)
{
  if (slot) {
    slot->remove_ref();
  }
}

}  // namespace detail

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename K, typename V>
class CacheSlotRef
{
 public:
  CacheSlotRef() = default;

  /*implicit*/ CacheSlotRef(const PinnedCacheSlot<K, V>& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
    if (pinned) {
      this->key_.emplace(pinned.key());
    }
  }

  /*implicit*/ CacheSlotRef(PinnedCacheSlot<K, V>&& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
    if (pinned) {
      this->key_.emplace(pinned.key());
    }
    pinned.reset();
  }

  CacheSlotRef(const CacheSlotRef& that) noexcept
      : key_{that.key_}
      , slot_{detail::increment_weak_ref(that.slot_)}
  {
  }

  CacheSlotRef(CacheSlotRef&& that) noexcept  //
      : key_{that.key_}
      , slot_{that.slot_}
  {
    that.slot_ = nullptr;
  }

  ~CacheSlotRef() noexcept
  {
    detail::decrement_weak_ref(this->slot_);
  }

  void swap(CacheSlotRef& that)
  {
    std::swap(this->key_, that.key_);
    std::swap(this->slot_, that.slot_);
  }

  CacheSlotRef& operator=(const CacheSlotRef& that) noexcept
  {
    CacheSlotRef copy{that};
    copy.swap(*this);
    return *this;
  }

  CacheSlotRef& operator=(CacheSlotRef&& that) noexcept
  {
    CacheSlotRef copy{std::move(that)};
    copy.swap(*this);
    return *this;
  }

  explicit operator bool() const
  {
    return this->key_ && this->slot_;
  }

  PinnedCacheSlot<K, V> pin()
  {
    if (!this->key_ || !this->slot_) {
      return {};
    }
    return this->slot_->acquire_pin(*this->key_);
  }

 private:
  Optional<K> key_;
  CacheSlot<K, V>* slot_ = nullptr;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename K, typename V>
class AtomicCacheSlotRef : public boost::equality_comparable<AtomicCacheSlotRef<K, V>>
{
 public:
  AtomicCacheSlotRef() = default;

  /*implicit*/ AtomicCacheSlotRef(const PinnedCacheSlot<K, V>& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
  }

  /*implicit*/ AtomicCacheSlotRef(PinnedCacheSlot<K, V>&& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
    pinned.reset();
  }

  AtomicCacheSlotRef(const AtomicCacheSlotRef& that) noexcept
      : slot_{detail::increment_weak_ref(that.slot_.load())}
  {
  }

  AtomicCacheSlotRef(AtomicCacheSlotRef&& that) noexcept : slot_{that.slot_.exchange(nullptr)}
  {
  }

  void unsynchronized_swap(AtomicCacheSlotRef& that)
  {
    this->slot_.store(that.slot_.exchange(this->slot_.load()));
  }

  AtomicCacheSlotRef& operator=(const AtomicCacheSlotRef& that) noexcept
  {
    AtomicCacheSlotRef copy{that};
    copy.unsynchronized_swap(*this);
    return *this;
  }

  AtomicCacheSlotRef& operator=(AtomicCacheSlotRef&& that) noexcept
  {
    AtomicCacheSlotRef copy{std::move(that)};
    copy.unsynchronized_swap(*this);
    return *this;
  }

  ~AtomicCacheSlotRef() noexcept
  {
    detail::decrement_weak_ref(this->slot_.load());
  }

  explicit operator bool() const
  {
    return this->slot_.load() != nullptr;
  }

  PinnedCacheSlot<K, V> pin(const K& key) const
  {
    auto* slot = this->slot_.load();
    if (!slot) {
      return {};
    }
    return slot->acquire_pin(key);
  }

  CacheSlot<K, V>* slot() const
  {
    return this->slot_.load();
  }

 private:
  std::atomic<CacheSlot<K, V>*> slot_{nullptr};
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename K, typename V>
inline bool operator==(const AtomicCacheSlotRef<K, V>& l, const AtomicCacheSlotRef<K, V>& r)
{
  return l.slot() == r.slot();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename K, typename V>
class PinnedCacheSlot : public boost::equality_comparable<PinnedCacheSlot<K, V>>
{
 public:
  PinnedCacheSlot() = default;

 private:
  template <typename, typename>
  friend class CacheSlot;

  explicit PinnedCacheSlot(CacheSlot<K, V>* slot) noexcept
      : slot_{slot}
      , value_{slot ? slot->value() : nullptr}
  {
  }

 public:
  PinnedCacheSlot(const PinnedCacheSlot& that) noexcept : slot_{that.slot_}, value_{that.value_}
  {
    if (this->slot_) {
      this->slot_->extend_pin();
    }
  }

  PinnedCacheSlot& operator=(const PinnedCacheSlot& that) noexcept
  {
    PinnedCacheSlot copy{that};
    this->swap(copy);
    return *this;
  }

  PinnedCacheSlot(PinnedCacheSlot&& that) noexcept : slot_{that.slot_}, value_{that.value_}
  {
    that.slot_ = nullptr;
    that.value_ = nullptr;
  }

  PinnedCacheSlot& operator=(PinnedCacheSlot&& that) noexcept
  {
    PinnedCacheSlot copy{std::move(that)};
    this->swap(copy);
    return *this;
  }

  ~PinnedCacheSlot() noexcept
  {
    this->reset();
  }

  void reset()
  {
    if (this->slot_) {
      this->slot_->release_pin();
      this->slot_ = nullptr;
      this->value_ = nullptr;
    }
  }

  void swap(PinnedCacheSlot& that)
  {
    std::swap(this->slot_, that.slot_);
    std::swap(this->value_, that.value_);
  }

  explicit operator bool() const
  {
    return this->slot_ != nullptr;
  }

  const K& key() const noexcept
  {
    return this->slot_->key();
  }

  CacheSlot<K, V>* slot() const noexcept
  {
    return this->slot_;
  }

  V* get() const noexcept
  {
    return this->value_;
  }

  V* operator->() const noexcept
  {
    return this->get();
  }

  V& operator*() const noexcept
  {
    return *this->get();
  }

  u32 pin_count() const noexcept
  {
    return this->slot_ ? this->slot_->pin_count() : 0;
  }

  u64 ref_count() const noexcept
  {
    return this->slot_ ? this->slot_->ref_count() : 0;
  }

 private:
  CacheSlot<K, V>* slot_ = nullptr;
  V* value_ = nullptr;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename K, typename V>
inline bool operator==(const PinnedCacheSlot<K, V>& l, const PinnedCacheSlot<K, V>& r)
{
  return l.slot() == r.slot() && l.get() == r.get();
}

}  // namespace llfs

#endif  // LLFS_CACHE_HPP
