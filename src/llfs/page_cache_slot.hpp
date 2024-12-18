//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_SLOT_HPP
#define LLFS_PAGE_CACHE_SLOT_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/lru_clock.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_view.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/latch.hpp>

#include <boost/intrusive_ptr.hpp>

#include <atomic>
#include <memory>

namespace llfs {

/** \brief A container for a single key/value pair in a PageDeviceCache.
 *
 *  PageCacheSlot objects are always in one of four states:
 *   - Invalid (initial)
 *   - Valid + Filled
 *   - Valid + Filled + Pinned
 *   - Valid + Cleared
 *
 */
class PageCacheSlot
{
 public:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // State Transition Diagram:
  //                                     ┌─────────┐
  //                               ┌─────│ Invalid │──────────┐
  //                        fill() │     └─────────┘          │ clear()
  //                               │          ▲               │
  //                               │          │               │
  //                               │          │evict()        │
  //                               ▼          │               ▼
  //                      ┌────────────────┐  │             ┌─────────────────┐
  //               ┌──────│ Valid + Filled │──┴─────────────│ Valid + Cleared │
  //               │      └────────────────┘                └─────────────────┘
  //               │               ▲              acquire_pin()  │       ▲
  // acquire_pin():│               │                             │       │
  //     0 -> 1    │               │release_pin():               │       │ release_pin()
  //               │               │    1 -> 0                   │       │
  //               │               │                             ▼       │
  //               │  ┌─────────────────────────┐       ┌──────────────────────────┐
  //               └─▶│ Valid + Filled + Pinned │       │ Valid + Cleared + Pinned │
  //                  └─────────────────────────┘       └──────────────────────────┘
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // State integer bit layout:
  //
  // ┌─────────────────┬────────────────────────────────────────────────────┬─┐
  // │Overflow (8 bits)│                Pin Count (47 bits)                 │ │
  // └─────────────────┴────────────────────────────────────────────────────┴─┘
  //                                                                         ▲
  //                                                    Valid? (1 bit)───────┘
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The byte offset of the Pin Count within the state integer.
   */
  static constexpr usize kPinCountShift = 1;

  /** \brief The number of unused (most-significant) bits; used to detect integer overflow.
   */
  static constexpr usize kOverflowBits = 8;

  /** \brief The amount to add/subtract to the state integer to increment or decrement the pin
   * count.
   */
  static constexpr u64 kPinCountDelta = u64{1} << kPinCountShift;

  /** \brief Used to detect pin count integer overflow; should always be zero.
   */
  static constexpr u64 kOverflowMask = ((u64{1} << kOverflowBits) - 1) << (64 - kOverflowBits);

  /** \brief The Valid bit.
   */
  static constexpr u64 kValidMask = 1;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Forward-declarations of member types.
  //
  class Pool;       // defined in <llfs/page_cache_slot_pool.hpp>
  class AtomicRef;  // defined in <llfs/page_cache_slot_atomic_ref.hpp>
  class PinnedRef;  // defined in <llfs/page_cache_slot_pinned_ref.hpp>

  using Self = PageCacheSlot;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the pin count bit field within the passed state integer.
   */
  static constexpr u64 get_pin_count(u64 state)
  {
    return state >> kPinCountShift;
  }

  /** \brief Returns true iff the pin count of `state` is non-zero, indicating the slot is in-use
   * and must not be evicted or modified.
   */
  static constexpr bool is_pinned(u64 state)
  {
    return Self::get_pin_count(state) != 0;
  }

  /** \brief Returns true iff the Valid? bit of `state` is set.
   */
  static constexpr bool is_valid(u64 state)
  {
    return (state & kValidMask) != 0;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheSlot(const PageCacheSlot&) = delete;
  PageCacheSlot& operator=(const PageCacheSlot&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new PageCacheSlot owned by the passed Pool.
   */
  explicit PageCacheSlot(Pool& pool) noexcept;

  /** \brief Destroys the cache slot; ref count and pin count MUST both be zero when the slot is
   * destroyed, or we will panic.
   */
  ~PageCacheSlot() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the PageCacheSlot::Pool containing this slot.
   */
  Pool& pool() const noexcept;

  /** \brief Returns the index of `this` within its pool.
   */
  usize index() const noexcept;

  /** \brief Returns the current key held in the slot, if valid; if the slot is invalid, the
   * returned value is undefined.
   */
  PageId key() const;

  /** Returns the current value held in the slot, if valid; if the slot is invalid, behavior is
   * undefined.
   *
   * Most callers of this function must not modify the returned Latch object.  There is one
   * exception to this rule: the caller who most recently called `fill` to transition the slot state
   * from 'Invalid' to 'Valid + Filled' is required to set the Latch value, which broadcasts to all
   * other observers of this slot that the page has been loaded.
   */
  batt::Latch<std::shared_ptr<const PageView>>* value() noexcept;

  /** \brief Returns true iff the slot is in a valid state.
   */
  bool is_valid() const noexcept;

  //----- --- -- -  -  -   -

  /** \brief Returns the current (weak/non-pinning) reference count.
   *
   * Do not confuse this with the pin count!  A non-zero ref count keeps the PageCacheSlot (and by
   * extension the pool that owns it) in scope, but it does not prevent the slot from being evicted
   * and refilled.  Think of this as a weak reference count.
   */
  u64 ref_count() const noexcept;

  /** \brief Adds a (weak/non-pinning) reference to the slot.
   *
   * Used to avoid premature destruction of the cache.
   */
  void add_ref() noexcept;

  /** \brief Removes a (weak/non-pinning) reference from the slot.
   *
   * Used to avoid premature destruction of the cache.
   */
  void remove_ref() noexcept;

  //----- --- -- -  -  -   -

  /** \brief Returns true iff the slot is in a pinned state.
   */
  bool is_pinned() const noexcept;

  /** \brief Returns the current pin count of the slot; if this is 0, the slot is not pinned.
   */
  u64 pin_count() const noexcept;

  /** \brief Conditionally pins the slot so it can't be evicted.
   *
   * This operation is conditioned on `key` matching the currently stored key in the slot.  If the
   * slot is in an invalid state or the key doesn't match, the operation will fail and an
   * empty/invalid value is returned.
   *
   * If ignore_key is true, then the pin will succeed if the slot is in a valid state, no matter
   * what the current key is.
   *
   * A slot is removed from the cache's LRU list when its pin count goes from 0 -> 1, and placed
   * back at the "most recently used" end of the LRU list when the pin count goes from 1 -> 0.
   */
  PinnedRef acquire_pin(PageId key, bool ignore_key = false) noexcept;

  /** \brief Called when creating a copy of PinnedCacheSlot, i.e. only when the pin count is going
   * from n -> n+1, where n > 0.
   */
  void extend_pin() noexcept;

  /** \brief Decreases the pin count by 1.
   *
   * If this unpins the slot, then we also remove a single weak ref.
   */
  void release_pin() noexcept;

  /** \brief If this slot is not pinned and it is not evicted, atomically increment the generation
   * counter and return true; else return false.
   *
   * If evict() succeeds (returns true), then the slot is in an "invalid" state.
   */
  bool evict() noexcept;

  /** \brief Evicts the slot iff it is evict-able and the current key matches the passed value.
   */
  bool evict_if_key_equals(PageId key) noexcept;

  /** \brief Resets the key and value for this slot.
   *
   * The generation counter must be odd (indicating the slot has been evicted) prior to calling this
   * function.
   *
   * May only be called when the slot is in an invalid state.
   */
  PinnedRef fill(PageId key) noexcept;

  /** \brief Sets the key and value of the slot to empty/null.
   *
   * This causes the slot to leave the invalid state, but all attempts to pin will fail until it is
   * evicted/filled.
   */
  void clear() noexcept;

  //----- --- -- -  -  -   -

  /** \brief Updates the latest use logical timestamp for this object, to make eviction less likely.
   *
   * Only has an effect if the "obsolete hint" (see set_obsolete_hint, get_obsolete_hint) is false.
   */
  void update_latest_use() noexcept;

  /** Give a hint to the cache that this slot is unlikely to be needed again in the future.
   *
   * This function sets the latest_use LTS to a very old value.
   */
  void set_obsolete_hint() noexcept;

  /** \brief Returns the current latest use logical timestamp.
   */
  i64 get_latest_use() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief The implementation of acquire_pin; returns true iff successful.
   */
  bool acquire_pin_impl(PageId key) noexcept;

  /** \brief Invoked when the ref count goes from 0 -> 1.
   */
  void notify_first_ref_acquired();

  /** \brief Invoked when the ref count goes from 1 -> 0.
   */
  void notify_last_ref_released();

  /** \brief Sets the valid bit; Panic if the previous state was not Invalid.
   */
  void set_valid();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Pool& pool_;
  PageId key_;
  Optional<batt::Latch<std::shared_ptr<const PageView>>> value_;
  std::atomic<u64> state_{0};
  std::atomic<u64> ref_count_{0};
  std::atomic<i64> latest_use_{0};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

namespace detail {

/** \brief Calls slot->add_ref() iff slot is not nullptr.
 */
inline PageCacheSlot* increment_weak_ref(PageCacheSlot* slot)
{
  if (slot) {
    slot->add_ref();
  }
  return slot;
}

/** \brief Calls slot->remove_ref() iff slot is not nullptr.
 */
inline void decrement_weak_ref(PageCacheSlot* slot)
{
  if (slot) {
    slot->remove_ref();
  }
}

}  //namespace detail

}  //namespace llfs

#include <llfs/page_cache_slot_pinned_ref.hpp>
//
#include <llfs/page_cache_slot_atomic_ref.hpp>
#include <llfs/page_cache_slot_pool.hpp>

#endif  // LLFS_PAGE_CACHE_SLOT_HPP
