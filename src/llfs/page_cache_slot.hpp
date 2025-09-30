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
#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_view.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/latch.hpp>

#include <boost/intrusive_ptr.hpp>

#include <atomic>
#include <memory>

namespace llfs {

#ifndef LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT
#define LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT 0
#endif

#ifndef LLFS_PAGE_CACHE_SLOT_ENABLE_ASSERTS
#define LLFS_PAGE_CACHE_SLOT_ENABLE_ASSERTS 0
#endif

#if LLFS_PAGE_CACHE_SLOT_ENABLE_ASSERTS
//----- --- -- -  -  -   -
#define LLFS_PAGE_CACHE_ASSERT BATT_CHECK
#define LLFS_PAGE_CACHE_ASSERT_EQ BATT_CHECK_EQ
#define LLFS_PAGE_CACHE_ASSERT_NE BATT_CHECK_NE
#define LLFS_PAGE_CACHE_ASSERT_GT BATT_CHECK_GT
//----- --- -- -  -  -   -
#else  // LLFS_PAGE_CACHE_SLOT_ENABLE_CHECKS
//----- --- -- -  -  -   -
#define LLFS_PAGE_CACHE_ASSERT BATT_ASSERT
#define LLFS_PAGE_CACHE_ASSERT_EQ BATT_ASSERT_EQ
#define LLFS_PAGE_CACHE_ASSERT_NE BATT_ASSERT_NE
#define LLFS_PAGE_CACHE_ASSERT_GT BATT_ASSERT_GT
//----- --- -- -  -  -   -
#endif  //LLFS_PAGE_CACHE_SLOT_ENABLE_CHECKS

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
  //                               │          │ evict()       │
  //                               ▼          │               ▼
  //                      ┌────────────────┐  │             ┌─────────────────┐
  //               ┌──────│ Valid + Filled │──┴─────────────│ Valid + Cleared │
  //               │      └────────────────┘                └─────────────────┘
  //               │               ▲               acquire_pin() │       ▲
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
  static constexpr u64 kValidMask = 0b1;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Forward-declarations of member types.
  //
  class Pool;       // defined in <llfs/page_cache_slot_pool.hpp>
  class AtomicRef;  // defined in <llfs/page_cache_slot_atomic_ref.hpp>
  class PinnedRef;  // defined in <llfs/page_cache_slot_pinned_ref.hpp>

  class ExternalAllocation
  {
   public:
    friend class Pool;
    friend class PageCacheSlot;

    ExternalAllocation() noexcept : pool_{nullptr}, size_{0}
    {
    }

    ExternalAllocation(const ExternalAllocation&) = delete;
    ExternalAllocation& operator=(const ExternalAllocation&) = delete;

    ExternalAllocation(ExternalAllocation&& that) noexcept
        : pool_{std::move(that.pool_)}
        , size_{that.size_}
    {
      that.pool_ = nullptr;
      that.size_ = 0;
    }

    ExternalAllocation& operator=(ExternalAllocation&& that) noexcept
    {
      if (this != &that) {
        this->release();

        this->pool_ = std::move(that.pool_);
        this->size_ = that.size_;

        that.pool_ = nullptr;
        that.size_ = 0;
      }
      return *this;
    }

    ~ExternalAllocation() noexcept
    {
      this->release();
    }

    void release() noexcept;

    Pool& pool() const
    {
      return *this->pool_;
    }

    usize size() const
    {
      return this->size_;
    }

    explicit operator bool() const noexcept
    {
      return this->pool_ && this->size_ != 0;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    ExternalAllocation(Pool& pool, usize size) noexcept : pool_{&pool}, size_{size}
    {
    }

    /** \brief May only be called by PageCacheSlot::fill; releases the claim without changing the
     * pool_'s resident_size.
     */
    void absorb()
    {
      this->pool_ = nullptr;
      this->size_ = 0;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    boost::intrusive_ptr<Pool> pool_;
    usize size_;
  };

  using Self = PageCacheSlot;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the pin count bit field within the passed state integer.
   */
  BATT_ALWAYS_INLINE static constexpr u64 get_pin_count(u64 state)
  {
    return state >> kPinCountShift;
  }

  /** \brief Returns true iff the pin count of `state` is non-zero, indicating the slot is in-use
   * and must not be evicted or modified.
   */
  BATT_ALWAYS_INLINE static constexpr bool is_pinned(u64 state)
  {
    return Self::get_pin_count(state) != 0;
  }

  /** \brief Returns true iff the Valid? bit of `state` is set.
   */
  BATT_ALWAYS_INLINE static constexpr bool is_valid(u64 state)
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
  Pool& pool() const;

  /** \brief Returns the index of `this` within its pool.
   */
  usize index() const;

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
  batt::Latch<std::shared_ptr<const PageView>>* value();

  /** \brief Returns true iff the slot is in a valid state.
   */
  bool is_valid() const;

  //----- --- -- -  -  -   -

  /** \brief Returns the current (weak/non-pinning) reference count.
   *
   * Do not confuse this with the pin count!  A non-zero ref count keeps the PageCacheSlot (and by
   * extension the pool that owns it) in scope, but it does not prevent the slot from being evicted
   * and refilled.  Think of this as a weak reference count.
   */
  u64 cache_slot_ref_count() const;

  /** \brief Adds a (weak/non-pinning) reference to the slot.
   *
   * Used to avoid premature destruction of the cache.
   */
  void add_ref();

  /** \brief Removes a (weak/non-pinning) reference from the slot.
   *
   * Used to avoid premature destruction of the cache.
   */
  void remove_ref();

  //----- --- -- -  -  -   -

  /** \brief Returns true iff the slot is in a pinned state.
   */
  bool is_pinned() const;

  /** \brief Returns the current pin count of the slot; if this is 0, the slot is not pinned.
   */
  u64 pin_count() const;

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
  PinnedRef acquire_pin(PageId key, IgnoreKey ignore_key = IgnoreKey{false},
                        IgnoreGeneration ignore_generation = IgnoreGeneration{false});

  /** \brief Called when creating a copy of PinnedCacheSlot, i.e. only when the pin count is going
   * from n -> n+1, where n > 0.
   */
  void extend_pin();

  /** \brief Decreases the pin count by 1.
   *
   * If this unpins the slot, then we also remove a single weak ref.
   */
  void release_pin();

  /** \brief If this slot is not pinned and it is not evicted, atomically increment the generation
   * counter and return true; else return false.
   *
   * If evict() succeeds (returns true), then the slot is in an "invalid" state.
   */
  bool evict();

  /** \brief Evicts the slot iff it is evict-able and the current key matches the passed value.
   */
  bool evict_if_key_equals(PageId key);

  /** \brief Attempts to evict the slot; the caller must be holding a pin.  Ownership of the
   * caller's pin is transferred to this function; the pin is released regardless of whether the
   * eviction succeeded.
   */
  bool evict_and_release_pin(ExternalAllocation* reclaim);

  /** \brief Resets the key and value for this slot.
   *
   * The generation counter must be odd (indicating the slot has been evicted) prior to calling this
   * function.
   *
   * May only be called when the slot is in an invalid state.
   */
  PinnedRef fill(PageId key, PageSize page_size, i64 lru_priority, ExternalAllocation claim);

  /** \brief Sets the key and value of the slot to empty/null.
   *
   * This causes the slot to leave the invalid state, but all attempts to pin will fail until it is
   * evicted/filled.
   */
  void clear();

  //----- --- -- -  -  -   -

  /** \brief Updates the latest use logical timestamp for this object, to make eviction less likely.
   */
  void update_latest_use(i64 lru_priority);

  /** Give a hint to the cache that this slot is unlikely to be needed again in the future.
   *
   * This function sets the latest_use LTS to a very old value.
   */
  void set_obsolete_hint();

  /** \brief If the latest use counter for this slot non-positive, returns true (indicating this
   * slot is ready to be evicted); otherwise, decrement the use counter and return false.
   */
  bool expire();

  /** \brief Returns the current latest use logical timestamp.
   */
  i64 get_latest_use() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief The implementation of acquire_pin; returns true iff successful.
   */
  bool acquire_pin_impl(PageId key);

#if LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT

  /** \brief Invoked when the ref count goes from 0 -> 1.
   */
  void notify_first_ref_acquired();

  /** \brief Invoked when the ref count goes from 1 -> 0.
   */
  void notify_last_ref_released();

#endif  // LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT

  /** \brief Sets the valid bit; Panic if the previous state was not Invalid.
   */
  void set_valid();

  /** \brief Called when this slot is successfully evicted.
   */
  void on_evict_success(ExternalAllocation* reclaim);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Pool& pool_;
  PageId key_;
  Optional<batt::Latch<std::shared_ptr<const PageView>>> value_;
  batt::Latch<std::shared_ptr<const PageView>>* p_value_ = nullptr;
  std::atomic<u64> state_{0};
#if LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT
  std::atomic<u64> ref_count_{0};
#endif
  std::atomic<i64> latest_use_{0};
  PageSize page_size_{0};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PageCacheSlot::Pool& PageCacheSlot::pool() const
{
  return this->pool_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline PageId PageCacheSlot::key() const
{
  return this->key_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline batt::Latch<std::shared_ptr<const PageView>>* PageCacheSlot::value()
{
  return this->p_value_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline bool PageCacheSlot::is_valid() const
{
  return Self::is_valid(this->state_.load(std::memory_order_acquire));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline bool PageCacheSlot::is_pinned() const
{
  return Self::is_pinned(this->state_.load(std::memory_order_acquire));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline u64 PageCacheSlot::pin_count() const
{
  return Self::get_pin_count(this->state_.load(std::memory_order_acquire));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline u64 PageCacheSlot::cache_slot_ref_count() const
{
#if LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT
  return this->ref_count_.load(std::memory_order_acquire);
#else
  return 0;
#endif
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline void PageCacheSlot::add_ref()
{
#if LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT
  [[maybe_unused]] const auto observed_count =
      this->ref_count_.fetch_add(1, std::memory_order_relaxed);

  if (observed_count == 0) {
    this->notify_first_ref_acquired();
  }
#endif  // LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline void PageCacheSlot::remove_ref()
{
#if LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT
  const auto observed_count = this->ref_count_.fetch_sub(1, std::memory_order_release);
  LLFS_PAGE_CACHE_ASSERT_GT(observed_count, 0);

  if (observed_count == 1) {
    (void)this->ref_count_.load(std::memory_order_acquire);

    this->notify_last_ref_released();
  }
#endif  // LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline void PageCacheSlot::extend_pin()
{
  const auto old_state = this->state_.fetch_add(kPinCountDelta, std::memory_order_relaxed);
  const auto new_state = old_state + kPinCountDelta;

  LLFS_PAGE_CACHE_ASSERT_EQ(new_state & Self::kOverflowMask, 0);

  LLFS_PAGE_CACHE_ASSERT(Self::is_pinned(old_state))
      << "This method should never be called in cases where the current pin count might be 0; "
         "use acquire_pin() instead.";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline void PageCacheSlot::update_latest_use(i64 lru_priority)
{
  const i64 desired = std::max<i64>(lru_priority, 1);
  const i64 observed = this->latest_use_.load();
  if (observed != desired) {
    this->latest_use_.store(desired);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline void PageCacheSlot::set_obsolete_hint()
{
  this->latest_use_.store(0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline bool PageCacheSlot::expire()
{
  const i64 observed = this->latest_use_.fetch_sub(1);
  return observed <= 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline i64 PageCacheSlot::get_latest_use() const
{
  return this->latest_use_.load();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline void PageCacheSlot::release_pin()
{
  const auto old_state = this->state_.fetch_sub(kPinCountDelta, std::memory_order_release);

  LLFS_PAGE_CACHE_ASSERT(Self::is_pinned(old_state))
      << "Each call to release_pin should have a previous call to "
         "acquire_pin, so we should always observe a prior pinned state. "
      << BATT_INSPECT(old_state);

  const auto new_state = old_state - kPinCountDelta;
  const bool newly_unpinned = !Self::is_pinned(new_state);

  LLFS_PAGE_CACHE_ASSERT_EQ(new_state & Self::kOverflowMask, 0);

  if (newly_unpinned) {
    // Load the state with `acquire` order to create a full memory barrier.
    //
    (void)this->state_.load(std::memory_order_acquire);

    this->remove_ref();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline void PageCacheSlot::set_valid()
{
  const auto observed_state = this->state_.fetch_or(kValidMask, std::memory_order_release);
  LLFS_PAGE_CACHE_ASSERT(!Self::is_valid(observed_state))
      << "Must go from an invalid state to valid!";
}

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

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline auto PageCacheSlot::acquire_pin(PageId key, IgnoreKey ignore_key,
                                                          IgnoreGeneration ignore_generation)
    -> PinnedRef
{
  const auto old_state = this->state_.fetch_add(kPinCountDelta, std::memory_order_acquire);
  const auto new_state = old_state + kPinCountDelta;
  const bool newly_pinned = !Self::is_pinned(old_state);

  LLFS_PAGE_CACHE_ASSERT_EQ(new_state & Self::kOverflowMask, 0);
  LLFS_PAGE_CACHE_ASSERT(Self::is_pinned(new_state));

  BATT_SUPPRESS_IF_GCC("-Wmaybe-uninitialized")

  // We must always do this, even if the pin fails, so that we don't have an unmatched
  // `remove_ref` in `release_pin` below.
  //
  if (newly_pinned) {
    this->add_ref();
  }

  // If the pin_count > 1 (because of the fetch_add above) and the slot is valid, it is safe to read
  // the key.  If the key doesn't match, release the ref and return failure.
  //
  if (!Self::is_valid(old_state) ||
      (!ignore_key && (!this->key_.is_valid() ||
                       !((!ignore_generation && this->key_ == key) ||
                         (ignore_generation && is_same_physical_page(this->key_, key)))))) {
    this->release_pin();
    return PinnedRef{};
  }

  BATT_UNSUPPRESS_IF_GCC()

  // If we aren't ignoring the slot's key and are looking to use the slot's value,
  // make sure that the value is in a valid state before creating a PinnedRef.
  //
  if (!ignore_key) {
    LLFS_PAGE_CACHE_ASSERT(this->value_);
  }

  return PinnedRef{this, CallerPromisesTheyAcquiredPinCount{}};
}

}  //namespace llfs

#endif  // LLFS_PAGE_CACHE_SLOT_HPP
