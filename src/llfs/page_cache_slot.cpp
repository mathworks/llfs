//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_slot.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageCacheSlot::PageCacheSlot(Pool& pool) noexcept : pool_{pool}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot::Pool& PageCacheSlot::pool() const noexcept
{
  return this->pool_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::index() const noexcept
{
  return this->pool_.index_of(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageId PageCacheSlot::key() const
{
  return this->key_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Latch<std::shared_ptr<const PageView>>* PageCacheSlot::value() const noexcept
{
  return this->value_.get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::is_valid() const noexcept
{
  return Self::is_valid(this->state_.load(std::memory_order_acquire));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::is_pinned() const noexcept
{
  return Self::is_pinned(this->state_.load(std::memory_order_acquire));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u32 PageCacheSlot::pin_count() const noexcept
{
  return Self::get_pin_count(this->state_.load(std::memory_order_acquire));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 PageCacheSlot::ref_count() const noexcept
{
  return this->ref_count_.load(std::memory_order_acquire);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::add_ref() noexcept
{
  const auto observed_count = this->ref_count_.fetch_add(1, std::memory_order_relaxed);
  if (observed_count == 0) {
    this->notify_first_ref_acquired();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::remove_ref() noexcept
{
  const auto observed_count = this->ref_count_.fetch_sub(1, std::memory_order_release);
  BATT_CHECK_GT(observed_count, 0);
  if (observed_count == 1) {
    (void)this->ref_count_.load(std::memory_order_acquire);
    this->notify_last_ref_released();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCacheSlot::acquire_pin(PageId key, bool ignore_key) noexcept -> PinnedRef
{
  const auto observed_state = this->state_.fetch_add(kIncreasePinDelta, std::memory_order_acquire);
  const auto new_state = observed_state + kIncreasePinDelta;
  const bool newly_pinned = !this->is_pinned(observed_state);

  if (new_state & kIncreasePinOverflow) {
    this->state_.fetch_and(~kIncreasePinOverflow);
  }

  BATT_CHECK(this->is_pinned(new_state));

  BATT_SUPPRESS_IF_GCC("-Wmaybe-uninitialized")

  // We must always do this, even if the pin fails, so that we don't have an unmatched
  // `remove_ref` in `release_pin` below.
  //
  if (newly_pinned) {
    this->add_ref();
  }

  // If the pin_count > 1 (because of the fetch_add above) and the slot is valid (because
  // generation is not odd), it is safe to read the key.  If the key doesn't match, release the ref
  // and return failure.
  //
  if (!this->is_valid(observed_state) ||
      (!ignore_key && (!this->key_.is_valid() || this->key_ != key))) {
    this->release_pin();
    return PinnedRef{};
  }

  BATT_UNSUPPRESS_IF_GCC()

  return PinnedRef{this};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::extend_pin() noexcept
{
  const auto observed_state = this->state_.fetch_add(kIncreasePinDelta, std::memory_order_relaxed);
  const auto new_state = observed_state + kIncreasePinDelta;

  if (new_state & kIncreasePinOverflow) {
    this->state_.fetch_and(~kIncreasePinOverflow);
  }

  BATT_CHECK(this->is_pinned(observed_state))
      << "This method should never be called in cases where the current pin count might be 0; "
         "use acquire_pin() instead.";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::update_latest_use() noexcept
{
  this->latest_use_.store(LRUClock::advance_local());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::set_obsolete_hint() noexcept
{
  this->latest_use_.store(LRUClock::read_global() - (i64{1} << 56));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 PageCacheSlot::get_latest_use() const noexcept
{
  return this->latest_use_.load();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::release_pin() noexcept
{
  const auto observed_state = this->state_.fetch_add(kDecreasePinDelta, std::memory_order_release);

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

    this->remove_ref();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::evict() noexcept
{
  // Use a CAS loop here to guarantee an atomic transition from Valid + Filled (unpinned) state to
  // Invalid.
  //
  auto observed_state = this->state_.load(std::memory_order_acquire);
  for (;;) {
    if (Self::is_pinned(observed_state) || !this->is_valid(observed_state)) {
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::evict_if_key_equals(PageId key) noexcept
{
  // The slot must be pinned in order to read the key, so increase the pin count.
  //
  const auto prior_state = this->state_.fetch_add(kIncreasePinDelta, std::memory_order_acquire);
  auto observed_state = prior_state + kIncreasePinDelta;

  if (observed_state & kIncreasePinOverflow) {
    observed_state = this->state_.fetch_and(~kIncreasePinOverflow) & ~kIncreasePinOverflow;
  }

  // Use a CAS loop here to guarantee an atomic transition from Valid + Filled (unpinned) state to
  // Invalid.
  //
  for (;;) {
    // To succeed, we must be holding the only pin, the slot must be valid, and the key must match.
    //
    if (!(Self::get_pin_count(observed_state) == 1 && this->is_valid(observed_state) &&
          this->key_ == key)) {
      const auto post_release_state =
          this->state_.fetch_add(kDecreasePinDelta, std::memory_order_release) + kDecreasePinDelta;
      if (post_release_state & kDecreasePinOverflow) {
        this->state_.fetch_and(~kDecreasePinOverflow);
      }
      return false;
    }

    // Clear the valid bit from the state mask and release the pin count we acquired above.
    //
    auto target_state = (observed_state & ~kValidMask) + kDecreasePinDelta;
    if (target_state & kDecreasePinOverflow) {
      target_state &= ~kDecreasePinOverflow;
    }
    BATT_CHECK(!Self::is_pinned(target_state));

    if (this->state_.compare_exchange_weak(observed_state, target_state)) {
      BATT_CHECK(!this->is_valid());
      return true;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCacheSlot::fill(
    PageId key, boost::intrusive_ptr<batt::Latch<std::shared_ptr<const PageView>>>&& value) noexcept
    -> PinnedRef
{
  BATT_CHECK(!this->is_valid());
  BATT_CHECK(key.is_valid());

  this->key_ = key;
  this->value_ = std::move(value);
  this->update_latest_use();

  auto observed_state = this->state_.fetch_add(kIncreasePinDelta) + kIncreasePinDelta;
  if (observed_state & kIncreasePinOverflow) {
    observed_state = this->state_.fetch_and(~kIncreasePinOverflow) & ~kIncreasePinOverflow;
  }
  BATT_CHECK(Self::is_pinned(observed_state));

  this->add_ref();
  this->set_valid();

  return PinnedRef{this};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::clear() noexcept
{
  BATT_CHECK(!this->is_valid());

  this->key_ = PageId{};
  this->value_ = nullptr;
  this->set_valid();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::notify_first_ref_acquired()
{
  intrusive_ptr_add_ref(std::addressof(this->pool_));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::notify_last_ref_released()
{
  intrusive_ptr_release(std::addressof(this->pool_));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::set_valid()
{
  const auto observed_state = this->state_.fetch_or(kValidMask, std::memory_order_release);
  BATT_CHECK(!this->is_valid(observed_state)) << "Must go from an invalid state to valid!";
}

}  //namespace llfs
