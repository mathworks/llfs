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
  BATT_CHECK(!this->is_pinned());
  BATT_CHECK(!this->is_valid());
  BATT_CHECK_EQ(this->cache_slot_ref_count(), 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot::~PageCacheSlot() noexcept
{
  BATT_CHECK(!this->is_pinned()) << BATT_INSPECT(this->pin_count())
                                 << BATT_INSPECT(this->cache_slot_ref_count())
                                 << BATT_INSPECT((void*)this);
  BATT_CHECK_EQ(this->cache_slot_ref_count(), 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::index() const
{
  return this->pool_.index_of(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCacheSlot::fill(PageId key, PageSize page_size, i64 lru_priority, ExternalAllocation claim)
    -> PinnedRef
{
  BATT_CHECK(!this->is_valid());
  BATT_CHECK(key.is_valid());
  BATT_CHECK_EQ(std::addressof(claim.pool()), std::addressof(this->pool_));
  BATT_CHECK_EQ(claim.size(), page_size);

  this->key_ = key;
  this->value_.emplace();
  this->p_value_ = std::addressof(*this->value_);
  this->page_size_ = page_size;
  this->update_latest_use(lru_priority);

  auto observed_state = this->state_.fetch_add(kPinCountDelta) + kPinCountDelta;
  BATT_CHECK_EQ(observed_state & Self::kOverflowMask, 0);
  BATT_CHECK(Self::is_pinned(observed_state));

  this->pool_.metrics().admit_count.add(1);
  this->pool_.metrics().admit_byte_count.add(page_size);

  claim.absorb();

  this->add_ref();
  this->set_valid();

  return PinnedRef{this, CallerPromisesTheyAcquiredPinCount{}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::clear()
{
  BATT_CHECK(!this->is_valid());

  this->pool_.metrics().erase_count.add(1);
  this->pool_.metrics().erase_byte_count.add(this->page_size_);

  BATT_CHECK_EQ(this->value_, None);
  BATT_CHECK_EQ(this->p_value_, nullptr);
  BATT_CHECK_EQ(this->page_size_, PageSize{0});

  this->key_ = PageId{};
  this->latest_use_.store(0);
  this->set_valid();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::evict()
{
  // Use a CAS loop here to guarantee an atomic transition from Valid + Filled (unpinned) state to
  // Invalid.
  //
  auto observed_state = this->state_.load(std::memory_order_acquire);
  for (;;) {
    if (Self::is_pinned(observed_state) || !Self::is_valid(observed_state)) {
      return false;
    }

    // Clear the valid bit from the state mask.
    //
    const auto target_state = observed_state & ~kValidMask;
    if (this->state_.compare_exchange_weak(observed_state, target_state)) {
      this->on_evict_success(nullptr);
      return true;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::evict_if_key_equals(PageId key)
{
  static_assert(std::is_same_v<decltype(this->state_)::value_type, u64>);

  // The slot must be pinned in order to read the key, so increase the pin count.
  //
  const u64 old_state = this->state_.fetch_add(kPinCountDelta, std::memory_order_acquire);
  u64 observed_state = old_state + kPinCountDelta;

  const bool newly_pinned = !Self::is_pinned(old_state);
  if (newly_pinned) {
    this->add_ref();
  }

  BATT_CHECK_EQ(observed_state & Self::kOverflowMask, 0);

  // Use a CAS loop here to guarantee an atomic transition from Valid + Filled (unpinned) state to
  // Invalid.
  //
  for (;;) {
    // To succeed, we must be holding the only pin, the slot must be valid, and the key must match.
    //
    if (!(Self::get_pin_count(observed_state) == 1 && Self::is_valid(observed_state) &&
          this->key_ == key)) {
      this->release_pin();
      return false;
    }

    // Clear the valid bit from the state mask and release the pin count we acquired above.
    //
    u64 target_state = ((observed_state - kPinCountDelta) & ~kValidMask);

    BATT_CHECK(!Self::is_pinned(target_state) && !Self::is_valid(target_state))
        << BATT_INSPECT(target_state);

    if (this->state_.compare_exchange_weak(observed_state, target_state)) {
      this->on_evict_success(nullptr);

      // At this point, we always expect to be going from pinned to unpinned.
      // In order to successfully evict the slot, we must be holding the only pin,
      // as guarenteed by the first if statement in the for loop.
      //
      this->remove_ref();
      return true;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::evict_and_release_pin(ExternalAllocation* reclaim)
{
  static_assert(std::is_same_v<decltype(this->state_)::value_type, u64>);

  u64 observed_state = this->state_.load(std::memory_order_acquire);

  BATT_CHECK(Self::is_pinned(observed_state));
  BATT_CHECK_EQ(observed_state & Self::kOverflowMask, 0);

  // Use a CAS loop here to guarantee an atomic transition from Valid + Filled (unpinned) state to
  // Invalid.
  //
  for (;;) {
    // To succeed, we must be holding the only pin and the slot must be valid.
    //
    if (!(Self::get_pin_count(observed_state) == 1 && Self::is_valid(observed_state))) {
      this->release_pin();
      return false;
    }

    // Clear the valid bit from the state mask and release the pin count we acquired above.
    //
    u64 target_state = ((observed_state - kPinCountDelta) & ~kValidMask);

    BATT_CHECK(!Self::is_pinned(target_state) && !Self::is_valid(target_state))
        << BATT_INSPECT(target_state);

    if (this->state_.compare_exchange_weak(observed_state, target_state)) {
      this->on_evict_success(reclaim);

      // At this point, we always expect to be going from pinned to unpinned.
      // In order to successfully evict the slot, we must be holding the only pin,
      // as guarenteed by the first if statement in the for loop.
      //
      this->remove_ref();
      return true;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::on_evict_success(ExternalAllocation* reclaim)
{
  BATT_CHECK(!this->is_valid());

  this->pool_.metrics().evict_count.add(1);
  this->pool_.metrics().evict_byte_count.add(this->page_size_);

  if (reclaim != nullptr) {
    *reclaim = ExternalAllocation{this->pool_, this->page_size_};
  } else {
    this->pool_.resident_size_.fetch_sub(this->page_size_);
  }

  this->value_ = None;
  this->p_value_ = nullptr;
  this->page_size_ = PageSize{0};
}

#if LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT

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

#endif  // LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::ExternalAllocation::release() noexcept
{
  if (this->pool_) {
    const i64 prior_resident_size = this->pool_->resident_size_.fetch_sub(this->size_);
    BATT_CHECK_GE(prior_resident_size, (i64)this->size_);
    this->pool_ = nullptr;
    this->size_ = 0;
  }
}

}  //namespace llfs
