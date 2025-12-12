//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_device_cache.hpp>
//

#include <llfs/optional.hpp>

#include <batteries/env.hpp>

namespace llfs {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct NewSlot {
  PageCacheSlot* p_slot = nullptr;
  PageCacheSlot::PinnedRef pinned_ref;
};

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageDeviceCache::PageDeviceCache(
    const PageIdFactory& page_ids, boost::intrusive_ptr<PageCacheSlot::Pool>&& slot_pool) noexcept
    : page_ids_{page_ids}
    , slot_pool_{std::move(slot_pool)}
    , cache_(this->page_ids_.get_physical_page_count(), nullptr)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageDeviceCache::~PageDeviceCache() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageIdFactory& PageDeviceCache::page_ids() const noexcept
{
  return this->page_ids_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<PageCacheSlot::PinnedRef> PageDeviceCache::find_or_insert(
    PageId key, PageSize page_size, LruPriority lru_priority, PageCacheOvercommit& overcommit,
    const batt::SmallFn<void(const PageCacheSlot::PinnedRef&)>& initialize)
{
  static const bool kEvictOldGenSlot =
      batt::getenv_as<bool>("LLFS_EVICT_OLD_GEN_SLOT").value_or(true);

  LLFS_PAGE_CACHE_ASSERT_EQ(PageIdFactory::get_device_id(key), this->page_ids_.get_device_id());
  this->metrics().query_count.add(1);

  // Lookup the cache table entry for the given page id.
  //
  const i64 physical_page = this->page_ids_.get_physical_page(key);
  std::atomic<PageCacheSlot*>& slot_ptr_ref = this->get_slot_ptr_ref(physical_page);

  // Initialized lazily (at most once) below, only when we discover we might
  // need them.
  //
  Optional<NewSlot> new_slot;

  // Let's take a look at what's there now...
  //
  PageCacheSlot* observed_slot_ptr = slot_ptr_ref.load();

  for (;;) {
    // If the current index is invalid, then there's no point trying to pin it, so check that
    // first.
    //
    if (observed_slot_ptr != nullptr) {
      // If the CAS at the end of this loop failed spuriously, we might end up here...
      //
      if (new_slot && observed_slot_ptr == new_slot->p_slot) {
        break;
      }

      // Looks like there is already a slot for this physical page... let's try to pin it to see
      // if it still contains the desired page.
      //
      PageCacheSlot* slot = observed_slot_ptr;
      PageCacheSlot::PinnedRef pinned =
          slot->acquire_pin(key, IgnoreKey{false}, IgnoreGeneration{kEvictOldGenSlot});
      if (pinned) {
        if (new_slot) {
          LLFS_PAGE_CACHE_ASSERT(new_slot->pinned_ref);
          LLFS_PAGE_CACHE_ASSERT_NE(slot->value(), new_slot->pinned_ref.value());

          // [tastolfi 2024-02-09] I can't think of a reason why the new_value would ever be visible
          // to anyone if we go down this code path, but just in case, resolve the Latch with a
          // unique status code so that we don't get hangs waiting for pages to load.
          //
          new_slot->pinned_ref.value()->set_error(
              ::llfs::make_status(StatusCode::kPageCacheSlotNotInitialized));
        }

        // If the pinned slot is an exact match, then return with success.
        //
        if (!kEvictOldGenSlot || pinned.key() == key) {
          // Refresh the LTS.
          //
          slot->update_latest_use(lru_priority);

          // Done! (Found existing value)
          //
          this->metrics().hit_count.add(1);
          return {std::move(pinned)};
        }

        // It is an old generation of the same page!  Attempt to evict and reuse this slot.
        //
        BATT_CHECK(is_same_physical_page(pinned.key(), key));
        pinned.release_ownership_of_pin();

        PageCacheSlot::ExternalAllocation claim;
        if (slot->evict_and_release_pin(&claim)) {
          BATT_CHECK_EQ(claim.size(), page_size);

          this->metrics().evict_prior_generation_count.add(1);
          new_slot.emplace();
          new_slot->p_slot = slot;
          new_slot->pinned_ref = slot->fill(key, page_size, lru_priority, std::move(claim));

          LLFS_PAGE_CACHE_ASSERT_EQ(new_slot->p_slot, observed_slot_ptr);
          break;
        }
        //
        // Eviction failed; we need to treat this branch and what follows *as if* !pinned.
      }
      this->metrics().stale_count.add(1);
    }

    // No existing value found, or pin failed; allocate a new slot, fill it, and attempt to CAS it
    // into the cache array.
    //
    if (!new_slot) {
      PageCacheSlot::ExternalAllocation claim;

      new_slot.emplace();
      std::tie(new_slot->p_slot, claim) = this->slot_pool_->allocate(page_size, overcommit);
      if (!new_slot->p_slot) {
        this->metrics().full_count.add(1);
        return ::llfs::make_status(StatusCode::kCacheSlotsFull);
      }
      LLFS_PAGE_CACHE_ASSERT(!new_slot->p_slot->is_valid());

      new_slot->pinned_ref = new_slot->p_slot->fill(key, page_size, lru_priority, std::move(claim));
    }

    // If we can atomically overwrite the slot index value we saw above (CAS), then we are done!
    //
    if (slot_ptr_ref.compare_exchange_weak(observed_slot_ptr, new_slot->p_slot)) {
      break;
    }
  }

  LLFS_PAGE_CACHE_ASSERT(new_slot);

  // We purposely delayed this step until we knew that this thread must initialize the cache
  // slot's Latch.  This function will probably start I/O (or do something else in a test case...)
  //
  initialize(new_slot->pinned_ref);

  // Done! (Admitted new value)
  //
  return {std::move(new_slot->pinned_ref)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<PageCacheSlot::PinnedRef> PageDeviceCache::try_find(PageId key,
                                                                   LruPriority lru_priority)
{
  LLFS_PAGE_CACHE_ASSERT_EQ(PageIdFactory::get_device_id(key), this->page_ids_.get_device_id());
  this->metrics().query_count.add(1);

  // Lookup the cache table entry for the given page id.
  //
  const i64 physical_page = this->page_ids_.get_physical_page(key);
  std::atomic<PageCacheSlot*>& slot_ptr_ref = this->get_slot_ptr_ref(physical_page);

  // Let's take a look at what's there now...
  //
  PageCacheSlot* observed_slot_ptr = slot_ptr_ref.load();

  // There is a slot assigned to this physical page, so try to pin it.
  //
  if (observed_slot_ptr != nullptr) {
    PageCacheSlot::PinnedRef pinned = observed_slot_ptr->acquire_pin(key);
    if (pinned) {
      observed_slot_ptr->update_latest_use(lru_priority);
      this->metrics().hit_count.add(1);
      return {std::move(pinned)};
    }
    this->metrics().stale_count.add(1);
  }

  return {batt::StatusCode::kUnavailable};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageDeviceCache::erase(PageId key)
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(key), this->page_ids_.get_device_id());

  // Lookup the cache table entry for the given page id.
  //
  const i64 physical_page = this->page_ids_.get_physical_page(key);
  std::atomic<PageCacheSlot*>& slot_ptr_ref = this->get_slot_ptr_ref(physical_page);

  PageCacheSlot* slot_ptr = slot_ptr_ref.load();
  if (slot_ptr == nullptr) {
    return;
  }

  // Helper function.
  //
  const auto invalidate_ref = [&] {
    const PageCacheSlot* slot_ptr_to_erase = slot_ptr;
    do {
      if (slot_ptr_ref.compare_exchange_weak(slot_ptr, nullptr)) {
        break;
      }
    } while (slot_ptr == slot_ptr_to_erase);
  };

  // If the slot is still holding the passed id, then clear it out.
  //
  PageCacheSlot* slot = slot_ptr;
  if (slot->evict_if_key_equals(key)) {
    invalidate_ref();

    // Important!  Only clear the slot once we have invalidated our table entry.
    //
    slot->clear();
    if (!this->slot_pool_->push_free_slot(slot)) {
      slot->set_obsolete_hint();
    }

  } else {
    // If we weren't able to evict `key`, we can still try to read the slot to see if it contains
    // `key` but is non-evictable (because there are outstanding pins); if this is the case, then
    // clear it from our table.
    //
    PageCacheSlot::PinnedRef pinned = slot->acquire_pin(PageId{}, IgnoreKey{true});
    if (pinned) {
      const PageId observed_key = pinned.key();
      if (!observed_key.is_valid() || observed_key == key ||
          PageIdFactory::get_device_id(observed_key) != this->page_ids_.get_device_id() ||
          this->page_ids_.get_physical_page(observed_key) != physical_page) {
        invalidate_ref();
        if (observed_key == key) {
          if (!this->slot_pool_->push_free_slot(slot)) {
            slot->set_obsolete_hint();
          }
        }
      } else {
        // The table contains an older or newer generation of the same physical page; leave it
        // alone!
        //
        BATT_CHECK_EQ(this->page_ids_.get_physical_page(observed_key), physical_page)
            << BATT_INSPECT(key) << BATT_INSPECT(observed_key);
      }
    }
  }
}

}  //namespace llfs
