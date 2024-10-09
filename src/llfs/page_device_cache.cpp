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

namespace llfs {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct NewSlot {
  PageCacheSlot* p_slot = nullptr;
  PageCacheSlot::PinnedRef pinned_ref;
  usize slot_index = PageDeviceCache::kInvalidIndex;
};

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageDeviceCache::PageDeviceCache(
    const PageIdFactory& page_ids, boost::intrusive_ptr<PageCacheSlot::Pool>&& slot_pool) noexcept
    : page_ids_{page_ids}
    , slot_pool_{std::move(slot_pool)}
    , cache_(this->page_ids_.get_physical_page_count(), kInvalidIndex)
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
    PageId key, const std::function<void(const PageCacheSlot::PinnedRef&)>& initialize)
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(key), this->page_ids_.get_device_id());
  this->metrics().query_count.fetch_add(1);

  // Lookup the cache table entry for the given page id.
  //
  const i64 physical_page = this->page_ids_.get_physical_page(key);
  std::atomic<usize>& slot_index_ref = this->get_slot_index_ref(physical_page);

  // Initialized lazily (at most once) below, only when we discover we might
  // need them.
  //
  Optional<NewSlot> new_slot;

  // Let's take a look at what's there now...
  //
  usize observed_slot_index = slot_index_ref.load();

  for (;;) {
    // If the current index is invalid, then there's no point trying to pin it, so check that
    // first.
    //
    if (observed_slot_index != kInvalidIndex) {
      // If the CAS at the end of this loop failed spuriously, we might end up here...
      //
      if (new_slot && observed_slot_index == new_slot->slot_index) {
        break;
      }

      // Looks like there is already a slot for this physical page... let's try to pin it to see
      // if it still contains the desired page.
      //
      PageCacheSlot* slot = this->slot_pool_->get_slot(observed_slot_index);
      PageCacheSlot::PinnedRef pinned = slot->acquire_pin(key);
      if (pinned) {
        if (new_slot) {
          BATT_CHECK(new_slot->pinned_ref);
          BATT_CHECK_NE(slot->value(), new_slot->pinned_ref.value());

          // [tastolfi 2024-02-09] I can't think of a reason why the new_value would ever be visible
          // to anyone if we go down this code path, but just in case, resolve the Latch with a
          // unique status code so that we don't get hangs waiting for pages to load.
          //
          new_slot->pinned_ref.value()->set_error(
              ::llfs::make_status(StatusCode::kPageCacheSlotNotInitialized));
        }

        // Refresh the LTS.
        //
        slot->update_latest_use();

        // Done! (Found existing value)
        //
        this->metrics().hit_count.fetch_add(1);
        return {std::move(pinned)};
      }
      this->metrics().stale_count.fetch_add(1);
    }

    // No existing value found, or pin failed; allocate a new slot, fill it, and attempt to CAS it
    // into the cache array.
    //
    if (!new_slot) {
      new_slot.emplace();
      new_slot->p_slot = this->slot_pool_->allocate();
      if (!new_slot->p_slot) {
        this->metrics().full_count.fetch_add(1);
        return ::llfs::make_status(StatusCode::kCacheSlotsFull);
      }
      BATT_CHECK(!new_slot->p_slot->is_valid());

      new_slot->pinned_ref = new_slot->p_slot->fill(key);
      new_slot->slot_index = new_slot->p_slot->index();

      BATT_CHECK_EQ(new_slot->p_slot, this->slot_pool_->get_slot(new_slot->slot_index));
    }
    BATT_CHECK_NE(new_slot->slot_index, kInvalidIndex);

    // If we can atomically overwrite the slot index value we saw above (CAS), then we are done!
    //
    if (slot_index_ref.compare_exchange_weak(observed_slot_index, new_slot->slot_index)) {
      break;
    }
  }

  BATT_CHECK(new_slot);

  // We purposely delayed this step until we knew that this thread must initialize the cache
  // slot's Latch.  This function will probably start I/O (or do something else in a test case...)
  //
  initialize(new_slot->pinned_ref);

  // Done! (Inserted new value)
  //
  this->metrics().insert_count.fetch_add(1);
  return {std::move(new_slot->pinned_ref)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageDeviceCache::erase(PageId key)
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(key), this->page_ids_.get_device_id());

  // Lookup the cache table entry for the given page id.
  //
  const i64 physical_page = this->page_ids_.get_physical_page(key);
  std::atomic<usize>& slot_index_ref = this->get_slot_index_ref(physical_page);

  usize slot_index = slot_index_ref.load();
  if (slot_index == kInvalidIndex) {
    return;
  }

  // Helper function.
  //
  const auto invalidate_ref = [&] {
    const usize slot_index_to_erase = slot_index;
    do {
      if (slot_index_ref.compare_exchange_weak(slot_index, kInvalidIndex)) {
        break;
      }
    } while (slot_index == slot_index_to_erase);
    this->metrics().erase_count.fetch_add(1);
  };

  // If the slot is still holding the passed id, then clear it out.
  //
  PageCacheSlot* slot = this->slot_pool_->get_slot(slot_index);
  if (slot->evict_if_key_equals(key)) {
    invalidate_ref();

    // Important!  Only clear the slot once we have invalidated our table entry.
    //
    slot->clear();
    slot->set_obsolete_hint();
  } else {
    // If we weren't able to evict `key`, we can still try to read the slot to see if it contains
    // `key` but is non-evictable (because there are outstanding pins); if this is the case, then
    // clear it from our table.
    //
    PageCacheSlot::PinnedRef pinned = slot->acquire_pin(PageId{}, /*ignore_key=*/true);
    if (pinned) {
      const PageId observed_key = pinned.key();
      if (!observed_key.is_valid() || observed_key == key ||
          PageIdFactory::get_device_id(observed_key) != this->page_ids_.get_device_id() ||
          this->page_ids_.get_physical_page(observed_key) != physical_page) {
        invalidate_ref();
        if (observed_key == key) {
          slot->set_obsolete_hint();
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::atomic<usize>& PageDeviceCache::get_slot_index_ref(i64 physical_page)
{
  static_assert(sizeof(std::atomic<usize>) == sizeof(usize));
  static_assert(alignof(std::atomic<usize>) == alignof(usize));

  BATT_CHECK_LT((usize)physical_page, this->cache_.size());

  return reinterpret_cast<std::atomic<usize>&>(this->cache_[physical_page]);
}

}  //namespace llfs
