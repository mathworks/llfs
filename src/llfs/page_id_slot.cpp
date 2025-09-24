//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_id_slot.hpp>
//

#include <llfs/page_loader.hpp>
#include <llfs/page_view.hpp>
#include <llfs/pinned_page.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto PageIdSlot::from_pinned_page(const PinnedPage& pinned) -> Self
{
  return Self{
      .page_id = pinned->page_id(),
      .cache_slot_ref = PageCacheSlot::AtomicRef{pinned.get_cache_slot()},
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::StatusOr<PinnedPage> PageIdSlot::load_through_impl(
    PageCacheSlot::AtomicRef& cache_slot_ref, PageLoader& loader, PageId page_id,
    const PageLoadOptions& load_options)
{
  {
    batt::StatusOr<PinnedPage> pinned = Self::try_pin_impl(cache_slot_ref, page_id);
    if (pinned.ok()) {
      return pinned;
    }
  }
  batt::StatusOr<PinnedPage> pinned = loader.load_page(page_id, load_options);
  if (pinned.ok()) {
    cache_slot_ref = pinned->get_cache_slot();
  }

  return pinned;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::StatusOr<PinnedPage> PageIdSlot::try_pin_impl(
    PageCacheSlot::AtomicRef& cache_slot_ref, PageId page_id)
{
  PageIdSlot::metrics().load_total_count.add(1);

  PageCacheSlot::PinnedRef cache_slot = cache_slot_ref.pin(page_id);
  if (!cache_slot) {
    PageIdSlot::metrics().load_slot_miss_count.add(1);
    return make_status(StatusCode::kPinFailedPageEvicted);
  }

  PageIdSlot::metrics().load_slot_hit_count.add(1);

  batt::StatusOr<std::shared_ptr<const PageView>> page_view = cache_slot->await();
  BATT_REQUIRE_OK(page_view);

  return PinnedPage{page_view->get(), std::move(cache_slot)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<PinnedPage> PageIdSlot::try_pin() const
{
  return Self::try_pin_impl(this->cache_slot_ref, this->page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<PinnedPage> PageIdSlot::try_pin_through(BasicPageLoader<PinnedPage>& loader,
                                                       const PageLoadOptions& load_options) const
{
  StatusOr<PinnedPage> pinned_page = this->try_pin();
  if (pinned_page.ok()) {
    return pinned_page;
  }

  pinned_page = loader.try_pin_cached_page(this->page_id, load_options);
  BATT_REQUIRE_OK(pinned_page);

  this->cache_slot_ref = pinned_page->get_cache_slot();

  return pinned_page;
}

}  // namespace llfs
