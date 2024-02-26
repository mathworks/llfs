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
      .cache_slot_ref = pinned.get_cache_slot(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::StatusOr<PinnedPage> PageIdSlot::load_through_impl(
    PageCacheSlot::AtomicRef& cache_slot_ref, PageLoader& loader,
    const Optional<PageLayoutId>& required_layout, PinPageToJob pin_page_to_job,
    OkIfNotFound ok_if_not_found, PageId page_id) noexcept
{
  {
    batt::StatusOr<PinnedPage> pinned = Self::try_pin_impl(cache_slot_ref, page_id);
    if (pinned.ok()) {
      return pinned;
    }
  }
  batt::StatusOr<PinnedPage> pinned = loader.get_page_with_layout_in_job(
      page_id, required_layout, pin_page_to_job, ok_if_not_found);
  if (pinned.ok()) {
    cache_slot_ref = pinned->get_cache_slot();
  }

  return pinned;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::StatusOr<PinnedPage> PageIdSlot::try_pin_impl(
    PageCacheSlot::AtomicRef& cache_slot_ref, PageId page_id) noexcept
{
  PageIdSlot::metrics().load_total_count.fetch_add(1);

  PageCacheSlot::PinnedRef cache_slot = cache_slot_ref.pin(page_id);
  if (!cache_slot) {
    PageIdSlot::metrics().load_slot_miss_count.fetch_add(1);
    return make_status(StatusCode::kPinFailedPageEvicted);
  }

  PageIdSlot::metrics().load_slot_hit_count.fetch_add(1);

  batt::StatusOr<std::shared_ptr<const PageView>> page_view = cache_slot->await();
  BATT_REQUIRE_OK(page_view);

  return PinnedPage{page_view->get(), std::move(cache_slot)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<PinnedPage> PageIdSlot::load_through(PageLoader& loader,
                                                    const Optional<PageLayoutId>& required_layout,
                                                    PinPageToJob pin_page_to_job,
                                                    OkIfNotFound ok_if_not_found) const noexcept
{
  return Self::load_through_impl(this->cache_slot_ref, loader, required_layout, pin_page_to_job,
                                 ok_if_not_found, this->page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<PinnedPage> PageIdSlot::try_pin() const noexcept
{
  return Self::try_pin_impl(this->cache_slot_ref, this->page_id);
}

}  // namespace llfs
