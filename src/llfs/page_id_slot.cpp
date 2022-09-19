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
batt::StatusOr<PinnedPage> PageIdSlot::load_through(PageLoader& loader,
                                                    const Optional<PageLayoutId>& required_layout,
                                                    PinPageToJob pin_page_to_job,
                                                    OkIfNotFound ok_if_not_found) const noexcept
{
  {
    batt::StatusOr<PinnedPage> pinned = this->try_pin();
    if (pinned.ok()) {
      return pinned;
    }
  }
  batt::StatusOr<PinnedPage> pinned =
      loader.get(this->page_id, required_layout, pin_page_to_job, ok_if_not_found);
  if (pinned.ok()) {
    this->cache_slot_ref = pinned->get_cache_slot();
  }

  return pinned;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<PinnedPage> PageIdSlot::try_pin() const noexcept
{
  PageIdSlot::metrics().load_total_count.fetch_add(1);

  const page_id_int id_val = this->page_id.int_value();

  PinnedCacheSlot<page_id_int, batt::Latch<std::shared_ptr<const PageView>>> cache_slot =
      this->cache_slot_ref.pin(id_val);

  if (!cache_slot) {
    PageIdSlot::metrics().load_slot_miss_count.fetch_add(1);
    return make_status(StatusCode::kPinFailedPageEvicted);
  }

  PageIdSlot::metrics().load_slot_hit_count.fetch_add(1);

  batt::StatusOr<std::shared_ptr<const PageView>> page_view = cache_slot->await();
  BATT_REQUIRE_OK(page_view);

  return PinnedPage{page_view->get(), std::move(cache_slot)};
}

}  // namespace llfs
