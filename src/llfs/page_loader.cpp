//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_loader.hpp>
//

#include <llfs/pinned_page.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool bool_from(PinPageToJob pin_page, bool default_value)
{
  switch (pin_page) {
    case PinPageToJob::kFalse:
      return false;

    case PinPageToJob::kTrue:
      return true;

    case PinPageToJob::kDefault:
      return default_value;

    default:
      BATT_PANIC() << "bad value for pin_page: " << (int)pin_page;
      BATT_UNREACHABLE();
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageLoader

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page_slot_with_layout_in_job(
    const PageIdSlot& page_id_slot, const Optional<PageLayoutId>& required_layout,
    PinPageToJob pin_page_to_job, OkIfNotFound ok_if_not_found)
{
  return page_id_slot.load_through(*this, required_layout, pin_page_to_job, ok_if_not_found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page_slot_with_layout(
    const PageIdSlot& page_id_slot, const Optional<PageLayoutId>& required_layout,
    OkIfNotFound ok_if_not_found)
{
  return this->get_page_slot_with_layout_in_job(page_id_slot, required_layout,
                                                PinPageToJob::kDefault, ok_if_not_found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page_slot_in_job(const PageIdSlot& page_id_slot,
                                                      PinPageToJob pin_page_to_job,
                                                      OkIfNotFound ok_if_not_found)
{
  return this->get_page_slot_with_layout_in_job(page_id_slot, /*required_layout=*/None,
                                                pin_page_to_job, ok_if_not_found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page_slot(const PageIdSlot& page_id_slot,
                                               OkIfNotFound ok_if_not_found)
{
  return this->get_page_slot_with_layout_in_job(page_id_slot, /*required_layout=*/None,
                                                PinPageToJob::kDefault, ok_if_not_found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page_with_layout(PageId page_id,
                                                      const Optional<PageLayoutId>& required_layout,
                                                      OkIfNotFound ok_if_not_found)
{
  return this->get_page_with_layout_in_job(page_id, required_layout, PinPageToJob::kDefault,
                                           ok_if_not_found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page_in_job(PageId page_id, PinPageToJob pin_page_to_job,
                                                 OkIfNotFound ok_if_not_found)
{
  return this->get_page_with_layout_in_job(page_id, /*required_layout=*/None, pin_page_to_job,
                                           ok_if_not_found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page(PageId page_id, OkIfNotFound ok_if_not_found)
{
  return this->get_page_with_layout(page_id, /*required_layout=*/None, ok_if_not_found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get_page_slot_ref_with_layout_in_job(
    PageId page_id, PageCacheSlot::AtomicRef& slot_ref,
    const Optional<PageLayoutId>& required_layout, PinPageToJob pin_page_to_job,
    OkIfNotFound ok_if_not_found)
{
  return PageIdSlot::load_through_impl(slot_ref, *this, required_layout, pin_page_to_job,
                                       ok_if_not_found, page_id);
}

}  // namespace llfs
