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
StatusOr<PinnedPage> PageLoader::get(const PageIdSlot& page_id_slot,
                                     const Optional<PageLayoutId>& required_layout,
                                     PinPageToJob pin_page_to_job)
{
  return page_id_slot.load_through(*this, required_layout, pin_page_to_job);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get(const PageIdSlot& page_id_slot,
                                     const Optional<PageLayoutId>& required_layout)
{
  return this->get(page_id_slot, required_layout, PinPageToJob::kDefault);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get(const PageIdSlot& page_id_slot, PinPageToJob pin_page_to_job)
{
  return this->get(page_id_slot, /*required_layout=*/None, pin_page_to_job);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get(const PageIdSlot& page_id_slot)
{
  return this->get(page_id_slot, /*required_layout=*/None, PinPageToJob::kDefault);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get(PageId page_id, const Optional<PageLayoutId>& required_layout)
{
  return this->get(page_id, required_layout, PinPageToJob::kDefault);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get(PageId page_id, PinPageToJob pin_page_to_job)
{
  return this->get(page_id, /*required_layout=*/None, pin_page_to_job);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageLoader::get(PageId page_id)
{
  return this->get(page_id, /*required_layout=*/None);
}

}  // namespace llfs
