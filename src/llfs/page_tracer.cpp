//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_tracer.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class LoadingPageTracer
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LoadingPageTracer::LoadingPageTracer(PageLoader& page_loader, bool ok_if_not_found) noexcept
    : page_loader_{page_loader}
    , ok_if_not_found_{ok_if_not_found}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LoadingPageTracer::~LoadingPageTracer()
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<batt::BoxedSeq<PageId>> LoadingPageTracer::trace_page_refs(
    PageId from_page_id) noexcept
{
  batt::StatusOr<PinnedPage> status_or_page = this->page_loader_.load_page(
      from_page_id, PageLoadOptions{OkIfNotFound{this->ok_if_not_found_}});
  BATT_REQUIRE_OK(status_or_page);

  this->pinned_page_ = std::move(*status_or_page);
  BATT_CHECK_NOT_NULLPTR(this->pinned_page_);

  return this->pinned_page_->trace_refs();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class CachingPageTracer
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CachingPageTracer::CachingPageTracer(
    const std::vector<std::unique_ptr<PageDeviceEntry>>& page_devices, PageTracer& loader) noexcept
    : page_devices_{page_devices}
    , loader_{loader}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CachingPageTracer::~CachingPageTracer()
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<batt::BoxedSeq<PageId>> CachingPageTracer::trace_page_refs(
    PageId from_page_id) noexcept
{
  const page_device_id_int device_id = PageIdFactory::get_device_id(from_page_id);
  BATT_CHECK_LT(device_id, this->page_devices_.size());

  const batt::BoolStatus has_outgoing_refs =
      this->page_devices_[device_id]->no_outgoing_refs_cache.has_outgoing_refs(from_page_id);

  // If we already have information that this page has no outgoing refs, do not load the page and
  // call trace refs; there's nothing we need to do.
  //
  if (has_outgoing_refs == batt::BoolStatus::kFalse) {
    return batt::seq::Empty<PageId>{} | batt::seq::boxed();
  }

  // If has_outgoing_refs has any other value, we must call trace_refs and set
  // the outgoing refs information if it hasn't ever been traced before.
  //
  batt::StatusOr<batt::BoxedSeq<PageId>> outgoing_refs =
      this->loader_.trace_page_refs(from_page_id);
  BATT_REQUIRE_OK(outgoing_refs);

  if (has_outgoing_refs == batt::BoolStatus::kUnknown) {
    bool new_status_has_refs = false;
    if ((*outgoing_refs).peek()) {
      new_status_has_refs = true;
    }
    this->page_devices_[device_id]->no_outgoing_refs_cache.set_page_state(
        from_page_id, HasOutgoingRefs{new_status_has_refs});
  }

  return outgoing_refs;
}

}  // namespace llfs
