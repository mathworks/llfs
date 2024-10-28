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
LoadingPageTracer::LoadingPageTracer(PageLoader& page_loader,
                                     batt::Optional<bool> ok_if_not_found) noexcept
    : page_loader_{page_loader}
    , ok_if_not_found_{*ok_if_not_found}
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
  batt::StatusOr<PinnedPage> status_or_page =
      this->page_loader_.get_page(from_page_id, OkIfNotFound{this->ok_if_not_found_});
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
  const u64 physical_page =
      this->page_devices_[device_id]->arena.device().page_ids().get_physical_page(from_page_id);
  const page_generation_int generation =
      this->page_devices_[device_id]->arena.device().page_ids().get_generation(from_page_id);

  const OutgoingRefsStatus outgoing_refs_status = static_cast<OutgoingRefsStatus>(
      this->page_devices_[device_id]->no_outgoing_refs_cache.get_page_bits(physical_page,
                                                                           generation));

  // If we already have information that this page has no outgoing refs, do not load the page and
  // call trace refs; there's nothing we need to do.
  //
  if (outgoing_refs_status == OutgoingRefsStatus::kNoOutgoingRefs) {
    return batt::seq::Empty<PageId>{} | batt::seq::boxed();
  }

  // If outgoing_refs_status has any other status, we must call trace_refs and set
  // the outgoing refs information if it hasn't ever been traced before.
  //
  batt::StatusOr<batt::BoxedSeq<PageId>> outgoing_refs =
      this->loader_.trace_page_refs(from_page_id);
  BATT_REQUIRE_OK(outgoing_refs);

  if (outgoing_refs_status == OutgoingRefsStatus::kNotTraced) {
    bool new_status_no_refs = true;
    if ((*outgoing_refs).peek()) {
      new_status_no_refs = false;
    }
    this->page_devices_[device_id]->no_outgoing_refs_cache.set_page_bits(
        physical_page, generation, HasNoOutgoingRefs{new_status_no_refs});
  }

  return outgoing_refs;
}

}  // namespace llfs
