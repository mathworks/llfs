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
LoadingPageTracer::LoadingPageTracer(PageLoader& page_loader) noexcept : page_loader_{page_loader}
{
}

LoadingPageTracer::~LoadingPageTracer()
{
}

batt::StatusOr<batt::BoxedSeq<PageId>> LoadingPageTracer::trace_page_refs(
    PageId from_page_id) noexcept
{
  BATT_CHECK(this->page_loader_.is_valid());

  batt::StatusOr<PinnedPage> status_or_page =
      this->page_loader_.get().get_page(from_page_id, OkIfNotFound{false});
  BATT_REQUIRE_OK(status_or_page);

  PinnedPage& page = *status_or_page;
  BATT_CHECK_NOT_NULLPTR(page);

  return page->trace_refs();
}

}  // namespace llfs
