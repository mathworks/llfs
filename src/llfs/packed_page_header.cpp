//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_page_header.hpp>
//

#include <llfs/status_code.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PackedPageHeader::sanity_check(usize page_size, PageId page_id) const noexcept
{
  if (this->magic != PackedPageHeader::kMagic) {
    return make_status(StatusCode::kPageHeaderBadMagic);
  }
  if (this->page_id.as_page_id() != page_id) {
    return make_status(StatusCode::kPageHeaderBadPageId);
  }
  if (this->size != page_size) {
    return make_status(StatusCode::kPageHeaderBadPageSize);
  }
  if (this->unused_begin > this->unused_end) {
    return make_status(StatusCode::kPageHeaderBadUnusedSize);
  }
  if (this->unused_begin > page_size) {
    return make_status(StatusCode::kPageHeaderBadUnusedBegin);
  }
  if (this->unused_end > page_size) {
    return make_status(StatusCode::kPageHeaderBadUnusedEnd);
  }

  return batt::OkStatus();
}

}  // namespace llfs
