//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_page_header.hpp>
//

#include <llfs/logging.hpp>
#include <llfs/status_code.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PackedPageHeader::sanity_check(PageSize page_size, PageId page_id,
                                      const PageIdFactory& id_factory) const noexcept
{
  if (this->magic != PackedPageHeader::kMagic) {
    return make_status(StatusCode::kPageHeaderBadMagic);
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
  const PageId header_page_id = this->page_id.as_page_id();
  if (header_page_id != page_id) {
    const i64 expected_physical_page = id_factory.get_physical_page(page_id);
    const i64 actual_physical_page = id_factory.get_physical_page(header_page_id);
    const page_device_id_int expected_device_id = id_factory.get_device_id(page_id);
    const page_device_id_int actual_device_id = id_factory.get_device_id(header_page_id);
    const page_generation_int expected_generation = id_factory.get_generation(page_id);
    const page_generation_int actual_generation = id_factory.get_generation(header_page_id);

    if (actual_physical_page == expected_physical_page  //
        && actual_device_id == expected_device_id       //
        && actual_generation != expected_generation) {
      return make_status(StatusCode::kPageHeaderBadGeneration);
    }

    return make_status(StatusCode::kPageHeaderBadPageId);
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status require_page_layout(const PageBuffer& page,
                           const Optional<PageLayoutId>& required_layout) noexcept
{
  if (required_layout) {
    const PageLayoutId layout_id = get_page_header(page).layout_id;
    if (layout_id != *required_layout) {
      const Status status = ::llfs::make_status(StatusCode::kPageHeaderBadLayoutId);
      LLFS_LOG_ERROR() << status << std::endl
                       << BATT_INSPECT(layout_id) << BATT_INSPECT(required_layout) << std::endl;
      return status;
    }
  }
  return OkStatus();
}

}  // namespace llfs
