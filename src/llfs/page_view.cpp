//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_view.hpp>
//

#include <atomic>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageView::validate(PageId expected_id)
{
  // TODO [tastolfi 2021-09-07] return error status codes instead of panic

  const PackedPageHeader& header = get_page_header(*this->data_);

  BATT_CHECK_EQ(header.magic, PackedPageHeader::kMagic);

  BATT_CHECK_EQ(header.page_id.id_val, expected_id.int_value())
      << "\npage_id=      @" << std::hex << expected_id << "\nheader.id_val=@" << std::hex
      << header.page_id.id_val;

#if LLFS_DISABLE_PAGE_CRC
#else
  // TODO [tastolfi 2020-12-29] - give an error instead of asserting here.
  //
  BATT_CHECK_EQ(header.crc32.value(), compute_page_crc64(*this->data_));
#endif

  return OkStatus();
}

StatusOr<usize> PageView::get_keys([[maybe_unused]] ItemOffset lower_bound,
                                   [[maybe_unused]] const Slice<KeyView>& key_buffer_out,
                                   [[maybe_unused]] StableStringStore& storage) const
{
  return StatusOr<usize>{batt::StatusCode::kUnimplemented};
}

}  // namespace llfs
