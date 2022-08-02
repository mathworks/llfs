//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_log_page_header.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PackedLogPageHeader::reset(u64 slot_offset) noexcept
{
  this->magic = PackedLogPageHeader::kMagic;
  this->slot_offset = slot_offset;
  this->commit_size = 0;
  this->crc64 = -1;  // TODO [tastolfi 2022-02-09] implement me
}

}  // namespace llfs
