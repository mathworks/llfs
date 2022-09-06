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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedLogPageHeader& t)
{
  return out << "PackedLogPageHeader{.magic=" << std::hex << t.magic.value() << std::dec  //
             << ", .slot_offset=" << t.slot_offset.value()                                //
             << ", .commit_size=" << t.commit_size.value()                                //
             << ", .crc64=" << std::hex << t.crc64.value() << std::dec                    //
             << ", .trim_pos=" << t.trim_pos.value()                                      //
             << ", .flush_pos=" << t.flush_pos.value()                                    //
             << ", .commit_pos=" << t.commit_pos.value()                                  //
             << ",}";
}

}  // namespace llfs
