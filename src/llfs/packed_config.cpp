//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_config.hpp>
//

#include <boost/uuid/uuid_io.hpp>

#include <ostream>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ std::string_view PackedConfigSlotBase::Tag::to_string(u16 value)
{
  switch (value) {
    case kNone:
      return "kNone";
    case kPageArena:
      return "kPageArena";
    case kVolume:
      return "kVolume";
    case kLogDevice:
      return "kLogDevice";
    case kPageDevice:
      return "kPageDevice";
    case kPageAllocator:
      return "kPageAllocator";
    case kVolumeContinuation:
      return "kVolumeContinuation";
    default:
      break;
  }
  return "UNKNOWN";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedConfigSlot& slot)
{
  return out << "PackedConfigSlot{.tag=" << PackedConfigSlotBase::Tag::to_string(slot.tag)
             << ", .slot_i=" << (int)slot.slot_i << ", .n_slots=" << (int)slot.n_slots
             << ", .uuid=" << slot.uuid << ",}";
}

}  // namespace llfs
