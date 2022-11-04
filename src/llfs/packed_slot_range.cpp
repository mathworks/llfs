//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_slot_range.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedSlotRange* pack_object_to(const SlotRange& object, PackedSlotRange* packed_object,
                                DataPacker*)
{
  packed_object->lower_bound = object.lower_bound;
  packed_object->upper_bound = object.upper_bound;

  return packed_object;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedSlotRange& t)
{
  return out << "[" << t.lower_bound.value() << ", " << t.upper_bound.value() << ")";
}

}  // namespace llfs
