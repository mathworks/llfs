#include <llfs/packable_ref.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string_view raw_data_from_slot(const SlotParse& slot, const PackedRawData* packed)
{
  BATT_CHECK_LE((const void*)slot.body.data(), (const void*)packed);

  const usize offset = byte_distance(slot.body.data(), packed);

  BATT_CHECK_LE(offset, slot.body.size());

  return std::string_view{slot.body.data() + offset, slot.body.size() - offset};
}

}  // namespace llfs
