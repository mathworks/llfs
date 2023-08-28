//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/pack_as_raw.hpp>
//

#include <llfs/data_packer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackAsRawData pack_as_raw(std::string_view bytes)
{
  return PackAsRawData{bytes};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackAsRawData& pack_as_raw_data)
{
  return pack_as_raw_data.bytes.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedRawData* pack_object(const PackAsRawData& pack_as_raw_data, DataPacker* dst)
{
  Optional<std::string_view> packed =
      dst->pack_raw_data(pack_as_raw_data.bytes.data(), pack_as_raw_data.bytes.size());
  if (!packed) {
    return nullptr;
  }
  return const_cast<PackedRawData*>(reinterpret_cast<const PackedRawData*>(packed->data()));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedRawData&)
{
  // TODO [tastolfi 2022-02-15] should this be 0 instead? infinity/int_max? panic?
  return sizeof(PackedRawData);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::Ref<const PackedRawData>> unpack_object(const PackedRawData& packed, DataReader*)
{
  return batt::as_cref(packed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string_view raw_data_from_slot(const SlotParse& slot, const PackedRawData* packed)
{
  BATT_CHECK_LE((const void*)slot.body.data(), (const void*)packed);

  const usize offset = byte_distance(slot.body.data(), packed);

  BATT_CHECK_LE(offset, slot.body.size());

  return std::string_view{slot.body.data() + offset, slot.body.size() - offset};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackAsRawData& t)
{
  return out << "PackAsRawData{.bytes=" << batt::c_str_literal(t.bytes) << ",}";
}

}  // namespace llfs
