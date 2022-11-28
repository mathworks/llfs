//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_status.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedStatus& packed_status)
{
  return sizeof(PackedStatus)                                          //
         + (packed_sizeof(packed_status.group) - sizeof(PackedBytes))  //
         + (packed_sizeof(packed_status.message) - sizeof(PackedBytes));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const batt::Status& status)
{
  return sizeof(PackedStatus)                                                      //
         + packed_sizeof_str_data(std::string_view{status.group().name()}.size())  //
         + packed_sizeof_str_data(status.message().size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::Status> unpack_object(const PackedStatus& packed_status, DataReader*)
{
  // This is complicated by the fact that the remote group type may not have been registered
  // locally.  TODO [tastolfi 2022-11-04]
  //
  return {batt::StatusCode::kUnimplemented};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedStatus* pack_object_to(const batt::Status& status, PackedStatus* packed_status,
                             DataPacker* packer)
{
  packed_status->code = status.code_entry().enum_value;

  if (!packer->pack_string_to(&packed_status->group, status.group().name())) {
    return nullptr;
  }
  if (!packer->pack_string_to(&packed_status->message, status.message())) {
    return nullptr;
  }

  return packed_status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedStatus& t)
{
  return out << t.group.as_str() << "{" << t.code.value() << "}:" << t.message.as_str();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status validate_packed_value(const llfs::PackedStatus& packed, const void* buffer_data,
                                   std::size_t buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(packed.group, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(packed.message, buffer_data, buffer_size));

  return batt::OkStatus();
}

}  // namespace llfs
