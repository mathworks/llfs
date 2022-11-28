//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_bytes.hpp>
//

#include <llfs/data_packer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::string_view> unpack_object(const PackedBytes& obj, DataReader* /*src*/)
{
  return obj.as_str();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
::llfs::PackedBytes* pack_object_to(const std::string_view& from, ::llfs::PackedBytes* to,
                                    ::llfs::DataPacker* dst)
{
  if (dst->pack_string_to(to, from)) {
    return to;
  }
  return nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status validate_packed_value(const PackedBytes& packed_bytes, const void* buffer_data,
                                   usize buffer_size)
{
  const char* packed_begin = reinterpret_cast<const char*>(&packed_bytes);
  const char* packed_end = packed_begin + sizeof(PackedBytes);
  const char* buffer_begin = static_cast<const char*>(buffer_data);
  const char* buffer_end = buffer_begin + buffer_size;

  BATT_CHECK_LT((const void*)packed_begin, (const void*)packed_end);

  if ((const void*)packed_begin < (const void*)buffer_begin) {
    return ::llfs::make_status(StatusCode::kUnpackCastPackedBytesStructUnder);
  }

  if ((const void*)packed_end > (const void*)buffer_end) {
    return ::llfs::make_status(StatusCode::kUnpackCastPackedBytesStructOver);
  }

  std::string_view str = packed_bytes.as_str();
  const char* str_begin = str.data();
  const char* str_end = str_begin + str.size();

  if ((const void*)str_begin < (const void*)buffer_begin) {
    return ::llfs::make_status(StatusCode::kUnpackCastPackedBytesDataUnder);
  }

  if ((const void*)str_end > (const void*)buffer_end) {
    return ::llfs::make_status(StatusCode::kUnpackCastPackedBytesDataOver);
  }

  return batt::OkStatus();
}

}  // namespace llfs
