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

}  // namespace llfs
