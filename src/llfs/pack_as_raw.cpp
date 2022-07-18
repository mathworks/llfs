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
void* pack_object(const PackAsRawData& pack_as_raw_data, DataPacker* dst)
{
  Optional<std::string_view> packed =
      dst->pack_raw_data(pack_as_raw_data.bytes.data(), pack_as_raw_data.bytes.size());
  if (!packed) {
    return nullptr;
  }
  return const_cast<char*>(packed->data());
}

}  // namespace llfs
