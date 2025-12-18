//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_art.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ usize PackedART::common_prefix_len(const std::string_view& min_key,  //
                                              const std::string_view& max_key,  //
                                              usize base_prefix_len)
{
  BATT_CHECK_GE(min_key.size(), base_prefix_len);
  BATT_CHECK_GE(max_key.size(), base_prefix_len);

  usize prefix_len = base_prefix_len;

  const char* key0 = &min_key[base_prefix_len];
  const char* key1 = &max_key[base_prefix_len];

  usize common_len = std::min(min_key.size(), max_key.size()) - base_prefix_len;

  while (common_len && *key0 == *key1) {
    ++prefix_len;
    --common_len;
    ++key0;
    ++key1;
  }

  return prefix_len;
}

}  //namespace llfs
