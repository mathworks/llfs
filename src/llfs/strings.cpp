//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/strings.hpp>
//

#include <algorithm>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string_view find_common_prefix(usize skip_len, const std::string_view& a,
                                    const std::string_view& b)
{
  usize len = std::min(a.size(), b.size());
  if (skip_len >= len) {
    return std::string_view{};
  }

  len -= skip_len;
  auto a_first = a.begin() + skip_len;
  auto b_first = b.begin() + skip_len;
  const auto [a_match_end, b_match_end] = std::mismatch(a_first, std::next(a_first, len), b_first);
  const usize prefix_len = std::distance(a_first, a_match_end);

  return std::string_view{a.data() + skip_len, prefix_len};
}

}  //namespace llfs
