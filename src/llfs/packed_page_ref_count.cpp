//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_page_ref_count.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageRefCount& t)
{
  out << "PackedPageRefCount{.page_id=" << std::hex << std::setw(5) << std::setfill('0')
      << t.page_id.value() << ", .ref_count=";

  if (t.ref_count == kRefCount_1_to_0) {
    out << "kRefCount_1_to_0";
  } else {
    out << std::setw(0) << std::dec << t.ref_count.value();
  }

  return out << ",}";
}

}  // namespace llfs
