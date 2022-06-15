//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_ref_count.hpp>
//

#include <batteries/hash.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator==(const PageRefCount& l, const PageRefCount& r)
{
  return l.page_id == r.page_id && l.ref_count == r.ref_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize hash_value(const PageRefCount& prc)
{
  return batt::hash(prc.page_id, prc.ref_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PageRefCount& t)
{
  out << "{.page=" << std::hex << t.page_id << std::dec << ", .delta=";
  if (t.ref_count > 0) {
    out << "+";
  }
  return out << t.ref_count << ",}";
}

}  // namespace llfs
