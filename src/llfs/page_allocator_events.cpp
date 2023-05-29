//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator_events.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorAttach& t)
{
  return out << "PackedPageAllocatorAttach{.user_slot=" << t.user_slot << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorDetach& t)
{
  return out << "PackedPageAllocatorDetach{.user_slot=" << t.user_slot << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageRefCountRefresh& t)
{
  return out << "{" << PackedPageRefCount{t.page_id, t.ref_count} << ", user_index=" << t.user_index
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorTxn& t)
{
  out << "PackedPageAllocatorTxn{.user_slot=" << t.user_slot << ", .user_index=" << t.user_index
      << ", .ref_counts[" << t.ref_counts.size() << "]={";
  for (const auto& item : t.ref_counts) {
    out << item << ", ";
  }
  return out << "},}";
}

}  // namespace llfs
