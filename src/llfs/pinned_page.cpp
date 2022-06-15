//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/pinned_page.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator==(const PinnedPage& l, const PinnedPage& r)
{
  return l.get() == r.get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator!=(const PinnedPage& l, const PinnedPage& r)
{
  return l.get() != r.get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator==(const PinnedPage& l, const std::nullptr_t& r)
{
  return l.get() == r;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator!=(const PinnedPage& l, const std::nullptr_t& r)
{
  return l.get() != r;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator==(const std::nullptr_t& l, const PinnedPage& r)
{
  return l == r.get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator!=(const std::nullptr_t& l, const PinnedPage& r)
{
  return l != r.get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
page_id_int get_page_id_int(const PinnedPage& pinned)
{
  if (!pinned) {
    return kInvalidPageId;
  }
  return pinned->page_id().int_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageId get_page_id(const PinnedPage& pinned)
{
  if (!pinned) {
    return PageId{};
  }
  return pinned->page_id();
}

}  // namespace llfs
