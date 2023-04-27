//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_page_id.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> trace_refs(const PackedPageId& page_id)
{
  return batt::seq::single_item(page_id.unpack()) | batt::seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> trace_refs(const PageId& page_id)
{
  return batt::seq::single_item(batt::make_copy(page_id)) | batt::seq::boxed();
}

}  //namespace llfs
