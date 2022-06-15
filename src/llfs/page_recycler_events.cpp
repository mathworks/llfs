//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_recycler_events.hpp>
//

#include <llfs/page_recycler_options.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PageToRecycle& t)
{
  return out << "PageToRecycle{.page_id=" << t.page_id << ", .slot_offset=" << t.slot_offset
             << ", .depth=" << t.depth << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageRecyclerInfo&)
{
  return out << "RecyclerInfo{...}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedRecyclePagePrepare& t)
{
  return out << "Prepare{.page=" << PageId{t.page_id.value()} << ", .batch_slot=" << t.batch_slot
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedRecycleBatchCommit& t)
{
  return out << "Commit{.batch_slot=" << t.batch_slot << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedPageRecyclerInfo PackedPageRecyclerInfo::from(const boost::uuids::uuid& uuid,
                                                    const PageRecyclerOptions& options)
{
  return PackedPageRecyclerInfo{
      .uuid = uuid,
      .info_refresh_rate = options.info_refresh_rate,
      .batch_size = options.batch_size,
      .refresh_factor = options.refresh_factor,
      .max_page_ref_depth = kMaxPageRefDepth,
      .max_refs_per_page = options.max_refs_per_page,
  };
}

}  // namespace llfs
