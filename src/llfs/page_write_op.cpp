//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_write_op.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ std::unique_ptr<PageWriteOp[]> PageWriteOp::allocate_array(
    usize n, batt::Watch<i64>& done_counter)
{
  auto ops = std::make_unique<PageWriteOp[]>(n);
  for (auto& op : as_slice(ops.get(), n)) {
    op.done_counter = &done_counter;
  }
  return ops;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageWriteOp::PageWriteOp() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageWriteOp::~PageWriteOp() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status parallel_drop_pages(const std::vector<PageId>& deleted_pages, PageCache& cache, u64 job_id,
                           u64 callers)
{
  // Do all drops asynchronously in parallel.
  //
  const usize n_ops = deleted_pages.size();
  if (n_ops == 0) {
    return OkStatus();
  }

  batt::Watch<i64> done_counter{0};
  std::unique_ptr<PageWriteOp[]> ops = PageWriteOp::allocate_array(n_ops, done_counter);
  {
    usize i = 0;
    for (const PageId page_id : deleted_pages) {
      ops[i].page_id = page_id;
      cache.arena_for_page_id(page_id).device().drop(page_id, ops[i].get_handler());
      ++i;

#if LLFS_TRACK_NEW_PAGE_EVENTS
      cache.track_new_page_event(NewPageTracker{
          .ts = 0,
          .job_id = job_id,
          .page_id = page_id,
          .callers = callers | Caller::PageCacheJob_commit_2,
          .event_id = (int)NewPageTracker::Event::kDrop,
      });
#endif  // LLFS_TRACK_NEW_PAGE_EVENTS
    }
  }

  // Wait for all concurrent page ops to finish.
  //
  auto final_count = done_counter.await_true([&](i64 n) {
    return n == (i64)n_ops;
  });
  BATT_REQUIRE_OK(final_count);

  // Purge all dropped pages from the cache to reduce memory pressure.
  //
  Status overall_status;
  for (PageWriteOp& op : as_slice(ops.get(), n_ops)) {
    Status page_status = op.result;
    overall_status.Update(page_status);
    if (page_status.ok()) {
      cache.purge(op.page_id, callers | Caller::PageCacheJob_commit_2, job_id);
    }
  }

  return overall_status;
}

}  // namespace llfs
