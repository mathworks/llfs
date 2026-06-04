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
/*explicit*/ PageWriteOp::PageWriteOp() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageWriteOp::~PageWriteOp() noexcept
{
  BATT_CHECK_EQ(this->is_pending(), false) << "This write op has an uninvoked handler!";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::CustomAllocHandler<PageWriteOp::HandlerImpl> PageWriteOp::get_handler(
    PageId id, batt::Watch<i64>* done_counter) noexcept
{
  BATT_CHECK_EQ(std::exchange(this->pending_, true), false)
      << "Only one handler per PageWriteOp is allowed at a time!";

  BATT_CHECK_NOT_NULLPTR(this->done_counter_);

  this->page_id_ = id;
  this->done_counter_ = done_counter;

  return make_custom_alloc_handler(this->handler_memory_, HandlerImpl{.op_ = this});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageWriteOp::HandlerImpl::operator()(PageDevice::WriteResult result) const noexcept
{
  this->op_->handle_write(std::move(result));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageWriteOp::handle_write(PageDevice::WriteResult result) noexcept
{
  BATT_CHECK_EQ(std::exchange(this->pending_, false), true)
      << "PageWriteOp handlers may only be invoked once!";

  BATT_CHECK_NOT_NULLPTR(this->done_counter_);

  this->result_ = std::move(result);
  this->done_counter_->fetch_add(1);
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
  auto ops = std::make_unique<PageWriteOp[]>(n_ops);
  {
    usize i = 0;
    for (const PageId page_id : deleted_pages) {
      cache.arena_for_page_id(page_id).device().drop(page_id,
                                                     ops[i].get_handler(page_id, &done_counter));
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
    Status page_status = op.result();
    overall_status.Update(page_status);
    if (page_status.ok()) {
      cache.purge(op.page_id(), callers | Caller::PageCacheJob_commit_2, job_id);
    }
  }

  return overall_status;
}

}  // namespace llfs
