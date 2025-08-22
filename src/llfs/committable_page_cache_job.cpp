//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/committable_page_cache_job.hpp>
//

#include <llfs/trace_refs_recursive.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<CommittablePageCacheJob> CommittablePageCacheJob::from(
    std::unique_ptr<PageCacheJob> job, u64 callers)
{
  StatusOr<usize> prune_status = job->prune(callers | Caller::PageCacheJob_finalize);
  BATT_REQUIRE_OK(prune_status);

  // This job will no longer be changing, so unpin pages to save memory.
  //
  job->unpin_all();

  auto committable_job = CommittablePageCacheJob{std::move(job)};

  // Calculate page reference count updates for all devices.
  //
  BATT_ASSIGN_OK_RESULT(committable_job.ref_count_updates_,
                        committable_job.get_page_ref_count_updates(callers));

  return committable_job;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CommittablePageCacheJob::CommittablePageCacheJob(
    std::unique_ptr<PageCacheJob> finalized_job) noexcept
    : job_{std::move(finalized_job)}
    , tracker_{new FinalizedJobTracker{this->job_}}
{
  BATT_CHECK(this->job_->is_pruned());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CommittablePageCacheJob::~CommittablePageCacheJob() noexcept
{
  if (this->job_ && this->tracker_) {
    this->tracker_->progress_.modify([](PageCacheJobProgress old) {
      if (is_terminal_state(old)) {
        return old;
      }
      return PageCacheJobProgress::kCancelled;
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 CommittablePageCacheJob::job_id() const
{
  return this->job_->job_id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> CommittablePageCacheJob::new_page_ids() const
{
  return as_seq(this->job_->get_new_pages().begin(), this->job_->get_new_pages().end())  //
         | seq::map([](const auto& kv_pair) -> PageId {
             return kv_pair.first;
           })  //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize CommittablePageCacheJob::new_page_count() const noexcept
{
  return this->job_->get_new_pages().size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> CommittablePageCacheJob::deleted_page_ids() const
{
  return BoxedSeq<PageId>{
      as_seq(this->job_->get_deleted_pages().begin(), this->job_->get_deleted_pages().end())};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize CommittablePageCacheJob::deleted_page_count() const noexcept
{
  return this->job_->get_deleted_pages().size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageRefCount> CommittablePageCacheJob::root_set_deltas() const
{
  return as_seq(this->job_->get_root_set_delta().begin(),
                this->job_->get_root_set_delta().end())  //
         | seq::map([](const auto& kv_pair) {
             return PageRefCount{
                 .page_id = kv_pair.first,
                 .ref_count = kv_pair.second,
             };
           })  //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize CommittablePageCacheJob::root_count() const noexcept
{
  usize count = 0;
  for (const auto& [page_id, ref_count] : this->job_->get_root_set_delta()) {
    count += ref_count;
  }
  return count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<page_device_id_int> CommittablePageCacheJob::page_device_ids() const
{
  // Make sure the ref_count_updates_ is initialized!
  //
  BATT_CHECK(this->ref_count_updates_.initialized);

  return as_seq(this->ref_count_updates_.per_device.begin(),
                this->ref_count_updates_.per_device.end())  //
         | seq::map([](const auto& kv_pair) -> page_device_id_int {
             return kv_pair.first;
           })  //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize CommittablePageCacheJob::page_device_count() const noexcept
{
  return this->ref_count_updates_.per_device.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void CommittablePageCacheJob::cancel()
{
  if (!this->tracker_) {
    return;
  }

  this->tracker_->cancel();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FinalizedPageCacheJob CommittablePageCacheJob::finalized_job() const
{
  return FinalizedPageCacheJob{batt::make_copy(this->tracker_)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status commit(std::unique_ptr<PageCacheJob> job, const JobCommitParams& params, u64 callers,
              slot_offset_type prev_caller_slot, batt::Watch<slot_offset_type>* durable_caller_slot)
{
  BATT_ASSIGN_OK_RESULT(auto committable_job,
                        CommittablePageCacheJob::from(std::move(job), callers));

  return commit(std::move(committable_job), params, callers, prev_caller_slot, durable_caller_slot);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
Status commit(CommittablePageCacheJob committable_job, const JobCommitParams& params, u64 callers,
              slot_offset_type prev_caller_slot, batt::Watch<slot_offset_type>* durable_caller_slot)
{
  return committable_job.commit_impl(params, callers, prev_caller_slot, durable_caller_slot);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::commit_impl(const JobCommitParams& params, u64 callers,
                                            slot_offset_type prev_caller_slot,
                                            batt::Watch<slot_offset_type>* durable_caller_slot)
{
  BATT_CHECK_NOT_NULLPTR(params.caller_uuid);

  bool success = false;
  const auto on_return = batt::finally([&] {
    if (!success) {
      this->tracker_->progress_.modify([](PageCacheJobProgress p) {
        if (p == PageCacheJobProgress::kDurable) {
          return p;
        }
        return PageCacheJobProgress::kCancelled;
      });
    }
  });

  const PageCacheJob* job = this->job_.get();
  BATT_CHECK_NOT_NULLPTR(job);

  LLFS_VLOG(1) << "commit(PageCacheJob): entered";

  // Make sure the job is pruned!
  //
  BATT_CHECK(job->is_pruned());

  // Write new pages.
  //
  Status write_status = LLFS_COLLECT_LATENCY(job->cache().metrics().page_write_latency,  //
                                             this->write_new_pages());
  BATT_REQUIRE_OK(write_status);

  // Make sure the ref_count_updates_ is initialized!
  //
  BATT_CHECK(this->ref_count_updates_.initialized);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Wait until all previous commits in our pipeline have successfully updated ref counts.
  //
  Status pipeline_status =
      LLFS_COLLECT_LATENCY(this->job_->cache().metrics().pipeline_wait_latency,  //
                           this->job_->await_base_job_durable());
  BATT_REQUIRE_OK(pipeline_status);

  if (durable_caller_slot) {
    BATT_CHECK(slot_less_than(prev_caller_slot, params.caller_slot));
    BATT_REQUIRE_OK(await_slot_offset(prev_caller_slot, *durable_caller_slot));
  }

  // Update ref counts, keeping track of the sync point for each device's allocator; this allows the
  // updates to happen in parallel.  We go through again below to synchronize them.
  //
  BATT_ASSIGN_OK_RESULT(DeadPages dead_pages,
                        LLFS_COLLECT_LATENCY(job->cache().metrics().update_ref_counts_latency,
                                             this->start_ref_count_updates(
                                                 params, this->ref_count_updates_, callers)));

  // Wait for all ref count updates to complete.
  //
  Status ref_count_status =
      LLFS_COLLECT_LATENCY(job->cache().metrics().ref_count_sync_latency,
                           this->await_ref_count_updates(this->ref_count_updates_));
  BATT_REQUIRE_OK(ref_count_status);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Now we can allow future commits in our pipeline to continue.
  //
  this->tracker_->progress_.set_value(PageCacheJobProgress::kDurable);

  if (durable_caller_slot) {
    const slot_offset_type prev_durable_slot = durable_caller_slot->set_value(params.caller_slot);
    BATT_CHECK_EQ(prev_durable_slot, prev_caller_slot);
  }

  // If there are any dead pages, assign their ownership to the recycler.
  //  - TODO [tastolfi 2021-06-12] this can be moved to its own pipeline stage/task.
  //
  Status recycle_status = this->recycle_dead_pages(params, dead_pages);
  BATT_REQUIRE_OK(recycle_status);

  // Drop any deleted pages from storage.
  //
  // IMPORTANT: this must be done after updating page ref counts; otherwise if we crash, we will
  // never be able to recover the refcounts that must go down because the page is being dropped.
  // `PageDevice::drop` is idempotent because of the generation number.
  //
  Status drop_status = this->drop_deleted_pages(callers);
  BATT_REQUIRE_OK(drop_status);

  LLFS_VLOG(1) << "commit(PageCacheJob): done";

  success = true;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::write_new_pages()
{
  BATT_REQUIRE_OK(this->start_writing_new_pages());
  BATT_REQUIRE_OK(this->write_new_pages_context_->await_finish());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::start_writing_new_pages()
{
  if (!this->write_new_pages_context_) {
    BATT_CHECK(this->job_->is_pruned());

    this->write_new_pages_context_ = std::make_unique<WriteNewPagesContext>(this);
    BATT_REQUIRE_OK(this->write_new_pages_context_->start());
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ CommittablePageCacheJob::WriteNewPagesContext::WriteNewPagesContext(
    CommittablePageCacheJob* that) noexcept
    : that{that}
    , job{that->job_.get()}
    , op_count{0}
    , used_byte_count{0}
    , total_byte_count{0}
    , done_counter{0}
    , n_ops{0}
    , ops{nullptr}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::WriteNewPagesContext::start()
{
  LLFS_VLOG(1) << "commit(PageCacheJob): writing new pages";

  if (this->that->job_->get_new_pages().empty()) {
    return OkStatus();
  }

  BATT_CHECK_NOT_NULLPTR(this->job);

  // Write the pages to their respective PageDevice asynchronously/concurrently to maximize
  // throughput.
  //
  this->n_ops = this->job->get_new_pages().size();
  this->ops = PageWriteOp::allocate_array(this->n_ops, this->done_counter);
  LLFS_VLOG(1) << "commit(PageCacheJob): writing new pages";
  {
    usize i = 0;
    for (auto& p : this->job->get_new_pages()) {
      const PageId page_id = p.first;

      // There's no need to write recovered pages, since they are already durable; skip.
      //
      if (this->job->is_recovered_page(page_id)) {
        this->ops[i].get_handler()(batt::OkStatus());
        continue;
      }

      const PageCacheJob::NewPage& new_page = p.second;
      std::shared_ptr<const PageView> new_page_view = new_page.view();
      BATT_CHECK_NOT_NULLPTR(new_page_view);
      BATT_CHECK_EQ(page_id, new_page_view->page_id());
      BATT_CHECK(this->job->get_already_pinned(page_id) != None) << BATT_INSPECT(page_id);

      // Finalize the client uuid and slot that uniquely identifies this transaction, so we can
      // guarantee exactly-once side effects in the presence of crashes.
      {
        std::shared_ptr<PageBuffer> mutable_page_buffer = new_page.buffer();
        BATT_CHECK_NOT_NULLPTR(mutable_page_buffer);
      }

      // We will need this information to update the metrics below.
      //
      const PackedPageHeader& page_header = new_page.const_page_header();
      const usize page_size = page_header.size;
      const usize used_size = page_header.used_size();

      this->ops[i].page_id = page_id;

      this->job->cache().arena_for_page_id(page_id).device().write(new_page.const_buffer(),
                                                                   this->ops[i].get_handler());

      this->total_byte_count += page_size;
      this->used_byte_count += used_size;
      this->op_count += page_size / 4096;
      ++i;
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::WriteNewPagesContext::await_finish()
{
  // Wait for all concurrent page writes to finish.
  //
  auto final_count = this->done_counter.await_true([&](i64 n) {
    return n == (i64)this->n_ops;
  });
  BATT_REQUIRE_OK(final_count);

  // Only proceed if all writes succeeded.
  //
  Status all_ops_status = OkStatus();
  for (auto& op : as_slice(this->ops.get(), this->n_ops)) {
    this->job->cache().track_new_page_event(NewPageTracker{
        .ts = 0,
        .job_id = this->job->job_id,
        .page_id = op.page_id,
        .callers = Caller::PageCacheJob_commit_0,
        .event_id = op.result.ok() ? (int)NewPageTracker::Event::kWrite_Ok
                                   : (int)NewPageTracker::Event::kWrite_Fail,
    });
    all_ops_status.Update(op.result);
  }
  BATT_REQUIRE_OK(all_ops_status);

  this->job->cache().metrics().total_bytes_written += this->total_byte_count;
  this->job->cache().metrics().used_bytes_written += this->used_byte_count;
  this->job->cache().metrics().total_write_ops += this->op_count;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto CommittablePageCacheJob::start_ref_count_updates(const JobCommitParams& params,
                                                      PageRefCountUpdates& updates, u64 /*callers*/)
    -> StatusOr<DeadPages>
{
  LLFS_VLOG(1) << "commit(PageCacheJob): updating ref counts" << BATT_INSPECT(params.caller_slot);

  DeadPages dead_pages;

  for (auto& [device_id, device_state] : updates.per_device) {
    // Hint to the cache that down-referenced pages will probably not be needed again soon.
    //
    this->hint_pages_obsolete(device_state.ref_count_updates);

    LLFS_VLOG(1) << "calling PageAllocator::update_page_ref_counts for device " << device_id << ";"
                 << BATT_INSPECT_RANGE(device_state.ref_count_updates);

    const PageArena& arena = this->job_->cache().arena_for_device_id(device_id);
    device_state.p_arena = &arena;

    BATT_ASSIGN_OK_RESULT(
        device_state.sync_point,
        arena.allocator().update_page_ref_counts(
            *params.caller_uuid, params.caller_slot, as_seq(device_state.ref_count_updates),
            /*dead_page_fn=*/
            [&dead_pages, recycle_depth = params.recycle_depth](PageId dead_page_id) {
              LLFS_VLOG(1) << "(recycle event) page is now dead: " << dead_page_id
                           << " depth=" << recycle_depth;
              dead_pages.ids.emplace_back(dead_page_id);
            }));
    //
    // ^^^ TODO [tastolfi 2021-09-13] deal with partial failure
  }

  return dead_pages;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::await_ref_count_updates(const PageRefCountUpdates& updates)
{
  LLFS_VLOG(1) << "commit(PageCacheJob): waiting on ref count sync";

  // Now wait for the allocator logs to flush.
  //
  for (const auto& [device_id, device_state] : updates.per_device) {
    Status sync_status = device_state.p_arena->allocator().sync(device_state.sync_point);
    BATT_REQUIRE_OK(sync_status);
  }
  //
  // NOTE: this is the "true" point at which a transaction is durably committed.  The commit slot
  // in a Tablet WAL is merely a reflection of this fact.

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto CommittablePageCacheJob::get_page_ref_count_updates(u64 /*callers*/)
    -> StatusOr<PageRefCountUpdates>
{
  std::unordered_map<PageId, i32, PageId::Hash> ref_count_delta = this->job_->get_root_set_delta();

  // New pages start with a ref count value of 2; 1 for the client doing the allocation, and 1 for
  // the future garabage collector that will release any references held by that page.
  //
  for (const auto& p : this->job_->get_new_pages()) {
    const PageId& id = p.first;
    if (id) {
      ref_count_delta[id] += 1;
    }
  }

  FinalizedPageCacheJob loader = this->finalized_job();

  // Trace any new pages reachable from the root set and increment their ref count; existing pages
  // are already accounted for existing ref counts (because pages are write-once).
  //
  Status trace_add_ref_status = this->job_->trace_new_roots(loader, [&ref_count_delta](PageId id) {
    if (id) {
      ref_count_delta[id] += 1;
    }
  });
  BATT_REQUIRE_OK(trace_add_ref_status);

  // Trace deleted pages non-recursively, decrementing the ref counts of all pages they directly
  // reference.
  //
  LoadingPageTracer loading_tracer{loader, /*ok_if_not_found=*/true};
  CachingPageTracer caching_tracer{this->job_->cache().devices_by_id(), loading_tracer};
  for (const PageId& deleted_page_id : this->job_->get_deleted_pages()) {
    // Decrement ref counts.
    //
    batt::StatusOr<batt::BoxedSeq<PageId>> outgoing_refs =
        caching_tracer.trace_page_refs(deleted_page_id);
    if (outgoing_refs.status() == batt::StatusCode::kNotFound) {
      this->not_found_deleted_pages_.insert(deleted_page_id);
      continue;
    }
    BATT_REQUIRE_OK(outgoing_refs);

    ref_count_delta[deleted_page_id] = kRefCount_1_to_0;

    *outgoing_refs | seq::for_each([&ref_count_delta, deleted_page_id](PageId id) {
      if (id) {
        LLFS_VLOG(1) << " decrementing ref count for page " << id
                     << " (because it was referenced from deleted page " << deleted_page_id << ")";
        ref_count_delta[id] -= 1;
      }
    });
  }

  // Build the final map of PageRefCount vectors, one per device.
  //
  PageRefCountUpdates updates;
  for (const auto& p : ref_count_delta) {
    if (p.second == 0) {
      continue;
    }
    const auto device_id = PageIdFactory::get_device_id(p.first);
    updates.per_device[device_id].ref_count_updates.emplace_back(PageRefCount{
        .page_id = p.first,
        .ref_count = p.second,
    });
  }

  updates.initialized = true;

  return updates;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void CommittablePageCacheJob::hint_pages_obsolete(
    const std::vector<PageRefCount>& ref_count_updates)
{
  for (const PageRefCount& prc : ref_count_updates) {
    if (prc.ref_count < 0) {
      Optional<PinnedPage> already_pinned = this->job_->get_already_pinned(PageId{prc.page_id});
      if (already_pinned) {
        already_pinned->hint_obsolete();
      }
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::recycle_dead_pages(const JobCommitParams& params,
                                                   const DeadPages& dead_pages)
{
  LLFS_VLOG(1) << "commit(PageCacheJob): recycling dead pages (count=" << dead_pages.ids.size()
               << ")";

  if (dead_pages.ids.empty()) {
    return OkStatus();
  }

  BATT_CHECK_NOT_NULLPTR(params.recycler.pointer());

  BATT_ASSIGN_OK_RESULT(
      slot_offset_type recycler_sync_point,
      params.recycler.recycle_pages(as_slice(dead_pages.ids), params.caller_slot,
                                    params.recycle_grant, params.recycle_depth + 1));

  LLFS_VLOG(1) << "commit(PageCacheJob): waiting for PageRecycler sync point"
               << BATT_INSPECT(params.caller_slot);

  return params.recycler.await_flush(recycler_sync_point);
  //
  // IMPORTANT: we must only finalize the job after making sure the list of dead pages is flushed to
  // the page recycler's log.
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CommittablePageCacheJob::drop_deleted_pages(u64 callers)
{
  LLFS_VLOG(1) << "commit(PageCacheJob): dropping deleted pages";

  // From the set of all deleted pages, filter out those that were not found during the attempt to
  // trace their outgoing refs.
  //
  return parallel_drop_pages(this->deleted_page_ids() | seq::filter([this](const PageId& id) {
                               auto iter = this->not_found_deleted_pages_.find(id);
                               return iter == this->not_found_deleted_pages_.end();
                             }) | seq::collect_vec(),
                             this->job_->cache(), this->job_->job_id, callers);
}

}  //namespace llfs
