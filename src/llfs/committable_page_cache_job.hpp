//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_COMMITTABLE_PAGE_CACHE_JOB_HPP
#define LLFS_COMMITTABLE_PAGE_CACHE_JOB_HPP

#include <llfs/config.hpp>
//
#include <llfs/finalized_page_cache_job.hpp>
#include <llfs/int_types.hpp>
#include <llfs/job_commit_params.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/page_write_op.hpp>
#include <llfs/seq.hpp>
#include <llfs/slot.hpp>

#include <batteries/async/watch.hpp>

#include <memory>

namespace llfs {

/** \brief A PageCacheJob that has been finalized and is ready to be committed to durable storage.
 *
 * This class is a move-only value type with unique_ptr-like semantics.  The usage pattern is:
 *
 * 1. Convert a unique_ptr<PageCacheJob> to CommittablePageCacheJob using
 *    CommittablePageCacheJob::from.
 * 2.
 */
class CommittablePageCacheJob
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Converts the passed PageCacheJob to a CommittablePageCacheJob.  No changes to the job
   * (e.g., adding new pages) are allowed after conversion to CommittablePageCacheJob.
   */
  static StatusOr<CommittablePageCacheJob> from(std::unique_ptr<PageCacheJob> job, u64 callers);

  // `commit` is implemented as a free function so that CommittablePageCacheJob can be moved into
  // it, emphasizing that this is an operation that can only be performed once per job.  Declare
  // friendship so it can operate like a member function.
  //
  friend Status commit(CommittablePageCacheJob committable_job, const JobCommitParams& params,
                       u64 callers, slot_offset_type prev_caller_slot,
                       batt::Watch<slot_offset_type>* durable_caller_slot);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs an empty/invalid CommittablePageCacheJob.
   */
  CommittablePageCacheJob() = default;

  /** \brief This class is moveable but not copyable.
   */
  CommittablePageCacheJob(const CommittablePageCacheJob&) = delete;

  /** \brief This class is moveable but not copyable.
   */
  CommittablePageCacheJob& operator=(const CommittablePageCacheJob&) = delete;

  /** \brief This class is moveable but not copyable.
   */
  CommittablePageCacheJob(CommittablePageCacheJob&&) = default;

  /** \brief This class is moveable but not copyable.
   */
  CommittablePageCacheJob& operator=(CommittablePageCacheJob&&) = default;

  /** \brief Sets the tracker status to kAborted if not already in a terminal state.
   */
  ~CommittablePageCacheJob() noexcept;

  /** \brief Creates a new FinalizedPageCacheJob that points to this job.
   *
   * The returned object can be used as the basis for future jobs (using
   * PageCacheJob::set_base_job), so that we don't have to wait for all the new pages to be written
   * to device in order to construct a dependent PageCacheJob.  That is, requests to load pages via
   * a PageCacheJob will automatically see any new pages written in the "base job"
   * (FinalizedPageCacheJob) of that PageCacheJob, even if they haven't actually been written to
   * storage yet.
   */
  FinalizedPageCacheJob finalized_job() const;

  /** \brief Returns the process-unique serial number of this job; this will be the same as
   * job->job_id, where job is the PageCacheJob used to create this.
   */
  u64 job_id() const;

  /** \brief Returns a sequence of PageIds for the new pages to be written by this job.
   */
  BoxedSeq<PageId> new_page_ids() const;

  /** \brief Returns a sequence of PageIds for the dead pages to be dropped by this job.
   */
  BoxedSeq<PageId> deleted_page_ids() const;

  /** \brief Returns the page reference count updates for _root_ refs (i.e., those accessible
   * directly from the WAL) for this job.
   */
  BoxedSeq<PageRefCount> root_set_deltas() const;

  /** \brief Returns the PageDevice ids for which this job will update ref counts.
   */
  BoxedSeq<page_device_id_int> page_device_ids() const;

  /** \brief Returns the number of new pages to be written when this job is committed.
   */
  usize new_page_count() const noexcept;

  /** \brief Returns the number of pages to be deleted when this job is committed.
   */
  usize deleted_page_count() const noexcept;

  /** \brief Returns the number of root page references that will be written to the log when this
   * job is committed.
   */
  usize root_count() const noexcept;

  /** \brief Returns the number of distinct PageDevice ids for which this job will trigger page ref
   * count updates.
   */
  usize page_device_count() const noexcept;

  /** \brief Returns true iff this object holds a valid (unique) reference to a PageCacheJob.  This
   * will be false after passing a CommittablePageCacheJob by move to commit().
   */
  explicit operator bool() const
  {
    return this->job_ && this->tracker_;
  }

  /** \brief Cancels this job, reporting batt::StatusCode::kCancelled to any status listeners.
   */
  void cancel();

  /** \brief Starts writing new page data to storage device(s).
   */
  Status start_writing_new_pages();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct DeviceUpdateState {
    std::vector<PageRefCount> ref_count_updates;
    const PageArena* p_arena = nullptr;
    slot_offset_type sync_point = 0;
  };

  struct PageRefCountUpdates {
    std::unordered_map<page_device_id_int, DeviceUpdateState> per_device;
    bool initialized = false;
  };

  struct DeadPages {
    std::vector<PageId> ids;
  };

  struct WriteNewPagesContext {
    CommittablePageCacheJob* const that;
    const PageCacheJob* const job;
    u64 op_count;
    u64 used_byte_count;
    u64 total_byte_count;
    batt::Watch<i64> done_counter;
    usize n_ops;
    std::unique_ptr<PageWriteOp[]> ops;

    //----- --- -- -  -  -   -

    explicit WriteNewPagesContext(CommittablePageCacheJob* that) noexcept;

    WriteNewPagesContext(const WriteNewPagesContext&) = delete;
    WriteNewPagesContext& operator=(const WriteNewPagesContext&) = delete;

    Status start();

    Status await_finish();
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit CommittablePageCacheJob(std::unique_ptr<PageCacheJob> finalized_job) noexcept;

  Status commit_impl(const JobCommitParams& params, u64 callers, slot_offset_type prev_caller_slot,
                     batt::Watch<slot_offset_type>* durable_caller_slot);

  Status write_new_pages();

  StatusOr<PageRefCountUpdates> get_page_ref_count_updates(u64 callers) const;

  StatusOr<DeadPages> start_ref_count_updates(const JobCommitParams& params,
                                              PageRefCountUpdates& updates, u64 callers);

  Status await_ref_count_updates(const PageRefCountUpdates& updates);

  Status recycle_dead_pages(const JobCommitParams& params, const DeadPages& dead_pages);

  void hint_pages_obsolete(const std::vector<PageRefCount>& prcs);

  Status drop_deleted_pages(u64 callers);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::shared_ptr<const PageCacheJob> job_;
  boost::intrusive_ptr<FinalizedJobTracker> tracker_;
  PageRefCountUpdates ref_count_updates_;
  std::unique_ptr<WriteNewPagesContext> write_new_pages_context_;
};

/** \brief Write all changes in `job` to durable storage.  This is guaranteed to be atomic.
 */
Status commit(CommittablePageCacheJob committable_job, const JobCommitParams& params, u64 callers,
              slot_offset_type prev_caller_slot = 0,
              batt::Watch<slot_offset_type>* durable_caller_slot = nullptr);

/** \brief Convenience shortcut for use cases where we do not pipeline job commits.
 */
Status commit(std::unique_ptr<PageCacheJob> job, const JobCommitParams& params, u64 callers,
              slot_offset_type prev_caller_slot = 0,
              batt::Watch<slot_offset_type>* durable_caller_slot = nullptr);

}  //namespace llfs

#endif  // LLFS_COMMITTABLE_PAGE_CACHE_JOB_HPP
