//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FINALIZED_PAGE_CACHE_JOB_HPP
#define LLFS_FINALIZED_PAGE_CACHE_JOB_HPP

#include <llfs/int_types.hpp>
#include <llfs/job_commit_params.hpp>
#include <llfs/page_cache_job_progress.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/watch.hpp>

#include <boost/uuid/uuid.hpp>

#include <memory>
#include <utility>

namespace llfs {

class PageCache;
class PageCacheJob;
class PageArena;
class FinalizedJobTracker;
class FinalizedPageCacheJob;
class CommittablePageCacheJob;

std::shared_ptr<const PageCacheJob> lock_job(const FinalizedJobTracker* tracker);

class FinalizedJobTracker : public boost::intrusive_ref_counter<FinalizedJobTracker>
{
 public:
  friend class CommittablePageCacheJob;

  using Self = FinalizedJobTracker;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  friend std::shared_ptr<const PageCacheJob> lock_job(const Self*);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit FinalizedJobTracker(const std::shared_ptr<const PageCacheJob>& job) noexcept
      : job_{job}
      , progress_{PageCacheJobProgress::kPending}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status await_durable()
  {
    StatusOr<PageCacheJobProgress> seen =
        this->progress_.await_true([](PageCacheJobProgress value) {
          return value == PageCacheJobProgress::kDurable || is_terminal_state(value);
        });

    BATT_REQUIRE_OK(seen);

    switch (*seen) {
      case PageCacheJobProgress::kPending:
        BATT_PANIC() << "Pending is not a terminal state!";
        BATT_UNREACHABLE();

      case PageCacheJobProgress::kAborted:
        return batt::StatusCode::kCancelled;

      case PageCacheJobProgress::kDurable:
        return OkStatus();
    }

    return batt::StatusCode::kUnknown;
  }

  PageCacheJobProgress get_progress() const
  {
    return this->progress_.get_value();
  }

 private:
  std::weak_ptr<const PageCacheJob> job_;
  batt::Watch<PageCacheJobProgress> progress_;
};

std::ostream& operator<<(std::ostream& out, const FinalizedPageCacheJob& t);

class FinalizedPageCacheJob : public PageLoader
{
 public:
  using Self = FinalizedPageCacheJob;

  friend class CommittablePageCacheJob;

  friend std::ostream& operator<<(std::ostream& out, const Self& t);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  FinalizedPageCacheJob() = default;

  FinalizedPageCacheJob(const Self&) noexcept;
  FinalizedPageCacheJob(Self&&) noexcept;

  Self& operator=(const Self&) noexcept;
  Self& operator=(Self&&) noexcept;

  void prefetch_hint(PageId page_id) override;

  StatusOr<PinnedPage> get(PageId page_id, const Optional<PageLayoutId>& required_layout,
                           PinPageToJob pin_page_to_job, OkIfNotFound ok_if_not_found) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void finalized_prefetch_hint(PageId page_id, PageCache& cache) const;

  StatusOr<PinnedPage> finalized_get(PageId page_id, const Optional<PageLayoutId>& required_layout,
                                     OkIfNotFound ok_if_not_found) const;

  u64 job_id() const;

  Status await_durable() const;

 private:
  explicit FinalizedPageCacheJob(boost::intrusive_ptr<FinalizedJobTracker>&& tracker) noexcept;

  boost::intrusive_ptr<FinalizedJobTracker> tracker_;
};

// Write all changes in `job` to durable storage.  This is guaranteed to be atomic.
//
Status commit(CommittablePageCacheJob committable_job, const JobCommitParams& params, u64 callers);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Represents a unique PageCacheJob that has been finalized and is ready to be committed to durable
// storage.
//
class CommittablePageCacheJob
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<CommittablePageCacheJob> from(std::unique_ptr<PageCacheJob> job, u64 callers);

  friend Status commit(CommittablePageCacheJob committable_job, const JobCommitParams& params,
                       u64 callers);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  CommittablePageCacheJob() = default;

  // Moveable but not copyable.
  //
  CommittablePageCacheJob(const CommittablePageCacheJob&) = delete;
  CommittablePageCacheJob& operator=(const CommittablePageCacheJob&) = delete;

  CommittablePageCacheJob(CommittablePageCacheJob&&) = default;
  CommittablePageCacheJob& operator=(CommittablePageCacheJob&&) = default;

  // Sets the tracker status to kAborted if not already in a terminal state.
  //
  ~CommittablePageCacheJob() noexcept;

  // Create a new FinalizedPageCacheJob that points to this underlying job.  The returned object can
  // be used as the basis for future jobs so that we don't have to wait for all the new pages to be
  // written to device.
  //
  FinalizedPageCacheJob finalized_job() const;

  u64 job_id() const;

  BoxedSeq<PageId> new_page_ids() const;

  BoxedSeq<PageId> deleted_page_ids() const;

  BoxedSeq<PageRefCount> root_set_deltas() const;

  BoxedSeq<page_device_id_int> page_device_ids() const;

  explicit operator bool() const
  {
    return this->job_ && this->tracker_;
  }

 private:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit CommittablePageCacheJob(std::unique_ptr<PageCacheJob> finalized_job) noexcept;

  Status commit_impl(const JobCommitParams& params, u64 callers);

  Status write_new_pages(const JobCommitParams& params, u64 callers);

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
};

// Convenience shortcut for use cases where we do not pipeline job commits.
//
Status commit(std::unique_ptr<PageCacheJob> job, const JobCommitParams& params, u64 callers);

}  // namespace llfs

#endif  // LLFS_FINALIZED_PAGE_CACHE_JOB_HPP
