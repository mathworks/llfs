//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FINALIZED_JOB_TRACKER_HPP
#define LLFS_FINALIZED_JOB_TRACKER_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_cache_job_progress.hpp>
#include <llfs/status.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/shared_ptr.hpp>

#include <memory>

namespace llfs {

class CommittablePageCacheJob;
class FinalizedJobTracker;
class PageCacheJob;

std::shared_ptr<const PageCacheJob> lock_job(const FinalizedJobTracker* tracker);

class FinalizedJobTracker : public boost::intrusive_ref_counter<FinalizedJobTracker>
{
 public:
  friend class CommittablePageCacheJob;

  using Self = FinalizedJobTracker;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  friend std::shared_ptr<const PageCacheJob> lock_job(const Self*);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit FinalizedJobTracker(const std::shared_ptr<const PageCacheJob>& job) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status await_durable();

  PageCacheJobProgress get_progress() const;

  void cancel();

 private:
  std::weak_ptr<const PageCacheJob> job_;
  batt::Watch<PageCacheJobProgress> progress_;
};

}  //namespace llfs

#endif  // LLFS_FINALIZED_JOB_TRACKER_HPP
