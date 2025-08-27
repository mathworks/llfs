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

#include <llfs/finalized_job_tracker.hpp>
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
class CommittablePageCacheJob;

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

  PageCache* page_cache() const override;

  void prefetch_hint(PageId page_id) override;

  StatusOr<PinnedPage> try_pin_cached_page(PageId page_id, const PageLoadOptions& options) override;

  StatusOr<PinnedPage> load_page(PageId page_id, const PageLoadOptions& options) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void finalized_prefetch_hint(PageId page_id, PageCache& cache) const;

  StatusOr<PinnedPage> finalized_get(PageId page_id, const PageLoadOptions& options) const;

  u64 job_id() const;

  Status await_durable() const;

 private:
  explicit FinalizedPageCacheJob(boost::intrusive_ptr<FinalizedJobTracker>&& tracker) noexcept;

  boost::intrusive_ptr<FinalizedJobTracker> tracker_;
};

std::ostream& operator<<(std::ostream& out, const FinalizedPageCacheJob& t);

}  // namespace llfs

#endif  // LLFS_FINALIZED_PAGE_CACHE_JOB_HPP
