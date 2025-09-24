//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/finalized_page_cache_job.hpp>
//

#include <llfs/committable_page_cache_job.hpp>
#include <llfs/page_cache_job.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const FinalizedPageCacheJob& t)
{
  auto j = lock_job(t.tracker_.get());
  return out << "FinalizedPageCacheJob{.job=" << j << " (job_id=" << (j ? j->job_id : 0) << "),}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ FinalizedPageCacheJob::FinalizedPageCacheJob(
    boost::intrusive_ptr<FinalizedJobTracker>&& tracker) noexcept
    : tracker_{std::move(tracker)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FinalizedPageCacheJob::FinalizedPageCacheJob(const FinalizedPageCacheJob& other) noexcept
    : tracker_{other.tracker_}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FinalizedPageCacheJob::FinalizedPageCacheJob(FinalizedPageCacheJob&& other) noexcept
    : tracker_{std::move(other.tracker_)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FinalizedPageCacheJob& FinalizedPageCacheJob::operator=(const FinalizedPageCacheJob& other) noexcept
{
  this->tracker_ = other.tracker_;
  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FinalizedPageCacheJob& FinalizedPageCacheJob::operator=(FinalizedPageCacheJob&& other) noexcept
{
  this->tracker_ = std::move(other.tracker_);
  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 FinalizedPageCacheJob::job_id() const
{
  const std::shared_ptr<const PageCacheJob> job = lock_job(this->tracker_.get());
  if (!job) {
    return 0;
  }
  return job->job_id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache* FinalizedPageCacheJob::page_cache() const /*override*/
{
  const std::shared_ptr<const PageCacheJob> job = lock_job(this->tracker_.get());
  if (job != nullptr) {
    return job->page_cache();
  }
  return nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FinalizedPageCacheJob::prefetch_hint(PageId page_id) /*override*/
{
  const std::shared_ptr<const PageCacheJob> job = lock_job(this->tracker_.get());
  if (job != nullptr) {
    job->const_prefetch_hint(page_id);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> FinalizedPageCacheJob::try_pin_cached_page(
    PageId page_id, const PageLoadOptions& options) /*override*/
{
  const std::shared_ptr<const PageCacheJob> job = lock_job(this->tracker_.get());
  if (job != nullptr) {
    return job->const_try_pin_cached_page(page_id, options);
  }

  return {batt::StatusCode::kUnavailable};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> FinalizedPageCacheJob::load_page(PageId page_id,
                                                      const PageLoadOptions& options) /*override*/
{
  if (bool_from(options.pin_page_to_job(), /*default_value=*/false)) {
    return Status{batt::StatusCode::kUnimplemented};
  }

  return this->finalized_get(page_id, options);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FinalizedPageCacheJob::finalized_prefetch_hint(PageId page_id, PageCache& cache) const
{
  const std::shared_ptr<const PageCacheJob> job = lock_job(this->tracker_.get());
  if (job != nullptr) {
    BATT_CHECK_EQ(&cache, &(job->cache()));
    job->const_prefetch_hint(page_id);
  } else {
    cache.prefetch_hint(page_id);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> FinalizedPageCacheJob::finalized_get(PageId page_id,
                                                          const PageLoadOptions& options) const
{
  const std::shared_ptr<const PageCacheJob> job = lock_job(this->tracker_.get());
  if (job == nullptr) {
    if (this->tracker_ && this->tracker_->get_progress() == PageCacheJobProgress::kCancelled) {
      return Status{batt::StatusCode::kCancelled};
    }
    return Status{batt::StatusCode::kUnavailable};
  }

  Optional<PinnedPage> already_pinned = job->get_already_pinned(page_id);
  if (already_pinned) {
    return std::move(*already_pinned);
  }

  // Use the base job if it is available.
  //
  return job->const_get(page_id, options);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status FinalizedPageCacheJob::await_durable() const
{
  // Non-existent jobs are trivially durable, because by definition they have no durable
  // side-effects.
  //
  if (this->tracker_ == nullptr) {
    return OkStatus();
  }

  return this->tracker_->await_durable();
}

}  // namespace llfs
