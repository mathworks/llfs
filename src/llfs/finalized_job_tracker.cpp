//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/finalized_job_tracker.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ FinalizedJobTracker::FinalizedJobTracker(
    const std::shared_ptr<const PageCacheJob>& job) noexcept
    : job_{job}
    , progress_{PageCacheJobProgress::kPending}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status FinalizedJobTracker::await_durable()
{
  StatusOr<PageCacheJobProgress> seen = this->progress_.await_true([](PageCacheJobProgress value) {
    return value == PageCacheJobProgress::kDurable || is_terminal_state(value);
  });

  BATT_REQUIRE_OK(seen);

  switch (*seen) {
    case PageCacheJobProgress::kPending:
      BATT_PANIC() << "Pending is not a terminal state!";
      BATT_UNREACHABLE();

    case PageCacheJobProgress::kCancelled:
      return batt::StatusCode::kCancelled;

    case PageCacheJobProgress::kDurable:
      return OkStatus();
  }

  return batt::StatusCode::kUnknown;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheJobProgress FinalizedJobTracker::get_progress() const
{
  return this->progress_.get_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FinalizedJobTracker::cancel()
{
  this->progress_.set_value(PageCacheJobProgress::kCancelled);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::shared_ptr<const PageCacheJob> lock_job(const FinalizedJobTracker* tracker)
{
  if (!tracker) {
    return nullptr;
  }
  return tracker->job_.lock();
}

}  //namespace llfs
