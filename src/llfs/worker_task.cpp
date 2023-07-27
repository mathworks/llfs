//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/worker_task.hpp>
//

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class WorkQueue

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void WorkQueue::close() noexcept
{
  const u64 prior_state = this->state_.fetch_or(WorkQueue::kClosedFlag);

  LLFS_VLOG(1) << "WorkQueue::close() halting all idle workers";
  this->halt_all_idle_workers(prior_state | WorkQueue::kClosedFlag);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void WorkQueue::halt_all_idle_workers(u64 observed_state) noexcept
{
  while (Self::get_worker_count(observed_state) > 0 && Self::get_job_count(observed_state) == 0) {
    u64 worker_count = Self::get_worker_count(observed_state);

    const u64 target_state = WorkQueue::kClosedFlag;
    if (!this->state_.compare_exchange_weak(observed_state, target_state)) {
      continue;
    }

    while (worker_count > 0) {
      WorkerTask* worker = nullptr;

      BATT_CHECK(this->worker_queue_.pop(worker));
      BATT_CHECK_NOT_NULLPTR(worker);

      worker->halt();
      --worker_count;
    }
    break;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status WorkQueue::push_worker(WorkerTask* worker)
{
  LLFS_VLOG(1) << "WorkQueue::push_worker";
  {
    const u64 observed_state = this->state_.load();

    if (Self::is_closed_state(observed_state) && Self::get_job_count(observed_state) == 0) {
      LLFS_VLOG(1) << " -- WorkQueue is closed and drained; halting worker";
      worker->halt();
      return batt::StatusCode::kClosed;
    }
  }

  if (!this->worker_queue_.push(worker)) {
    return batt::StatusCode::kUnavailable;
  }

  const u64 observed_state = this->state_.fetch_add(kWorkerIncrement) + kWorkerIncrement;

  LLFS_VLOG(1) << " --" << std::hex << BATT_INSPECT(observed_state);

  if (Self::is_closed_state(observed_state) && Self::get_job_count(observed_state) == 0) {
    LLFS_VLOG(1) << " -- WorkQueue is closed and drained; halting all idle workers";
    this->halt_all_idle_workers(observed_state);
    return batt::StatusCode::kClosed;
  }

  return this->dispatch(observed_state, __FUNCTION__);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status WorkQueue::dispatch(u64 observed_state, const char* from)
{
  LLFS_VLOG(1) << "WorkQueue::dispatch()" << BATT_INSPECT_STR(from);

  for (;;) {
    const auto worker_count = Self::get_worker_count(observed_state);
    const auto job_count = Self::get_job_count(observed_state);

    LLFS_VLOG(1) << " --" << BATT_INSPECT(worker_count) << BATT_INSPECT(job_count);

    if (worker_count == 0 || job_count == 0) {
      LLFS_VLOG(1) << " -- Nothing more we can do (idle state) returning";
      break;
    }

    const u64 target_state = observed_state - (kJobIncrement | kWorkerIncrement);

    if (this->state_.compare_exchange_weak(observed_state, target_state)) {
      LLFS_VLOG(1) << " -- Found worker/job pair; dispatching!";

      Job* next_job = nullptr;
      WorkerTask* next_worker = nullptr;

      BATT_CHECK(this->worker_queue_.pop(next_worker));
      BATT_CHECK(this->job_queue_.pop(next_job));

      BATT_CHECK_NOT_NULLPTR(next_job);
      BATT_CHECK_NOT_NULLPTR(next_worker);

      auto on_scope_exit = batt::finally([&] {
        next_job->~Job();
        usize job_i = next_job - ((Job*)this->job_storage_.data());
        BATT_CHECK(this->storage_queue_.push(job_i));
      });

      BATT_REQUIRE_OK(next_worker->dispatch_job(std::move(*next_job)));

      LLFS_VLOG(1) << " -- Handed job off successfully!";

      observed_state = target_state;
    }
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<usize> WorkQueue::allocate_job_slot()
{
  // Initialize the storage slot index `i` to an out-of-bounds value.
  //
  usize i = ~0;

  // Try to grab an available slot; if we fail, then try to increase the init upper bound.
  //
  if (!this->storage_queue_.pop(i)) {
    i = this->storage_init_upper_bound_.fetch_add(1);

    // If we are already at the limit, then undo the increment we just did and return failure (no
    // space available).
    //
    if (i >= this->job_storage_.size()) {
      this->storage_init_upper_bound_.fetch_sub(1);
      return {batt::Status{batt::StatusCode::kUnavailable}};
    }
  }

  return {i};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status WorkQueue::post_job(Job* job)
{
  // This should never fail because we succeeded in allocating a storage slot.
  //
  BATT_CHECK(this->job_queue_.push(job));

  const u64 observed_state = this->state_.fetch_add(kJobIncrement) + kJobIncrement;

  return this->dispatch(observed_state, __FUNCTION__);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class WorkerTask

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status WorkerTask::dispatch_job(WorkQueue::Job&& job)
{
  u32 observed_state = this->state_.get_value();

  if (observed_state == WorkerTask::kHaltedState) {
    return batt::StatusCode::kClosed;
  }
  BATT_CHECK_EQ(observed_state, WorkerTask::kReadyState);
  this->job_.emplace(std::move(job));

  const u32 prior_state = this->state_.set_value(WorkerTask::kWorkingState);
  BATT_CHECK_EQ(prior_state, observed_state);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void WorkerTask::halt()
{
  const u32 prior_state = this->state_.set_value(WorkerTask::kHaltedState);
  BATT_CHECK_EQ(prior_state, WorkerTask::kReadyState);

  this->state_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void WorkerTask::run()
{
  LLFS_VLOG(1) << "WorkerTask@" << (void*)this << " ENTERED";

  batt::Status status = [&]() -> batt::Status {
    while (!this->halt_requested_.load()) {
      LLFS_VLOG(1) << "WorkerTask@" << (void*)this << " Pushing self to work queue";

      BATT_CHECK_EQ(this->state_.get_value(), WorkerTask::kReadyState);
      BATT_REQUIRE_OK(this->work_queue_->push_worker(this));

      BATT_REQUIRE_OK(this->state_.await_equal(WorkerTask::kWorkingState));
      BATT_CHECK_NE(this->job_, batt::None);

      LLFS_VLOG(1) << "WorkerTask@" << (void*)this << " Got next job; running!";

      try {
        auto on_scope_exit = batt::finally([&] {
          this->job_ = batt::None;
        });
        this->job_->work_fn();
      } catch (...) {
        LLFS_LOG_ERROR() << "Unexpected exception TODO [tastolfi 2023-06-29] print details";
      }

      LLFS_VLOG(1) << "WorkerTask@" << (void*)this << " job done!";

      const u32 prior_state = this->state_.set_value(WorkerTask::kReadyState);
      BATT_CHECK_EQ(prior_state, WorkerTask::kWorkingState);
    }

    return batt::OkStatus();
  }();

  if (!status.ok()) {
    if (this->halt_requested_.load() || status == batt::StatusCode::kClosed) {
      LLFS_VLOG(1) << "WorkerTask terminated with status: " << status;
    } else {
      LLFS_LOG_WARNING() << "WorkerTask terminated unexpectedly with error status: " << status;
    }
  }
}

}  //namespace llfs
