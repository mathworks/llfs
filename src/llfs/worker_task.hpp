//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_WORKER_TASK_HPP
#define LLFS_WORKER_TASK_HPP

#include <llfs/config.hpp>
//
#include <llfs/logging.hpp>

#include <batteries/async/channel.hpp>
#include <batteries/async/task.hpp>
#include <batteries/small_fn.hpp>

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>

#include <array>
#include <atomic>
#include <type_traits>

namespace llfs {

class WorkerTask;

class WorkQueue
{
 public:
  static constexpr usize kMaxDepth = 4096;

  using Self = WorkQueue;

  struct Job {
    batt::SmallFn<void(), 256> work_fn;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u64 kWorkerIncrement = 1;
  static constexpr u64 kWorkerCountMask = (u64{1} << 32) - 1;
  static constexpr u64 kWorkerCountShift = 0;

  static constexpr u64 kJobIncrement = kWorkerIncrement << 32;
  static constexpr u64 kJobCountMask = kWorkerCountMask << 32;
  static constexpr u64 kJobCountShift = 32;

  static constexpr u64 get_worker_count(u64 state) noexcept
  {
    return (state & kWorkerCountMask) >> kWorkerCountShift;
  }

  static constexpr u64 get_job_count(u64 state) noexcept
  {
    return (state & kJobCountMask) >> kJobCountShift;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename WorkFnArg>
  batt::Status push_job(WorkFnArg&& arg)
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
        return batt::StatusCode::kUnavailable;
      }
    }

    // We have a currently unused storage slot.  Initialize it with the passed arg.
    //
    Job* job = new (&this->job_storage_[i]) Job{{BATT_FORWARD(arg)}};

    // This should never fail because we succeeded in allocating a storage slot.
    //
    BATT_CHECK(this->job_queue_.push(job));

    const u64 observed_state = this->state_.fetch_add(kJobIncrement) + kJobIncrement;

    return this->dispatch(observed_state);
  }

  batt::Status push_worker(WorkerTask* worker)
  {
    if (!this->worker_queue_.push(worker)) {
      return batt::StatusCode::kUnavailable;
    }

    const u64 observed_state = this->state_.fetch_add(kWorkerIncrement) + kWorkerIncrement;

    return this->dispatch(observed_state);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  batt::Status dispatch(u64 observed_state);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<u64> state_;

  std::atomic<usize> storage_init_upper_bound_{0};

  std::array<std::aligned_storage_t<sizeof(Job)>, kMaxDepth> job_storage_;

  boost::lockfree::queue<WorkerTask*, boost::lockfree::capacity<kMaxDepth>,
                         boost::lockfree::fixed_sized<true>>
      worker_queue_;

  boost::lockfree::queue<Job*, boost::lockfree::capacity<kMaxDepth>,
                         boost::lockfree::fixed_sized<true>>
      job_queue_;

  boost::lockfree::queue<usize, boost::lockfree::capacity<kMaxDepth>,
                         boost::lockfree::fixed_sized<true>>
      storage_queue_;
};

class WorkerTask
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename... TaskArgs>
  explicit WorkerTask(std::shared_ptr<WorkQueue>&& work_queue,
                      const boost::asio::any_io_executor& ex, TaskArgs&&... task_args) noexcept
      : work_queue_{std::move(work_queue)}
      , task_{ex,
              [this] {
                this->run();
              },
              BATT_FORWARD(task_args)...}
  {
  }

  batt::Status push_job(WorkQueue::Job&& job)
  {
    return this->work_channel_.write(job);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void run()
  {
    batt::Status status = [&]() -> batt::Status {
      while (!this->halt_requested_.load()) {
        BATT_REQUIRE_OK(this->work_queue_->push_worker(this));

        batt::StatusOr<WorkQueue::Job&> next_job_ref = this->work_channel_.read();
        BATT_REQUIRE_OK(next_job_ref);

        WorkQueue::Job next_job = std::move(*next_job_ref);

        this->work_channel_.consume();

        try {
          next_job.work_fn();
        } catch (...) {
          LLFS_LOG_ERROR() << "Unexpected exception TODO [tastolfi 2023-06-29] print details";
        }
      }

      return batt::OkStatus();
    }();

    if (!status.ok()) {
      if (this->halt_requested_.load()) {
        LLFS_VLOG(1) << "WorkerTask terminated with status: " << status;
      } else {
        LLFS_LOG_WARNING() << "WorkerTask terminated unexpectedly with error status: " << status;
      }
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::shared_ptr<WorkQueue> work_queue_;
  batt::Task task_;
  batt::Channel<WorkQueue::Job> work_channel_;
  std::atomic<bool> halt_requested_{false};
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

inline batt::Status WorkQueue::dispatch(u64 observed_state)
{
  while (Self::get_worker_count(observed_state) > 0 && Self::get_job_count(observed_state) > 0) {
    const u64 target_state = observed_state - (kJobIncrement | kWorkerIncrement);

    if (this->state_.compare_exchange_weak(observed_state, target_state)) {
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

      BATT_REQUIRE_OK(next_worker->push_job(std::move(*next_job)));
    }
  }
  return batt::OkStatus();
}

}  //namespace llfs

#endif  // LLFS_WORKER_TASK_HPP
