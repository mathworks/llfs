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

#include <batteries/async/task.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/optional.hpp>
#include <batteries/small_fn.hpp>

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>

#include <array>
#include <atomic>
#include <type_traits>

namespace llfs {

class WorkerTask;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class WorkQueue
{
 public:
  // These can be changed to tune the WorkQueue.
  //
  static constexpr usize kMaxDepth = 4096;
  static constexpr usize kWorkFnMaxSize = 256;

  using Self = WorkQueue;

  struct Job {
    batt::SmallFn<void(), kWorkFnMaxSize> work_fn;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u64 kWorkerIncrement = 1;
  static constexpr u64 kWorkerCountMask = (u64{1} << 31) - 1;
  static constexpr u64 kWorkerCountShift = 0;

  static constexpr u64 kJobIncrement = kWorkerIncrement << 31;
  static constexpr u64 kJobCountMask = kWorkerCountMask << 31;
  static constexpr u64 kJobCountShift = 31;

  static constexpr u64 kClosedFlag = u64{1} << 63;

  static constexpr bool is_closed_state(u64 state) noexcept
  {
    return (state & kClosedFlag) != 0;
  }

  static constexpr u64 get_worker_count(u64 state) noexcept
  {
    return (state & kWorkerCountMask) >> kWorkerCountShift;
  }

  static constexpr u64 get_job_count(u64 state) noexcept
  {
    return (state & kJobCountMask) >> kJobCountShift;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The first call to close causes the queue to go into a "draining" state; push_job will
   * fail with batt::StatusCode::kClosed, but push_worker will continue to succeed until there are
   * no more jobs left, then it will also fail with batt::StatusCode::kClosed.
   *
   * This draining method is the best we can do without an interface for job cancellation.
   */
  void close() noexcept;

  bool is_closed() const noexcept
  {
    return Self::is_closed_state(this->state_.load());
  }

  template <typename WorkFnArg>
  batt::Status push_job(WorkFnArg&& arg)
  {
    if (this->is_closed()) {
      return batt::StatusCode::kClosed;
    }

    // Allocate a free slot for the job.
    //
    BATT_ASSIGN_OK_RESULT(const usize i, this->allocate_job_slot());

    // We have a currently unused storage slot.  Initialize it with the passed arg.
    //
    Job* job = new (&this->job_storage_[i]) Job{{BATT_FORWARD(arg)}};

    // Post the job; this will automatically dispatch it if a worker is available.
    //
    return this->post_job(job);
  }

  batt::Status push_worker(WorkerTask* worker);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  batt::StatusOr<usize> allocate_job_slot();

  batt::Status post_job(Job* job);

  batt::Status dispatch(u64 observed_state, const char* from);

  void halt_all_idle_workers(u64 observed_state) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<u64> state_{0};

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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class WorkerTask
{
 public:
  static constexpr u32 kReadyState = 0;
  static constexpr u32 kWorkingState = 1;
  static constexpr u32 kHaltedState = 2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename... TaskArgs>
  explicit WorkerTask(batt::SharedPtr<WorkQueue>&& work_queue,
                      const boost::asio::any_io_executor& ex, TaskArgs&&... task_args) noexcept
      : work_queue_{std::move(work_queue)}
      , task_{ex,
              [this] {
                this->run();
              },
              BATT_FORWARD(task_args)...}
      , job_{}
      , state_{WorkerTask::kReadyState}
  {
  }

  batt::Status dispatch_job(WorkQueue::Job&& job);

  void pre_halt()
  {
    this->halt_requested_.store(true);
  }

  void halt();

  void join()
  {
    this->task_.join();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void run();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::SharedPtr<WorkQueue> work_queue_;
  batt::Task task_;
  batt::Optional<WorkQueue::Job> job_;
  batt::Watch<u32> state_;
  std::atomic<bool> halt_requested_{false};
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  //namespace llfs

#endif  // LLFS_WORKER_TASK_HPP
