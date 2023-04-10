//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_SIMULATION_HPP
#define LLFS_STORAGE_SIMULATION_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/log_device.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_size.hpp>
#include <llfs/simulated_log_device.hpp>
#include <llfs/simulated_page_device.hpp>
#include <llfs/slot.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/fake_execution_context.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/state_machine_model/entropy_source.hpp>
#include <batteries/stream_util.hpp>
#include <batteries/utility.hpp>

#include <boost/asio/post.hpp>

#include <memory>

namespace llfs {

#define LLFS_LOG_SIM_EVENT() LLFS_VLOG(1)

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class StorageSimulation
{
 public:
  class TaskSchedulerImpl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit StorageSimulation(batt::StateMachineEntropySource&& entropy_source) noexcept;

  StorageSimulation(const StorageSimulation&) = delete;
  StorageSimulation& operator=(const StorageSimulation&) = delete;

  ~StorageSimulation() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename Fn>
  void post(Fn&& fn)
  {
    boost::asio::post(this->fake_executor_, BATT_FORWARD(fn));
  }

  const batt::SharedPtr<PageCache>& cache() const noexcept
  {
    BATT_CHECK_NOT_NULLPTR(this->cache_);
    return this->cache_;
  }

  const batt::SharedPtr<PageCache>& init_cache() noexcept;

  batt::TaskScheduler& task_scheduler() noexcept;

  batt::StateMachineEntropySource& entropy_source() noexcept
  {
    return this->entropy_source_;
  }

  bool inject_failures_mode_on() const noexcept
  {
    return this->inject_failures_.get_value();
  }

  void set_inject_failures_mode(bool on) noexcept
  {
    this->inject_failures_.set_value(on);
  }

  bool inject_failure() noexcept
  {
    return this->inject_failures_mode_on() && this->entropy_source().pick_branch();
  }

  template <typename... Args>
  void log_event(Args&&... args) const noexcept
  {
    LLFS_LOG_SIM_EVENT() << batt::to_string(BATT_FORWARD(args)...);
  }

  void run_main_task(std::function<void()> main_fn);

  std::unique_ptr<LogDevice> get_log_device(const std::string& name, Optional<u64> capacity = None);

  std::unique_ptr<PageDevice> get_page_device(const std::string& name,
                                              Optional<PageCount> page_count = None,
                                              Optional<PageSize> page_size = None);

  void add_page_arena(PageCount page_count, PageSize page_size);

  StatusOr<std::unique_ptr<Volume>> get_volume(
      const std::string& name, const VolumeReader::SlotVisitorFn& slot_visitor_fn,
      const Optional<u64> root_log_capacity = None,
      const Optional<VolumeOptions>& volume_options = None,
      std::shared_ptr<SlotLockManager> trim_control = nullptr);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct PageArenaDeviceNames {
    std::string page_device_name;
    std::string allocator_log_device_name;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Used to select event orderings, operations into which to inject failures, etc.
  //
  batt::StateMachineEntropySource entropy_source_;

  // The logical simulation time.
  //
  batt::Watch<u64> step_{0};

  // Used for all tasks and I/O scheduling within the simulation, so we can control event orderings.
  //
  batt::FakeExecutionContext fake_io_context_;

  // An executor for `fake_io_context_`, cached for convenience.
  //
  batt::FakeExecutionContext::executor_type fake_executor_ = this->fake_io_context_.get_executor();

  // All tasks created in the simulation should use this scheduler (which will always return
  // `fake_executor_`).
  //
  std::unique_ptr<TaskSchedulerImpl> task_scheduler_impl_;

  // When true, failures will be injected into I/O operations according to the entropy source; when
  // false, no failures will be injected.
  //
  batt::Watch<bool> inject_failures_{false};

  // Used to create the PageArena objects that will be used to initialize the cache when
  // `init_cache()` is invoked.
  //
  std::vector<PageArenaDeviceNames> page_arena_device_names_;

  // PageCache instance for the simulation.
  //
  batt::SharedPtr<PageCache> cache_;

  // The set of all simulated log devices, indexed by name string (passed in by the test code).
  //
  std::unordered_map<std::string, std::shared_ptr<SimulatedLogDevice::Impl>> log_devices_;

  // The set of all simulated page devices, indexed by name string (passed in by the test code).
  //
  std::unordered_map<std::string, std::shared_ptr<SimulatedPageDevice::Impl>> page_devices_;

  // A counter to make unique object names.
  //
  std::atomic<i64> counter_{0};
};

}  //namespace llfs

#endif  // LLFS_STORAGE_SIMULATION_HPP
