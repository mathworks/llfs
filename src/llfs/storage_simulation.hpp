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
#include <llfs/page_size.hpp>
#include <llfs/simulated_log_device.hpp>
#include <llfs/simulated_page_device.hpp>
#include <llfs/slot.hpp>

#include <batteries/async/fake_execution_context.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/state_machine_model/entropy_source.hpp>
#include <batteries/utility.hpp>

#include <boost/asio/post.hpp>

#include <memory>

namespace llfs {

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename Fn>
  void post(Fn&& fn)
  {
    boost::asio::post(this->fake_executor_, BATT_FORWARD(fn));
  }

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

  std::unique_ptr<LogDevice> get_log_device(const std::string& name, Optional<u64> capacity = None);

  std::unique_ptr<PageDevice> get_page_device(const std::string& name,
                                              Optional<PageCount> page_count = None,
                                              Optional<PageSize> page_size = None);

 private:
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

  // The set of all simulated log devices, indexed by name string (passed in by the test code).
  //
  std::unordered_map<std::string, std::shared_ptr<SimulatedLogDevice::Impl>> log_devices_;

  // The set of all simulated page devices, indexed by name string (passed in by the test code).
  //
  std::unordered_map<std::string, std::shared_ptr<SimulatedPageDevice::Impl>> page_devices_;
};

}  //namespace llfs

#endif  // LLFS_STORAGE_SIMULATION_HPP
