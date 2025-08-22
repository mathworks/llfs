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
#include <llfs/testing/test_config.hpp>

#include <llfs/int_types.hpp>
#include <llfs/interval.hpp>
#include <llfs/log_device.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_size.hpp>
#include <llfs/simulated_log_device.hpp>
#include <llfs/simulated_log_device_storage.hpp>
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

/** \brief A simulated collection of LogDevices and PageDevices, used for event-reordering and
 * failure-injection testing.
 */
class StorageSimulation
{
 public:
  /** \brief Forward-declaration of batt::TaskScheduler implementation.
   */
  class TaskSchedulerImpl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Creates a new StorageSimulation using a default random engine seeded with the passed
   * value.
   */
  explicit StorageSimulation(RandomSeed seed) noexcept;

  /** \brief Creates a new StorageSimulation context that draws from the passed entropy source.
   */
  explicit StorageSimulation(batt::StateMachineEntropySource&& entropy_source) noexcept;

  /** \brief Disallows copy-construction of StorageSimulation.
   */
  StorageSimulation(const StorageSimulation&) = delete;

  /** \brief Disallows copy-assignment of StorageSimulation.
   */
  StorageSimulation& operator=(const StorageSimulation&) = delete;

  /** \brief Destroys the StorageSimulation; all external references to this object and all
   * LogDevice and PageDevice objects obtained from it must have been released when this destructor
   * runs!
   */
  ~StorageSimulation() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void set_low_level_log_devices(bool on) noexcept
  {
    this->low_level_log_devices_ = on;
  }

  /** \brief Schedules the passed function (signature: void()) to run at some future point in the
   * simulation.
   *
   * The precise timing of when `fn` will run is controlled by the entropy source for the
   * simulation; this will always happen inside StorageSimulation::run_main_task, however.
   */
  template <typename Fn>
  void post(Fn&& fn)
  {
    boost::asio::post(this->fake_executor_, BATT_FORWARD(fn));
  }

  /** \brief Returns a reference to the shared PageCache for the simulation; the PageCache must have
   * already been created (by a call to init_cache()) or this function will panic.
   */
  const batt::SharedPtr<PageCache>& cache() const noexcept
  {
    BATT_CHECK_NOT_NULLPTR(this->cache_);
    return this->cache_;
  }

  /** \brief Creates and returns a shared PageCache for the simulation.
   *
   * This PageCache will be able to access all PageArenas added to the simulation (via
   * StorageSimulation::add_page_arena) prior to this call.
   */
  const batt::SharedPtr<PageCache>& init_cache() noexcept;

  /** \brief Forces the PageCache to be shut down; this will stop all background tasks associated
   * with the PageAllocators.
   */
  void close_cache(bool main_fn_done = false) noexcept;

  /** \brief Returns a reference to a TaskScheduler that should be used for all Tasks involved in
   * the simulation.
   *
   * All Task activations will be processed in a manner similar to `this->post`; thus in order to
   * have control over task scheduling events, the order in which mutexes are acquired, etc. this
   * scheduler must always be used for everything in the simulation.
   */
  batt::TaskScheduler& task_scheduler() noexcept;

  /** \brief Convenience: invokes this->task_scheduler().schedule_task() and returns the resulting
   * executor.
   */
  boost::asio::any_io_executor schedule_task() noexcept
  {
    return this->task_scheduler().schedule_task();
  }

  /** \brief Convenience: invokes this->entropy_source().pick_int(min_value, max_value).
   */
  usize pick_int(usize min_value, usize max_value) const
  {
    return this->entropy_source().pick_int(min_value, max_value);
  }

  /** \brief Convenience: invokes this->entropy_source().pick_branch().
   */
  bool pick_branch() const
  {
    return this->entropy_source().pick_branch();
  }

  /** \brief Returns a reference to the entropy source passed in at construction time.
   */
  batt::StateMachineEntropySource& entropy_source() noexcept
  {
    return this->entropy_source_;
  }

  /** \brief Returns a reference to the entropy source passed in at construction time.
   */
  const batt::StateMachineEntropySource& entropy_source() const noexcept
  {
    return this->entropy_source_;
  }

  /** \brief Returns whether the simulation is currently injecting failures randomly into system
   * components.
   *
   * All simulated I/O events will succeed (albeit in some unspecified pseudo-random order) when
   * "inject failures mode" is off.  When it is on, failures will be injected at a rate of 50%.
   */
  bool inject_failures_mode_on() const noexcept
  {
    return this->inject_failures_.get_value();
  }

  /** \brief Changes whether the simulation is in "inject failures mode."
   *
   * \see inject_failures_mode_on
   */
  void set_inject_failures_mode(bool on) noexcept
  {
    this->log_event("inject_failures_mode = ", on ? "ON" : "OFF");
    this->inject_failures_.set_value(on);
  }

  /** \brief Notifies the simulation of an opportunity to inject a failure; if inject failures mode
   * is on, then this function will consult the entropy source and return true if the caller should
   * behave as if a failure occurred.  The precise error status is specific to the context of the
   * caller.
   */
  bool inject_failure() noexcept
  {
    return this->inject_failures_mode_on() && this->entropy_source().pick_branch();
  }

  /** \brief Logs a simulation event, for diagnosing specific simulation scenarios.
   *
   * Simulation logging is enabled via env var GLOG_vmodule="storage_simulation=1".
   */
  template <typename... Args>
  void log_event(Args&&... args) const noexcept
  {
    LLFS_LOG_SIM_EVENT() << batt::to_string(BATT_FORWARD(args)...);
  }

  /** \brief Returns true iff the simulation is running.
   */
  bool is_running() const noexcept
  {
    return this->is_running_.load();
  }

  /** \brief Launches the "driver" (main) simulation task.  This is the entry point to the
   * simulation run.
   */
  void run_main_task(std::function<void()> main_fn);

  /** \brief Causes all LogDevice and PageDevice instances in the simulation to act as though the
   * process has crashed; this means all future operations will fail, and any ongoing/pending
   * operations will not affect the simulated "durable" contents of the devices.
   */
  void crash_and_recover();

  /** \brief Creates/accesses a simulated LogDeviceStorage object.
   *
   * \param name A unique name used to identify the LogDevice storage in the context of this
   * simulation
   */
  SimulatedLogDeviceStorage get_log_device_storage(const std::string& name, Optional<u64> capacity,
                                                   Optional<Interval<i64>> file_offset);

  /** \brief Creates/accesses a simulated LogDevice.
   *
   * \param name A unique name used to identify the LogDevice in the context of this simulation
   * \param capacity The maximum size in bytes of the log; must be specified the first time this
   * function is called for a given name (i.e., when the log is initially created)
   */
  std::unique_ptr<LogDevice> get_log_device(const std::string& name, Optional<u64> capacity = None);

  /** \brief Creates/accesses a simulated LogDevice via the LogDeviceFactory interface.
   *
   * \param name A unique name used to identify the LogDevice in the context of this simulation
   * \param capacity The maximum size in bytes of the log; must be specified the first time this
   * function is called for a given name (i.e., when the log is initially created)
   */
  std::unique_ptr<LogDeviceFactory> get_log_device_factory(const std::string& name,
                                                           Optional<u64> capacity = None);

  /** \brief Creates/accesses a simulated PageDevice.
   *
   * The args `page_count` and `page_size` must be specified the first time this
   * function is called for a given name (i.e., when the device is initially created).
   *
   * \param name A unique name used to identify the LogDevice in the context of this simulation
   * \param page_count The number of pages in the device
   * \param page_size The size (bytes) of each page in the device; must be a power of 2,
   * >=kDirectIOBlockSize
   */
  std::unique_ptr<PageDevice> get_page_device(const std::string& name,
                                              Optional<PageCount> page_count = None,
                                              Optional<PageSize> page_size = None);

  /** \brief Adds a PageDevice and LogDevice (for a PageAllocator) with the given page size and
   * count.
   *
   * The newly added arena will be present in any future PageCache instances created by this
   * simulation.
   */
  void add_page_arena(PageCount page_count, PageSize page_size);

  /** \brief Registers the given page format with all future instances of PageCache created by the
   * simulation.
   */
  void register_page_reader(const PageLayoutId& layout_id, const char* file, int line,
                            const PageReader& reader);

  /** \brief Creates/accesses a simulated Volume.
   *
   * `trim_control` and `volume_options` are always optional; `root_log_capacity` must be specified
   * the first time this function is called for a given name to create the volume.
   *
   * LogDevices are created for the main log and recycler, based on the passed name.
   */
  StatusOr<std::unique_ptr<Volume>> get_volume(
      const std::string& name, const VolumeReader::SlotVisitorFn& slot_visitor_fn,
      const Optional<u64> root_log_capacity = None,
      const Optional<VolumeOptions>& volume_options = None,
      std::shared_ptr<SlotLockManager> trim_control = nullptr);

  /** \brief Returns the current simulation step.
   *
   * A simulation step is defined as running a single Task in the simulation until it yields or is
   * suspended awaiting some event.
   */
  u64 current_step() const noexcept
  {
    return this->step_.get_value();
  }

  /** \brief Blocks the calling task until the simulation step has reached or exceeded the specified
   * value.
   *
   * \return the new step value, or error code if the simulation was interrupted/ended before
   * the target step was reached.
   */
  StatusOr<u64> await_step(u64 minimum_step) noexcept
  {
    return this->step_.await_true([minimum_step](u64 observed_step) {
      return observed_step >= minimum_step;
    });
  }

  /** \brief Returns true if all blocks for a given page are present in a simulated PageDevice,
   * false if they are all absent, error status code otherwise.
   */
  StatusOr<bool> has_data_for_page_id(PageId page_id) const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct PageArenaDeviceNames {
    std::string page_device_name;
    std::string allocator_log_device_name;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Step the simulation forward repeatedly until there are no pending handlers that can
   * run.
   */
  void handle_events(bool main_fn_done);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Reads test configuration from the environment.
  //
  testing::TestConfig test_config_;

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

  // The set of all simulated log device storage (low-level), indexed by name string (passed in by
  // the test code).
  //
  std::unordered_map<std::string, std::shared_ptr<SimulatedLogDeviceStorage::DurableState>>
      log_storage_;

  // The set of all simulated page devices, indexed by name string (passed in by the test code).
  //
  std::unordered_map<std::string, std::shared_ptr<SimulatedPageDevice::Impl>> page_devices_;

  // Map from page_device_id to impl.
  //
  std::unordered_map<page_device_id_int, std::shared_ptr<SimulatedPageDevice::Impl>>
      page_device_id_map_;

  // A counter to make unique object names.
  //
  std::atomic<i64> counter_{0};

  // Page readers that should be registered with PageCache instances on each recovery.
  //
  std::unordered_map<PageLayoutId, PageCache::PageReaderFromFile, PageLayoutId::Hash> page_readers_;

  // Are we in low-level LogDevice simulation mode?
  //
  bool low_level_log_devices_ = this->test_config_.low_level_log_device_sim();

  // Set to true when we are inside the main simulation loop.
  //
  std::atomic<bool> is_running_{false};
};

}  //namespace llfs

#endif  // LLFS_STORAGE_SIMULATION_HPP
