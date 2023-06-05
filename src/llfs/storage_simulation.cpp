//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_simulation.hpp>
//

#include <llfs/simulated_log_device.hpp>
#include <llfs/simulated_log_device_impl.hpp>
#include <llfs/simulated_page_device.hpp>
#include <llfs/simulated_page_device_impl.hpp>

namespace llfs {

class StorageSimulation::TaskSchedulerImpl : public batt::TaskScheduler
{
 public:
  explicit TaskSchedulerImpl(StorageSimulation& simulation) noexcept : simulation_{simulation}
  {
  }

  boost::asio::any_io_executor schedule_task() override
  {
    return this->simulation_.fake_executor_;
  }

  void halt() override
  {
    //TODO [tastolfi 2023-04-05]
  }

  void join() override
  {
    //TODO [tastolfi 2023-04-05]
  }

 private:
  StorageSimulation& simulation_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ StorageSimulation::StorageSimulation(
    batt::StateMachineEntropySource&& entropy_source) noexcept
    : entropy_source_{std::move(entropy_source)}
    , task_scheduler_impl_{std::make_unique<TaskSchedulerImpl>(*this)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StorageSimulation::~StorageSimulation() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::TaskScheduler& StorageSimulation::task_scheduler() noexcept
{
  return *this->task_scheduler_impl_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageSimulation::run_main_task(std::function<void()> main_fn)
{
  LLFS_LOG_SIM_EVENT() << "entered run_main_task();" << BATT_INSPECT(batt::this_thread_id());
  auto on_scope_exit = batt::finally([&] {
    LLFS_LOG_SIM_EVENT() << "leaving run_main_task()";
  });

  batt::Task main_task{this->task_scheduler().schedule_task(), main_fn, "SimulationMainTask"};

  //----- --- -- -  -  -   -
  // Run the main task until there are no more events to process (either we have terminated or
  // deadlocked).
  //
  this->handle_events();
  //
  LLFS_LOG_SIM_EVENT() << "done handling events." << BATT_INSPECT(main_task.is_done());

  //----- --- -- -  -  -   -
  // Shut down the cache and call `handle_events` again to allow it to terminate all tasks
  // gracefully.
  //
  this->close_cache();

  //----- --- -- -  -  -   -
  // The main task should be terminated at this point.
  //
  BATT_CHECK_EQ(main_task.try_join(), batt::Task::IsDone{true})
      << "main task failed to terminate (possible deadlock?)" << [&main_task](std::ostream& out) {
           batt::print_debug_info(main_task.debug_info, out);
         };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageSimulation::crash_and_recover()
{
  const u64 recover_step = this->step_.fetch_add(1) + 1;

  LLFS_LOG_SIM_EVENT() << "entered crash_and_recover() step=" << recover_step;
  auto on_scope_exit = batt::finally([&] {
    LLFS_LOG_SIM_EVENT() << "leaving crash_and_recover()";
  });

  for (const auto& [name, p_log_device_impl] : this->log_devices_) {
    p_log_device_impl->crash_and_recover(recover_step);
  }
  for (const auto& [name, p_page_device_impl] : this->page_devices_) {
    p_page_device_impl->crash_and_recover(recover_step);
  }

  this->close_cache();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageSimulation::close_cache() noexcept
{
  if (this->cache_) {
    LLFS_LOG_SIM_EVENT() << "shutting down the PageCache...";
    this->cache_->close();
    //
    this->handle_events();
    //
    this->cache_->join();
    this->cache_ = nullptr;
    LLFS_LOG_SIM_EVENT() << "PageCache shut down successfully";
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageSimulation::handle_events()
{
  for (;;) {
    const u64 step = this->step_.fetch_add(1) + 1;
    batt::UniqueHandler<> next_event_handler =
        this->fake_io_context_.pop_ready_handler([this, step](usize n) -> usize {
          BATT_CHECK_GT(n, 0);
          usize i = this->entropy_source().pick_int(0, n - 1);
          LLFS_LOG_SIM_EVENT() << "selected event index " << (i + 1) << " of " << n
                               << "; step=" << step;
          return i;
        });

    if (!next_event_handler) {
      LLFS_LOG_SIM_EVENT() << "no more events to handle";
      return;
    }

    try {
      next_event_handler();
    } catch (...) {
      LLFS_LOG_ERROR() << "event handler threw exception!";
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<LogDevice> StorageSimulation::get_log_device(const std::string& name,
                                                             Optional<u64> capacity)
{
  auto iter = this->log_devices_.find(name);

  // If we didn't find the named device, then create it.
  //
  if (iter == this->log_devices_.end()) {
    BATT_CHECK(capacity)
        << "Must specify capacity if creating a simulated log device for the first time!";

    iter = this->log_devices_
               .emplace(name, std::make_shared<SimulatedLogDevice::Impl>(*this, name, *capacity))
               .first;
  }

  // At this point we should have a valid entry.
  //
  BATT_CHECK_NE(iter, this->log_devices_.end());
  BATT_CHECK_NOT_NULLPTR(iter->second);

  LLFS_LOG_SIM_EVENT() << "creating SimulatedLogDevice " << batt::c_str_literal(name);

  return std::make_unique<SimulatedLogDevice>(batt::make_copy(iter->second));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<PageDevice> StorageSimulation::get_page_device(const std::string& name,
                                                               Optional<PageCount> page_count,
                                                               Optional<PageSize> page_size)
{
  auto iter = this->page_devices_.find(name);

  // If we didn't find the named device, then create it.
  //
  if (iter == this->page_devices_.end()) {
    BATT_CHECK(page_count && page_size)
        << "Must specify page count/size if creating a simulated page device for the first time!";

    const page_device_id_int next_page_device_id = this->page_devices_.size();

    iter = this->page_devices_
               .emplace(name, std::make_shared<SimulatedPageDevice::Impl>(
                                  *this, name, *page_size, *page_count, next_page_device_id))
               .first;

    BATT_CHECK_NOT_NULLPTR(iter->second);

    this->page_device_id_map_.emplace(next_page_device_id, iter->second);
  }

  // At this point we should have a valid entry.
  //
  BATT_CHECK_NE(iter, this->page_devices_.end());
  BATT_CHECK_NOT_NULLPTR(iter->second);

  LLFS_LOG_SIM_EVENT() << "creating SimulatedPageDevice " << batt::c_str_literal(name);

  return std::make_unique<SimulatedPageDevice>(batt::make_copy(iter->second));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageSimulation::add_page_arena(PageCount page_count, PageSize page_size)
{
  PageArenaDeviceNames arena;

  //----- --- -- -  -  -   -
  // Helper - returns a name string including the passed prefix, the page_count and page_size, and a
  // fresh counter value.
  //
  const auto generate_candidate_name = [&](std::string_view prefix) {
    return batt::to_string(prefix, "_", page_size, "x", page_count, "_",
                           this->counter_.fetch_add(1));
  };

  //----- --- -- -  -  -   -
  // Generate a unique SimulatedPageDevice name and create the Impl for it, setting
  // `arena.page_device_name`.
  //
  for (;;) {
    const std::string candidate_name = generate_candidate_name("PageDevice");
    if (this->page_devices_.count(candidate_name)) {
      continue;
    }
    if (this->get_page_device(candidate_name, page_count, page_size) != nullptr) {
      arena.page_device_name = candidate_name;
      break;
    }
  }

  //----- --- -- -  -  -   -
  // Generate a unique SimulatedLogDevice name and create the Impl for it, setting
  // `arena.allocator_log_device_name`.
  //
  for (;;) {
    const std::string candidate_name = generate_candidate_name("PageAllocatorLog");
    if (this->log_devices_.count(candidate_name)) {
      continue;
    }
    if (this->get_log_device(candidate_name, /*capacity=*/PageAllocator::calculate_log_size(
                                 page_count, /*max_attachments=*/64)) != nullptr) {
      arena.allocator_log_device_name = candidate_name;
      break;
    }
  }

  // Because the PageCache may need to be created multiple times (if we simulate crash/recovery
  // cycles), we just save the device names and create the actual PageArena object just before
  // initializing the PageCache.
  //
  this->page_arena_device_names_.emplace_back(std::move(arena));

  LLFS_LOG_SIM_EVENT() << "added page arena, page_count=" << page_count
                       << ", page_size=" << page_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageSimulation::register_page_layout(const PageLayoutId& layout_id,
                                             const PageReader& reader)
{
  this->page_layouts_.emplace(layout_id, reader);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const batt::SharedPtr<PageCache>& StorageSimulation::init_cache() noexcept
{
  if (!this->cache_) {
    this->log_event("creating PageCache");

    std::vector<PageArena> arenas;
    for (const PageArenaDeviceNames& arena : this->page_arena_device_names_) {
      //----- --- -- -  -  -   -
      std::unique_ptr<PageDevice> page_device = this->get_page_device(arena.page_device_name);

      const PageIdFactory page_ids = page_device->page_ids();

      arenas.emplace_back(
          PageArena{std::move(page_device),
                    PageAllocator::recover_or_die(
                        PageAllocatorRuntimeOptions{
                            .scheduler = this->task_scheduler(),
                            .name = arena.allocator_log_device_name,
                        },
                        page_ids, *std::make_unique<BasicLogDeviceFactory>([this, &arena] {
                          return this->get_log_device(arena.allocator_log_device_name);
                        }))});
    }
    this->cache_ = BATT_OK_RESULT_OR_PANIC(PageCache::make_shared(std::move(arenas)));

    // Register all page layouts for this simulation.
    //
    for (const auto& [layout_id, reader] : this->page_layouts_) {
      this->log_event("registering page layout: ", layout_id);
      this->cache_->register_page_layout(layout_id, reader);
    }
  }
  return this->cache_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<Volume>> StorageSimulation::get_volume(
    const std::string& name, const VolumeReader::SlotVisitorFn& slot_visitor_fn,
    const Optional<u64> root_log_capacity, const Optional<VolumeOptions>& maybe_volume_options,
    std::shared_ptr<SlotLockManager> trim_control)
{
  const VolumeOptions volume_options = maybe_volume_options.value_or(VolumeOptions{
      .name = name,
      .uuid = None,
      .max_refs_per_page = MaxRefsPerPage{0},
      .trim_lock_update_interval = TrimLockUpdateInterval{512 /*bytes*/},
      .trim_delay_byte_count = TrimDelayByteCount{0},
  });

  //----- --- -- -  -  -   -
  const std::string root_log_device_name = batt::to_string(name, "_Volume_RootLogDevice");

  BasicLogDeviceFactory root_log_factory{[&] {
    return this->get_log_device(root_log_device_name, root_log_capacity);
  }};

  //----- --- -- -  -  -   -
  const std::string recycler_log_device_name = batt::to_string(name, "_Volume_RecyclerLogDevice");

  BasicLogDeviceFactory recycler_log_factory{[&] {
    return this->get_log_device(
        recycler_log_device_name,
        PageRecycler::calculate_log_size(
            PageRecyclerOptions{}.set_max_refs_per_page(volume_options.max_refs_per_page)));
  }};

  //----- --- -- -  -  -   -
  auto params = VolumeRecoverParams{
      .scheduler = &this->task_scheduler(),
      .options = volume_options,
      .cache = this->init_cache(),
      .root_log_factory = &root_log_factory,
      .recycler_log_factory = &recycler_log_factory,
      .trim_control = trim_control,
  };

  LLFS_LOG_SIM_EVENT() << "recovering simulated Volume " << batt::c_str_literal(name);

  return Volume::recover(std::move(params), slot_visitor_fn);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<bool> StorageSimulation::has_data_for_page_id(PageId page_id) const noexcept
{
  auto iter = this->page_device_id_map_.find(PageIdFactory::get_device_id(page_id));
  if (iter == this->page_device_id_map_.end()) {
    return {false};
  }

  return iter->second->has_data_for_page_id(page_id);
}

}  //namespace llfs
