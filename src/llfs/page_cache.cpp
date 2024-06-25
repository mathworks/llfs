//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache.hpp>
//

#include <llfs/committable_page_cache_job.hpp>
#include <llfs/memory_log_device.hpp>
#include <llfs/metrics.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/status_code.hpp>

#include <boost/range/irange.hpp>
#include <boost/uuid/random_generator.hpp>  // TODO [tastolfi 2021-04-05] remove me

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize get_page_size(std::shared_ptr<const PageCache::PageDeviceEntry> entry)
{
  return entry ? get_page_size(entry->arena) : 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageCache::PageDeleterImpl::PageDeleterImpl(PageCache& page_cache) noexcept
    : page_cache_{page_cache}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::PageDeleterImpl::delete_pages(const Slice<const PageToRecycle>& to_delete,
                                                PageRecycler& recycler,
                                                slot_offset_type caller_slot,
                                                batt::Grant& recycle_grant,
                                                i32 recycle_depth) /*override*/
{
  if (to_delete.empty()) {
    return OkStatus();
  }
  BATT_DEBUG_INFO("delete_pages()" << BATT_INSPECT_RANGE(to_delete) << BATT_INSPECT(recycle_depth));

  const boost::uuids::uuid& caller_uuid = recycler.uuid();

  JobCommitParams params{
      .caller_uuid = &caller_uuid,
      .caller_slot = caller_slot,
      .recycler = as_ref(recycler),
      .recycle_grant = &recycle_grant,
      .recycle_depth = recycle_depth,
  };

  // We must drop the page from the storage device and decrement ref counts
  // as an atomic transaction, so create a PageCacheJob.
  //
  std::unique_ptr<PageCacheJob> job = this->page_cache_.new_job();

  // Add the page to the job's delete list; this will load the page to verify
  // we can trace its refs.
  //
  LLFS_VLOG(1) << "[PageDeleterImpl::delete_pages] deleting: "
               << batt::dump_range(to_delete, batt::Pretty::True);

  for (const PageToRecycle& next_page : to_delete) {
    BATT_CHECK_EQ(next_page.depth, params.recycle_depth);
    Status pre_delete_status = job->delete_page(next_page.page_id);
    BATT_REQUIRE_OK(pre_delete_status);
  }

  // Commit the job.
  //
  Status job_status = commit(std::move(job), params, Caller::PageRecycler_recycle_task_main);
  BATT_REQUIRE_OK(job_status);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::shared_ptr<PageCache>> PageCache::make_shared(
    std::vector<PageArena>&& storage_pool, const PageCacheOptions& options)
{
  initialize_status_codes();

  std::shared_ptr<PageCache> page_cache{new PageCache(std::move(storage_pool), options)};

  return page_cache;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache::PageCache(std::vector<PageArena>&& storage_pool,
                     const PageCacheOptions& options) noexcept
    : options_{options}
    , metrics_{}
    , page_readers_{std::make_shared<batt::Mutex<PageLayoutReaderMap>>()}
{
  {
    batt::ScopedWriteLock<State> state(this->state_);
    state->cache_slot_pool_by_page_size_log2.fill(nullptr);
  }

  BATT_CHECK_OK(this->add_page_devices(storage_pool));

  // Register metrics.
  //
  const auto metric_name = [this](std::string_view property) {
    return batt::to_string("PageCache_", property);
  };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), this->metrics_.n)

  ADD_METRIC_(total_bytes_written);
  ADD_METRIC_(used_bytes_written);
  ADD_METRIC_(node_write_count);
  ADD_METRIC_(leaf_write_count);
  ADD_METRIC_(page_read_latency);
  ADD_METRIC_(page_write_latency);
  ADD_METRIC_(pipeline_wait_latency);
  ADD_METRIC_(update_ref_counts_latency);
  ADD_METRIC_(ref_count_sync_latency);

#undef ADD_METRIC_
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache::~PageCache() noexcept
{
  this->close();
  this->join();

  global_metric_registry()  //
      .remove(this->metrics_.total_bytes_written)
      .remove(this->metrics_.used_bytes_written)
      .remove(this->metrics_.node_write_count)
      .remove(this->metrics_.leaf_write_count)
      .remove(this->metrics_.page_read_latency)
      .remove(this->metrics_.page_write_latency)
      .remove(this->metrics_.pipeline_wait_latency)
      .remove(this->metrics_.update_ref_counts_latency)
      .remove(this->metrics_.ref_count_sync_latency);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageCacheOptions& PageCache::options() const
{
  return this->options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCache::register_page_layout(const PageLayoutId& layout_id, const PageReader& reader)
{
  LLFS_LOG_WARNING() << "PageCache::register_page_layout is DEPRECATED; please use "
                        "PageCache::register_page_reader";

  return this->page_readers_->lock()
      ->emplace(layout_id,
                PageReaderFromFile{
                    .page_reader = reader,
                    .file = __FILE__,
                    .line = __LINE__,
                })
      .second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status PageCache::register_page_reader(const PageLayoutId& layout_id, const char* file,
                                             int line, const PageReader& reader)
{
  auto locked = this->page_readers_->lock();
  const auto [iter, was_inserted] = locked->emplace(layout_id, PageReaderFromFile{
                                                                   .page_reader = reader,
                                                                   .file = file,
                                                                   .line = line,
                                                               });

  if (!was_inserted && (iter->second.file != file || iter->second.line != line)) {
    return ::llfs::make_status(StatusCode::kPageReaderConflict);
  }

  BATT_CHECK_EQ(iter->second.file, file);
  BATT_CHECK_EQ(iter->second.line, line);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::close()
{
  batt::ScopedReadLock<State> state(this->state_);
  for (const std::shared_ptr<PageDeviceEntry>& entry : state->page_devices) {
    if (entry) {
      entry->arena.halt();
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::join()
{
  batt::ScopedReadLock<State> state(this->state_);
  for (const std::shared_ptr<PageDeviceEntry>& entry : state->page_devices) {
    if (entry) {
      entry->arena.join();
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<PageCacheJob> PageCache::new_job()
{
  return std::make_unique<PageCacheJob>(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> PageCache::allocate_page_of_size(
    PageSize size, batt::WaitForResource wait_for_resource, u64 callers, u64 job_id,
    const batt::CancelToken& cancel_token)
{
  const PageSizeLog2 size_log2 = log2_ceil(size);
  BATT_CHECK_EQ(usize{1} << size_log2, size) << "size must be a power of 2";

  return this->allocate_page_of_size_log2(size_log2, wait_for_resource,
                                          callers | Caller::PageCache_allocate_page_of_size, job_id,
                                          std::move(cancel_token));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> PageCache::allocate_page_of_size_log2(
    PageSizeLog2 size_log2, batt::WaitForResource wait_for_resource, u64 callers, u64 job_id,
    const batt::CancelToken& cancel_token)
{
  BATT_CHECK_LT(size_log2, kMaxPageSizeLog2);

  LatencyTimer alloc_timer{this->metrics_.allocate_page_alloc_latency};

  static const char* kOperationName = "PageCache::allocate_page_of_size_log2";

  // TODO: [Gabe Bornstein 6/19/24] Tony mentioned there's a different method besides
  // `with_retry_policy` that we could use. It could potentially cause less congestion in the
  // pipeline of CheckpointGenerator -> Trim -> PageRecycler -> Etc. However, it's more complicated
  //
  return                        //
      batt::with_retry_policy(  //
                                // TODO: [Gabe Bornstein 6/20/24] Ensure MAX waittime is < 100 ms
                                //
          batt::ExponentialBackoff{
              .max_attempts = ~u64{0},
              .initial_delay_usec = 500,
              .backoff_factor = 3,
              .backoff_divisor = 2,
              .max_delay_usec = 1000 * 100,
          },
          kOperationName,  //
          [this, &size_log2, &cancel_token, &job_id, &callers, &wait_for_resource] {
            this->update_available_pages();

            batt::StatusOr<std::shared_ptr<PageBuffer>> page_buffer;
            batt::Status s = batt::StatusCode::kResourceExhausted;
            page_buffer = s;

            Slice<std::shared_ptr<const PageDeviceEntry>> device_entries =
                this->devices_with_page_size_log2(size_log2);
            // TODO: [Gabe Bornstein 6/20/24] Currently, we're dropping the state lock in between
            // `update_available_pages` and `allocate_page`. Could this cause issues? Could we run
            // out of pages?
            //
            batt::ScopedWriteLock<State> state(this->state_);
            // If available_pages does not have not have any pages that match device_entries,
            // nothing to do. No available pages. Return and try again.
            //
            for (std::shared_ptr<const PageDeviceEntry> device_entry : device_entries) {
              const PageArena& arena = device_entry->arena;
              if (state->arenas_with_available_pages.find(arena.id()) ==
                  state->arenas_with_available_pages.end()) {
                continue;
              }
              StatusOr<PageId> page_id =
                  arena.allocator().allocate_page(batt::WaitForResource::kFalse, cancel_token);
              if (!page_id.ok()) {
                if (page_id.status() == batt::StatusCode::kResourceExhausted) {
                  const u64 page_size = u64{1} << size_log2;
                  LLFS_LOG_INFO_FIRST_N(1)  //
                      << "Failed to allocate page (pool is empty): " << BATT_INSPECT(page_size);
                }
                continue;
              }
              BATT_CHECK_EQ(PageIdFactory::get_device_id(*page_id), arena.id());

              LLFS_VLOG(1) << "allocated page " << *page_id;

              this->track_new_page_event(NewPageTracker{
                  .ts = 0,
                  .job_id = job_id,
                  .page_id = *page_id,
                  .callers = callers,
                  .event_id = (int)NewPageTracker::Event::kAllocate,
              });
              // PageDevice::prepare must be thread-safe.
              //
              page_buffer = arena.device().prepare(*page_id);
              break;
            }

            return page_buffer;
          },                                   //
          batt::TaskSleepImpl{},               //
          [](const batt::Status& s) -> bool {  //
            VLOG(2) << "batt::StatusCode::kResourceExhausted == "
                    << (s == batt::StatusCode::kResourceExhausted);
            return batt::status_is_retryable(s) || (s == batt::StatusCode::kResourceExhausted);
          });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::deallocate_page(PageId page_id, u64 callers, u64 job_id)
{
  LLFS_VLOG(1) << "deallocated page " << std::hex << page_id;

  this->track_new_page_event(NewPageTracker{
      .ts = 0,
      .job_id = job_id,
      .page_id = page_id,
      .callers = callers,
      .event_id = (int)NewPageTracker::Event::kDeallocate,
  });

  this->arena_for_page_id(page_id).allocator().deallocate_page(page_id);
  this->purge(page_id, callers | Caller::PageCache_deallocate_page, job_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::attach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset)
{
  bool success = false;
  std::vector<const PageArena*> attached_arenas;

  auto detach_on_failure = batt::finally([&] {
    if (success) {
      return;
    }
    for (const PageArena* p_arena : attached_arenas) {
      auto detach_status = p_arena->allocator().detach_user(user_id, slot_offset);
      if (!detach_status.ok()) {
        LLFS_LOG_ERROR() << "Failed to detach after failed attachement: "
                         << BATT_INSPECT(p_arena->id());
      }
    }
  });

  for (std::shared_ptr<const PageDeviceEntry> entry : this->all_devices()) {
    BATT_CHECK_NOT_NULLPTR(entry);
    auto arena_status = entry->arena.allocator().attach_user(user_id, slot_offset);
    BATT_REQUIRE_OK(arena_status);
    attached_arenas.emplace_back(&entry->arena);
  }

  success = true;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::detach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset)
{
  for (std::shared_ptr<const PageDeviceEntry> entry : this->all_devices()) {
    BATT_CHECK_NOT_NULLPTR(entry);
    auto arena_status = entry->arena.allocator().detach_user(user_id, slot_offset);
    BATT_REQUIRE_OK(arena_status);
  }
  //
  // TODO [tastolfi 2021-04-02] - handle partial failure (?)

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::prefetch_hint(PageId page_id)
{
  (void)this->find_page_in_cache(page_id, /*require_tag=*/None, OkIfNotFound{false});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<std::shared_ptr<const PageCache::PageDeviceEntry>> PageCache::all_devices()
{
  batt::ScopedReadLock<State> state(this->state_);
  std::vector<std::shared_ptr<const PageCache::PageDeviceEntry>> devices =
      state->page_devices_by_page_size;
  return devices;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<std::shared_ptr<const PageCache::PageDeviceEntry>> PageCache::devices_with_page_size(
    usize size)
{
  const usize size_log2 = batt::log2_ceil(size);
  BATT_CHECK_EQ(size, usize{1} << size_log2) << "page size must be a power of 2";

  return this->devices_with_page_size_log2(size_log2);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<std::shared_ptr<const PageCache::PageDeviceEntry>> PageCache::devices_with_page_size_log2(
    usize size_log2)
{
  BATT_CHECK_LT(size_log2, kMaxPageSizeLog2);
  batt::ScopedReadLock<State> state(this->state_);
  return state->page_devices_by_page_size_log2[size_log2];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageArena& PageCache::arena_for_page_id(PageId page_id)
{
  return this->arena_for_device_id(PageIdFactory::get_device_id(page_id));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageArena& PageCache::arena_for_device_id(page_device_id_int device_id_val)
{
  batt::ScopedReadLock<State> state(this->state_);
  BATT_CHECK_LT(device_id_val, state->page_devices.size())
      << "the specified page_id's device is not in the storage pool for this cache";

  BATT_CHECK_NOT_NULLPTR(state->page_devices[device_id_val]);

  return state->page_devices[device_id_val]->arena;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::put_view(std::shared_ptr<const PageView>&& view, u64 callers,
                                         u64 job_id)
{
  BATT_CHECK_NOT_NULLPTR(view);

  if (view->get_page_layout_id() != view->header().layout_id) {
    return {::llfs::make_status(StatusCode::kPageHeaderBadLayoutId)};
  }

  if (this->page_readers_->lock()->count(view->get_page_layout_id()) == 0) {
    return {::llfs::make_status(StatusCode::kPutViewUnknownLayoutId)};
  }

  const PageView* p_view = view.get();
  const PageId page_id = view->page_id();

  PageDeviceEntry* const entry = this->get_device_for_page(page_id);
  BATT_CHECK_NOT_NULLPTR(entry);

  // Attempt to insert the new page view into the cache.
  //
  batt::StatusOr<PageCacheSlot::PinnedRef> pinned_cache_slot =
      entry->cache.find_or_insert(page_id, [&view](const PageCacheSlot::PinnedRef& pinned_ref) {
        pinned_ref->set_value(std::move(view));
      });

  this->track_new_page_event(NewPageTracker{
      .ts = 0,
      .job_id = job_id,
      .page_id = page_id,
      .callers = callers,
      .event_id = pinned_cache_slot.ok() ? (int)NewPageTracker::Event::kPutView_Ok
                                         : (int)NewPageTracker::Event::kPutView_Fail,
  });

  // If no slots are available, clients should back off and retry.
  //
  BATT_REQUIRE_OK(pinned_cache_slot);

  PinnedPage pinned_page{p_view, std::move(*pinned_cache_slot)};
  BATT_CHECK(bool{pinned_page});

  return pinned_page;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::purge(PageId page_id, u64 callers, u64 job_id)
{
  if (page_id.is_valid()) {
    this->track_new_page_event(NewPageTracker{
        .ts = 0,
        .job_id = job_id,
        .page_id = page_id,
        .callers = callers,
        .event_id = (int)NewPageTracker::Event::kPurge,
    });

    PageDeviceEntry* const entry = this->get_device_for_page(page_id);
    BATT_CHECK_NOT_NULLPTR(entry);

    entry->cache.erase(page_id);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status PageCache::add_page_devices(std::vector<PageArena>& arenas)
{
  {
    batt::ScopedWriteLock<State> state(this->state_);
    // TODO: [Gabe Bornstein 6/6/24] Read over this function and make sure it won't break anything
    // with just appending some values vs. creating page_cache from scratch.

    // Find the maximum page device id value.
    //
    // TODO: [Gabe Bornstein 6/6/24] Consider, this only looks for the max_page_device_id in
    // `arenas`. Do we need to include pre-existing ids in `page_devices` as well?
    //
    page_device_id_int max_page_device_id = 0;
    for (const PageArena& arena : arenas) {
      max_page_device_id = std::max(max_page_device_id, arena.device().get_id());
    }

    // Populate state.page_devices.
    //
    state->page_devices.resize(max_page_device_id + 1);
    for (PageArena& arena : arenas) {
      const page_device_id_int device_id = arena.device().get_id();
      const auto page_size_log2 = batt::log2_ceil(arena.device().page_size());

      BATT_CHECK_EQ(PageSize{1} << page_size_log2, arena.device().page_size())
          << "Page sizes must be powers of 2!";

      BATT_CHECK_LT(page_size_log2, kMaxPageSizeLog2);

      // Create a slot pool for this page size if we haven't already done so.
      //
      if (!state->cache_slot_pool_by_page_size_log2[page_size_log2]) {
        state->cache_slot_pool_by_page_size_log2[page_size_log2] = PageCacheSlot::Pool::make_new(
            /*n_slots=*/this->options_.max_cached_pages_per_size_log2[page_size_log2],
            /*name=*/batt::to_string("size_", u64{1} << page_size_log2));
      }

      BATT_CHECK_EQ(state->page_devices[device_id], nullptr)
          << "Duplicate entries found for the same device id!" << BATT_INSPECT(device_id);

      state->page_devices[device_id] = std::make_shared<PageDeviceEntry>(            //
          std::move(arena),                                                          //
          batt::make_copy(state->cache_slot_pool_by_page_size_log2[page_size_log2])  //
      );

      // We will sort these later.
      //
      state->page_devices_by_page_size.emplace_back(state->page_devices[device_id]);
    }
    // BATT_CHECK_EQ(state->page_devices_by_page_size.size(), storage_pool.size());

    // Sort the storage pool by page size (MUST be first).
    //
    std::sort(state->page_devices_by_page_size.begin(), state->page_devices_by_page_size.end(),
              PageSizeOrder{});

    // Index the storage pool into groups of arenas by page size.
    //
    for (usize size_log2 = 6; size_log2 < kMaxPageSizeLog2; ++size_log2) {
      auto iter_pair = std::equal_range(state->page_devices_by_page_size.begin(),
                                        state->page_devices_by_page_size.end(),
                                        PageSize{u32{1} << size_log2}, PageSizeOrder{});

      state->page_devices_by_page_size_log2[size_log2] =
          as_slice(state->page_devices_by_page_size.data() +
                       std::distance(state->page_devices_by_page_size.begin(), iter_pair.first),
                   as_range(iter_pair).size());
    }
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int PageCache::get_num_page_devices()
{
  batt::ScopedReadLock<State> state(this->state_);
  return state->page_devices.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache::PageDeviceEntry* PageCache::get_device_for_page(PageId page_id)
{
  batt::ScopedReadLock<State> state(this->state_);
  const page_device_id_int device_id = PageIdFactory::get_device_id(page_id);
  if (BATT_HINT_FALSE(device_id >= state->page_devices.size())) {
    return nullptr;
  }

  return state->page_devices[device_id].get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::get_page_with_layout_in_job(
    PageId page_id, const Optional<PageLayoutId>& require_layout, PinPageToJob pin_page_to_job,
    OkIfNotFound ok_if_not_found)
{
  if (bool_from(pin_page_to_job, /*default_value=*/false)) {
    return Status{batt::StatusCode::kUnimplemented};
  }

  ++this->metrics_.get_count;

  if (!page_id) {
    return ::llfs::make_status(StatusCode::kPageIdInvalid);
  }

  BATT_ASSIGN_OK_RESULT(PageCacheSlot::PinnedRef pinned_slot,  //
                        this->find_page_in_cache(page_id, require_layout, ok_if_not_found));

  BATT_ASSIGN_OK_RESULT(StatusOr<std::shared_ptr<const PageView>> loaded,  //
                        pinned_slot->await());

  BATT_CHECK_EQ(loaded->get() != nullptr, bool{pinned_slot});

  return PinnedPage{loaded->get(), std::move(pinned_slot)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCache::find_page_in_cache(PageId page_id, const Optional<PageLayoutId>& required_layout,
                                   OkIfNotFound ok_if_not_found)
    -> batt::StatusOr<PageCacheSlot::PinnedRef>
{
  if (!page_id) {
    return PageCacheSlot::PinnedRef{};
  }

  PageDeviceEntry* const entry = this->get_device_for_page(page_id);
  BATT_CHECK_NOT_NULLPTR(entry);

  return entry->cache.find_or_insert(page_id, [&](const PageCacheSlot::PinnedRef& pinned_slot) {
    this->async_load_page_into_slot(pinned_slot, required_layout, ok_if_not_found);
  });
}

void PageCache::update_available_pages()
{
  batt::ScopedWriteLock<State> state(this->state_);
  for (std::shared_ptr<PageDeviceEntry> page_device : state->page_devices) {
    if (page_device->arena.allocator().free_pool_size() != 0) {
      state->arenas_with_available_pages.insert(page_device->arena.id());
    } else {
      state->arenas_with_available_pages.erase(page_device->arena.id());
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::async_load_page_into_slot(const PageCacheSlot::PinnedRef& pinned_slot,
                                          const Optional<PageLayoutId>& required_layout,
                                          OkIfNotFound ok_if_not_found)
{
  const PageId page_id = pinned_slot.key();

  PageDeviceEntry* const entry = this->get_device_for_page(page_id);
  BATT_CHECK_NOT_NULLPTR(entry);

  entry->arena.device().read(
      page_id,
      /*read_handler=*/[this, required_layout, ok_if_not_found,

                        // Save the metrics and start time so we can record read latency etc.
                        //
                        start_time = std::chrono::steady_clock::now(),

                        // Keep a copy of pinned_slot while loading the page to limit the
                        // amount of churn under heavy read loads.
                        //
                        pinned_slot = batt::make_copy(pinned_slot)

  ](StatusOr<std::shared_ptr<const PageBuffer>>&& result) mutable {
        const PageId page_id = pinned_slot.key();
        auto* p_metrics = &this->metrics_;
        auto page_readers = this->page_readers_;

        BATT_DEBUG_INFO("PageCache::find_page_in_cache - read handler");

        auto cleanup = batt::finally([&] {
          pinned_slot = {};
        });

        batt::Latch<std::shared_ptr<const PageView>>* latch = pinned_slot.value();
        BATT_CHECK_NOT_NULLPTR(latch);

        if (!result.ok()) {
          if (!ok_if_not_found) {
            LLFS_LOG_WARNING() << "recent events for" << BATT_INSPECT(page_id)
                               << BATT_INSPECT(ok_if_not_found) << " (now=" << this->history_end_
                               << "):"
                               << batt::dump_range(
                                      this->find_new_page_events(page_id) | seq::collect_vec(),
                                      batt::Pretty::True);
          }
          latch->set_value(result.status());
          return;
        }
        p_metrics->page_read_latency.update(start_time);

        // Page read succeeded!  Find the right typed reader.
        //
        std::shared_ptr<const PageBuffer>& page_data = *result;
        p_metrics->total_bytes_read.add(page_data->size());

        PageLayoutId layout_id = get_page_header(*page_data).layout_id;
        if (required_layout) {
          if (*required_layout != layout_id) {
            latch->set_value(::llfs::make_status(StatusCode::kPageHeaderBadLayoutId));
            return;
          }
        }

        PageReader reader_for_layout;
        {
          auto locked = page_readers->lock();
          auto iter = locked->find(layout_id);
          if (iter == locked->end()) {
            LLFS_LOG_ERROR() << "Unknown page layout: "
                             << batt::c_str_literal(
                                    std::string_view{(const char*)&layout_id, sizeof(layout_id)})
                             << BATT_INSPECT(page_id);
            latch->set_value(make_status(StatusCode::kNoReaderForPageViewType));
            return;
          }
          reader_for_layout = iter->second.page_reader;
        }
        // ^^ Release the page_readers mutex ASAP

        StatusOr<std::shared_ptr<const PageView>> page_view =
            reader_for_layout(std::move(page_data));
        if (page_view.ok()) {
          BATT_CHECK_EQ(page_view->use_count(), 1u);
        }
        latch->set_value(std::move(page_view));
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCache::page_might_contain_key(PageId /*page_id*/, const KeyView& /*key*/) const
{
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::track_new_page_event(const NewPageTracker& tracker)
{
  const isize i = this->history_end_.fetch_add(1);
  BATT_CHECK_GE(i, 0);
  auto& slot = this->history_[i % this->history_.size()];
  slot = tracker;
  slot.ts = i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<NewPageTracker> PageCache::find_new_page_events(PageId page_id)
{
  const isize n = this->history_end_.load();
  return batt::as_seq(boost::irange(isize{0}, isize(this->history_.size())))  //
         | seq::map([this, n](isize i) {
             isize j = n - i;
             if (j < 0) {
               j += this->history_.size();
               BATT_CHECK_GE(j, 0);
             }
             return this->history_[j % this->history_.size()];
           })  //
         | seq::filter(
               [n, page_id, page_id_factory = this->arena_for_page_id(page_id).device().page_ids()](
                   const NewPageTracker& t) {
                 return t.ts < n &&
                        page_id_factory.get_device_id(t.page_id) ==
                            page_id_factory.get_device_id(page_id) &&
                        page_id_factory.get_physical_page(t.page_id) ==
                            page_id_factory.get_physical_page(page_id);
               })  //
         | seq::boxed();
}

}  // namespace llfs
