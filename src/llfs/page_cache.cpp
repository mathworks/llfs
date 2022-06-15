//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache.hpp>
//

#include <llfs/memory_log_device.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/status_code.hpp>

#include <turtle/util/metric_registry.hpp>

#include <boost/range/irange.hpp>
#include <boost/uuid/random_generator.hpp>  // TODO [tastolfi 2021-04-05] remove me

namespace llfs {

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
                                                batt::Grant& recycle_grant) /*override*/
{
  if (to_delete.empty()) {
    return OkStatus();
  }
  const boost::uuids::uuid& caller_uuid = recycler.uuid();

  JobCommitParams params{
      .caller_uuid = &caller_uuid,
      .caller_slot = caller_slot,
      .recycler = as_ref(recycler),
      .recycle_grant = &recycle_grant,
      .recycle_depth = to_delete.front().depth + 1,
  };

  // We must drop the page from the storage device and decrement ref counts
  // as an atomic transaction, so create a PageCacheJob.
  //
  std::unique_ptr<PageCacheJob> job = this->page_cache_.new_job();

  // Add the page to the job's delete list; this will load the page to verify
  // we can trace its refs.
  //
  VLOG(1) << "[PageDeleterImpl::delete_pages] deleting: "
          << batt::dump_range(to_delete, batt::Pretty::True);

  for (const PageToRecycle& next_page : to_delete) {
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
    , storage_pool_{std::move(storage_pool)}
    , arenas_by_size_log2_{}
    , arenas_by_device_id_{}
    , impl_for_size_log2_{}
    , page_readers_{std::make_shared<
          batt::Mutex<std::unordered_map<PageLayoutId, PageReader, PageLayoutId::Hash>>>()}
{
  // Sort the storage pool by page size (MUST be first).
  //
  std::sort(this->storage_pool_.begin(), this->storage_pool_.end(), PageSizeOrder{});

  // Index the storage pool into groups of arenas by page size.
  //
  for (usize size_log2 = 6; size_log2 < kMaxPageSizeLog2; ++size_log2) {
    auto iter_pair = std::equal_range(this->storage_pool_.begin(), this->storage_pool_.end(),
                                      PageSize{u32{1} << size_log2}, PageSizeOrder{});

    this->arenas_by_size_log2_[size_log2] = as_slice(
        this->storage_pool_.data() + std::distance(this->storage_pool_.begin(), iter_pair.first),
        as_range(iter_pair).size());

    if (!this->arenas_by_size_log2_[size_log2].empty()) {
      this->impl_for_size_log2_[size_log2] =
          CacheImpl::make_new(/*n_slots=*/this->options_.max_cached_pages_per_size_log2[size_log2],
                              /*name=*/batt::to_string("size_", u64{1} << size_log2));
    }
  }

  // Index the storage pool by page device id.
  //
  for (PageArena& arena : this->storage_pool_) {
    const bool already_present =
        !this->arenas_by_device_id_.emplace(arena.device().get_id(), &arena).second;
    BATT_CHECK(!already_present)
        << "All device ids within a storage pool must be unique; found duplicate: "
        << arena.device().get_id();
  }

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
  return this->page_readers_->lock()->emplace(layout_id, reader).second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::close()
{
  for (PageArena& arena : this->storage_pool_) {
    arena.close();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::join()
{
  for (PageArena& arena : this->storage_pool_) {
    arena.join();
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
    PageSize size, batt::WaitForResource wait_for_resource, u64 callers, u64 job_id)
{
  const PageSizeLog2 size_log2 = log2_ceil(size);
  BATT_CHECK_EQ(usize{1} << size_log2, size) << "size must be a power of 2";

  return this->allocate_page_of_size_log2(
      size_log2, wait_for_resource, callers | Caller::PageCache_allocate_page_of_size, job_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> PageCache::allocate_page_of_size_log2(
    PageSizeLog2 size_log2, batt::WaitForResource wait_for_resource, u64 callers, u64 job_id)
{
  BATT_CHECK_LT(size_log2, kMaxPageSizeLog2);

  LatencyTimer alloc_timer{this->metrics_.allocate_page_alloc_latency};

  Slice<const PageArena> arenas = this->arenas_for_page_size_log2(size_log2);

  // TODO [tastolfi 2021-09-08] If the caller wants to wait, which device should we wait on?  First
  // available? Random?  Round-Robin?
  //
  for (auto wait_arg : {batt::WaitForResource::kFalse, batt::WaitForResource::kTrue}) {
    for (const PageArena& arena : arenas) {
      StatusOr<PageId> page_id = arena.allocator().allocate_page(wait_arg);
      if (!page_id.ok()) {
        continue;
      }

      BATT_CHECK_EQ(PageIdFactory::get_device_id(*page_id), arena.id());

      VLOG(1) << "allocated page " << *page_id;

      this->track_new_page_event(NewPageTracker{
          .ts = 0,
          .job_id = job_id,
          .page_id = *page_id,
          .callers = callers,
          .event_id = (int)NewPageTracker::Event::kAllocate,
      });

      // PageDevice::prepare must be thread-safe.
      //
      return arena.device().prepare(*page_id);
    }

    if (wait_for_resource == batt::WaitForResource::kFalse) {
      break;
    }
  }

  LOG(WARNING) << "No arena with free space could be found";
  return Status{batt::StatusCode::kUnavailable};  // TODO [tastolfi 2021-10-20]
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::deallocate_page(PageId page_id, u64 callers, u64 job_id)
{
  VLOG(1) << "deallocated page " << std::hex << page_id;

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
        LOG(ERROR) << "Failed to detach after failed attachement: " << BATT_INSPECT(p_arena->id());
      }
    }
  });

  for (const PageArena& arena : this->storage_pool_) {
    auto arena_status = arena.allocator().attach_user(user_id, slot_offset);
    BATT_REQUIRE_OK(arena_status);
    attached_arenas.emplace_back(&arena);
  }

  success = true;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::detach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset)
{
  for (const PageArena& arena : this->storage_pool_) {
    auto arena_status = arena.allocator().detach_user(user_id, slot_offset);
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
  (void)this->find_page_in_cache(page_id, /*require_tag=*/None);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<const PageArena> PageCache::all_arenas() const
{
  return as_slice(this->storage_pool_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<const PageArena> PageCache::arenas_for_page_size(usize size) const
{
  const usize size_log2 = batt::log2_ceil(size);
  BATT_CHECK_EQ(size, usize{1} << size_log2) << "page size must be a power of 2";

  return this->arenas_for_page_size_log2(size_log2);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<const PageArena> PageCache::arenas_for_page_size_log2(usize size_log2) const
{
  BATT_CHECK_LT(size_log2, kMaxPageSizeLog2);

  return this->arenas_by_size_log2_[size_log2];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageArena& PageCache::arena_for_page_id(PageId page_id) const
{
  return this->arena_for_device_id(PageIdFactory::get_device_id(page_id));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageArena& PageCache::arena_for_device_id(page_device_id_int device_id_val) const
{
  auto iter = this->arenas_by_device_id_.find(device_id_val);

  BATT_CHECK_NE(iter, this->arenas_by_device_id_.end())
      << "the specified page_id's device is not in the storage pool for this cache";

  return *iter->second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::put_view(std::shared_ptr<const PageView>&& view, u64 callers,
                                         u64 job_id)
{
  BATT_CHECK_NOT_NULLPTR(view);

  const page_id_int id_val = view->page_id().int_value();
  BATT_CHECK_NE(id_val, kInvalidPageId);

  const PageView* p_view = view.get();
  auto latch = std::make_shared<batt::Latch<std::shared_ptr<const PageView>>>();
  latch->set_value(std::move(view));

  // Attempt to insert the new page view into the cache.
  //
  const auto page_id = PageId{id_val};
  auto pinned_cache_slot = this->impl_for_page(page_id).find_or_insert(id_val, [&] {
    return std::move(latch);
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
  if (page_id.int_value() != kInvalidPageId) {
    this->track_new_page_event(NewPageTracker{
        .ts = 0,
        .job_id = job_id,
        .page_id = page_id,
        .callers = callers,
        .event_id = (int)NewPageTracker::Event::kPurge,
    });

    this->impl_for_page(page_id).erase(page_id.int_value());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::get(PageId page_id, const Optional<PageLayoutId>& require_layout,
                                    PinPageToJob pin_page_to_job)
{
  if (bool_from(pin_page_to_job, /*default_value=*/false)) {
    return Status{batt::StatusCode::kUnimplemented};
  }

  ++this->metrics_.get_count;

  if (!page_id) {
    return Status{StatusCode::kPageIdInvalid};
  }

  BATT_ASSIGN_OK_RESULT(CacheImpl::PinnedSlot cache_slot,  //
                        this->find_page_in_cache(page_id, require_layout));

  BATT_ASSIGN_OK_RESULT(StatusOr<std::shared_ptr<const PageView>> loaded,  //
                        cache_slot->await());

  BATT_CHECK_EQ(bool{loaded->get()}, bool{cache_slot});

  return PinnedPage{loaded->get(), std::move(cache_slot)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCache::impl_for_page(PageId page_id) -> CacheImpl&
{
  const u32 page_size = this->arena_for_page_id(page_id).device().page_size();
  const usize page_size_log2 = batt::log2_ceil(page_size);

  BATT_CHECK_LT(page_size_log2, this->impl_for_size_log2_.size());

  return *this->impl_for_size_log2_[page_size_log2];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCache::find_page_in_cache(PageId page_id, const Optional<PageLayoutId>& required_layout)
    -> batt::StatusOr<CacheImpl::PinnedSlot>
{
  if (!page_id) {
    return CacheImpl::PinnedSlot{};
  }

  std::shared_ptr<batt::Latch<std::shared_ptr<const PageView>>> latch = nullptr;

  batt::StatusOr<CacheImpl::PinnedSlot> pinned_slot =
      this->impl_for_page(page_id).find_or_insert(page_id.int_value(), [&latch] {
        latch = std::make_shared<batt::Latch<std::shared_ptr<const PageView>>>();
        return latch;
      });

  if (latch) {
    BATT_CHECK(pinned_slot.ok());

    BATT_DEBUG_INFO("PageCache::find_page_in_cache - starting async page read: "
                    << BATT_INSPECT(page_id) << BATT_INSPECT(batt::Task::current_stack_pos()));

    this->arena_for_page_id(page_id).device().read(
        page_id,
        /*read_handler=*/[
                             // Save the metrics and start time so we can record read latency etc.
                             //
                             p_metrics = &this->metrics_,
                             start_time = std::chrono::steady_clock::now(),

                             // We need to update the latch, so retain it in the capture.
                             //
                             captured_latch = latch,

                             // Keep a copy of pinned_slot while loading the page to limit the
                             // amount of churn under heavy read loads.
                             //
                             pinned_slot = batt::make_copy(*pinned_slot),

                             // Save a shared_ptr to the typed page view readers so we can parse the
                             // page data.
                             //
                             page_readers = this->page_readers_,  //
                             required_layout, this, page_id

    ](StatusOr<std::shared_ptr<const PageBuffer>>&& result) mutable {
          BATT_DEBUG_INFO("PageCache::find_page_in_cache - read handler");

          auto cleanup = batt::finally([&] {
            pinned_slot = {};
          });

          auto latch = std::move(captured_latch);
          if (!result.ok()) {
            LOG(WARNING) << "recent events for" << BATT_INSPECT(page_id)
                         << " (now=" << this->history_end_ << "):"
                         << batt::dump_range(
                                this->find_new_page_events(page_id) | seq::collect_vec(),
                                batt::Pretty::True);
            latch->set_value(result.status());
            return;
          }
          p_metrics->page_read_latency.update(start_time);

          // Page read succeeded!  Find the right typed reader.
          //
          std::shared_ptr<const PageBuffer>& page_data = *result;
          p_metrics->total_bytes_read.add(page_data->size());

          const PageLayoutId layout_id = [&] {
            if (required_layout) {
              return *required_layout;
            }
            return get_page_header(*page_data).layout_id;
          }();

          PageReader reader_for_layout;
          {
            auto locked = page_readers->lock();
            auto iter = locked->find(layout_id);
            if (iter == locked->end()) {
              latch->set_value(Status{
                  batt::StatusCode::kInternal});  // TODO [tastolfi 2021-10-20] "No reader
                                                  // registered for the specified page view type"
              return;
            }
            reader_for_layout = iter->second;
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

  return pinned_slot;
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
BoxedSeq<NewPageTracker> PageCache::find_new_page_events(PageId page_id) const
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
