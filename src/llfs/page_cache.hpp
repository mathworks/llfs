//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_HPP
#define LLFS_PAGE_CACHE_HPP

#include <llfs/api_types.hpp>
#include <llfs/cache.hpp>
#include <llfs/caller.hpp>
#include <llfs/log_device.hpp>
#include <llfs/metrics.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_allocator.hpp>
#include <llfs/page_arena.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_cache_metrics.hpp>
#include <llfs/page_cache_options.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_filter.hpp>
#include <llfs/page_id_slot.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_recycler.hpp>
#include <llfs/page_view.hpp>
#include <llfs/pinned_page.hpp>
#include <llfs/seq.hpp>
#include <llfs/slot_read_lock.hpp>
#include <llfs/status.hpp>

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/latch.hpp>
#include <batteries/async/mutex.hpp>

#include <functional>
#include <iomanip>
#include <memory>
#include <sstream>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>

namespace llfs {

class PageCacheJob;
struct JobCommitParams;

struct NewPageTracker {
  enum struct Event {
    kMinValue,
    kAllocate,
    kDeallocate,
    kPurge,
    kPutView_Ok,
    kPutView_Fail,
    kWrite_Ok,
    kWrite_Fail,
    kDrop,
    kMaxValue
  };

  isize ts;
  u64 job_id;
  PageId page_id;
  u64 callers;
  int event_id;
};

inline std::ostream& operator<<(std::ostream& out, const NewPageTracker& t)
{
  static const char* event_names[] = {"allocate",       "deallocate", "purge",       "put_view(ok)",
                                      "put_view(fail)", "write(ok)",  "write(fail)", "drop"};

  return out << "{.ts=" << t.ts << ", .job_id=" << std::dec << t.job_id
             << ", .page_id=" << t.page_id
             << ", .callers=" << batt::dump_range(Caller::get_strings(t.callers)) << ", event="
             << event_names[std::clamp(t.event_id, (int)NewPageTracker::Event::kMinValue + 1,
                                       (int)NewPageTracker::Event::kMaxValue - 1) -
                            1]
             << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PageCache : public PageLoader
{
 public:
  using CacheImpl = Cache<page_id_int, batt::Latch<std::shared_ptr<const PageView>>>;

  class PageDeleterImpl : public PageDeleter
  {
   public:
    explicit PageDeleterImpl(PageCache& page_cache) noexcept;

    Status delete_pages(const Slice<const PageToRecycle>& to_delete, PageRecycler& recycler,
                        slot_offset_type caller_slot, batt::Grant& recycle_grant,
                        i32 recycle_depth) override;

   private:
    PageCache& page_cache_;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static std::atomic<bool>& job_debug_on()
  {
    static std::atomic<bool> on = false;
    return on;
  }

  static StatusOr<batt::SharedPtr<PageCache>> make_shared(
      std::vector<PageArena>&& storage_pool,
      const PageCacheOptions& options = PageCacheOptions::with_default_values());

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCache(const PageCache&) = delete;
  PageCache& operator=(const PageCache&) = delete;

  ~PageCache() noexcept;

  const PageCacheOptions& options() const;

  bool register_page_layout(const PageLayoutId& layout_id, const PageReader& reader);

  void close();

  void join();

  std::unique_ptr<PageCacheJob> new_job();

  StatusOr<std::shared_ptr<PageBuffer>> allocate_page_of_size(
      PageSize size, batt::WaitForResource wait_for_resource, u64 callers, u64 job_id);

  StatusOr<std::shared_ptr<PageBuffer>> allocate_page_of_size_log2(
      PageSizeLog2 size_log2, batt::WaitForResource wait_for_resource, u64 callers, u64 job_id);

  // Returns a page allocated via `allocate_page` to the free pool.  This MUST be done before the
  // page is written to the `PageDevice`.
  //
  void deallocate_page(PageId page_id, u64 callers, u64 job_id);

  Status attach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset);

  Status detach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset);

  Slice<const PageArena> arenas_for_page_size_log2(usize size_log2) const;

  Slice<const PageArena> arenas_for_page_size(usize size) const;

  Slice<const PageArena> all_arenas() const;

  const PageArena& arena_for_page_id(PageId id_val) const;

  const PageArena& arena_for_device_id(page_device_id_int device_id_val) const;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // PageLoader interface
  //
  using PageLoader::get_page;
  using PageLoader::get_page_in_job;
  using PageLoader::get_page_slot;
  using PageLoader::get_page_slot_in_job;
  using PageLoader::get_page_slot_with_layout;
  using PageLoader::get_page_slot_with_layout_in_job;
  using PageLoader::get_page_with_layout;

  // Gives a hint to the cache to fetch the pages for the given ids in the background because we are
  // going to need them soon.
  //
  void prefetch_hint(PageId page_id) override;

  // Loads the specified page or retrieves from cache.
  //
  StatusOr<PinnedPage> get_page_with_layout_in_job(PageId page_id,
                                                   const Optional<PageLayoutId>& required_layout,
                                                   PinPageToJob pin_page_to_job,
                                                   OkIfNotFound ok_if_not_found) override;
  //
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Insert a newly built PageView into the cache.
  //
  StatusOr<PinnedPage> put_view(std::shared_ptr<const PageView>&& view, u64 callers, u64 job_id);

  // Remove all cached data for the specified page.
  //
  void purge(PageId id_val, u64 callers, u64 job_id);

  bool page_might_contain_key(PageId id, const KeyView& key) const;

  BoxedSeq<NewPageTracker> find_new_page_events(PageId page_id) const;

  void track_new_page_event(const NewPageTracker& tracker);

  PageCacheMetrics& metrics()
  {
    return this->metrics_;
  }

  const CacheImpl::Metrics& metrics_for_page_size(PageSize page_size) const
  {
    const i32 page_size_log2 = batt::log2_ceil(page_size);

    BATT_CHECK_LT(static_cast<usize>(page_size_log2), this->impl_for_size_log2_.size());
    BATT_CHECK_NOT_NULLPTR(this->impl_for_size_log2_[page_size_log2]);

    return this->impl_for_size_log2_[page_size_log2]->metrics();
  }

 private:
  //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
  using PageLayoutReaderMap = std::unordered_map<PageLayoutId, PageReader, PageLayoutId::Hash>;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageCache(std::vector<PageArena>&& storage_pool,
                     const PageCacheOptions& options) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  CacheImpl& impl_for_page(PageId page_id);

  batt::StatusOr<CacheImpl::PinnedSlot> find_page_in_cache(
      PageId page_id, const Optional<PageLayoutId>& required_layout, OkIfNotFound ok_if_not_found);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // The configuration passed in at creation time.
  //
  const PageCacheOptions options_;

  // Metrics for this cache.
  //
  PageCacheMetrics metrics_;

  // The arenas backing up this cache, sorted in ascending order of page size (each arena has a
  // homogenous page size).
  //
  std::vector<PageArena> storage_pool_;

  // Slices of `this->storage_pool_` that group arenas by page size (log2).  For example,
  // `this->arenas_by_size_log2_[12]` is the slice of `this->storage_pool_` comprised of
  // PageArenas whose page size is 4096.
  //
  std::array<Slice<PageArena>, kMaxPageSizeLog2> arenas_by_size_log2_;

  // Index of `this->storage_pool_` by device id, for fast lookup from PageId to the PageArena that
  // contains the page.
  //
  std::unordered_map<page_device_id_int, PageArena*> arenas_by_device_id_;

  // Maintain cache maps from page id to the page data for each page size.
  //
  std::array<boost::intrusive_ptr<CacheImpl>, kMaxPageSizeLog2> impl_for_size_log2_;

  // A thread-safe shared map from PageLayoutId to PageReader function; layouts must be registered
  // with the PageCache so that we trace references during page recycling (aka garbage collection).
  //
  std::shared_ptr<batt::Mutex<std::unordered_map<PageLayoutId, PageReader, PageLayoutId::Hash>>>
      page_readers_;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // TODO [tastolfi 2021-09-08] We need something akin to the PageRecycler/PageAllocator to durably
  // store page filters so we can cache those and do fast exclusion tests.  This may belong at a
  // higher level, however...
  //
  //  bool update_page_filter(std::shared_ptr<PageFilter>&& new_filter);
  //  void build_page_filter(PinnedPage&& page);
  //  Slice<Mutex<std::shared_ptr<PageFilter>>> leaf_page_filters();
  //  void page_filter_builder_task_main();
  //
  //  Queue<PinnedPage<>> page_filter_build_queue_;
  //  Task page_filter_builder_;
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  std::array<NewPageTracker, 16384> history_;
  std::atomic<isize> history_end_{0};

};  // class PageCache

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_HPP
