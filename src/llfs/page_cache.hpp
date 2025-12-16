//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_CACHE_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/caller.hpp>
#include <llfs/log_device.hpp>
#include <llfs/metrics.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_allocate_options.hpp>
#include <llfs/page_allocator.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_cache_insert_options.hpp>
#include <llfs/page_cache_metrics.hpp>
#include <llfs/page_cache_options.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_device_entry.hpp>
#include <llfs/page_device_pairing.hpp>
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
#include <batteries/async/cancel_token.hpp>
#include <batteries/async/latch.hpp>
#include <batteries/async/mutex.hpp>
#include <batteries/utility.hpp>

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

#if LLFS_TRACK_NEW_PAGE_EVENTS

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

#endif  // LLFS_TRACK_NEW_PAGE_EVENTS

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PageCache : public PageLoader
{
 public:
  using PageDeviceEntry = ::llfs::PageDeviceEntry;

  struct PageReaderFromFile {
    PageReader page_reader;
    const char* file;
    int line;
  };

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

  using ExternalAllocation = PageCacheSlot::Pool::ExternalAllocation;

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a reference to a copy of the PageCacheOptions used to create this object.
   */
  const PageCacheOptions& options() const;

  Optional<PageId> page_shard_id_for(PageId full_page_id, const Interval<usize>& shard_range);

  /** \brief DEPRECATED - use register_page_reader.
   */
  bool register_page_layout(const PageLayoutId& layout_id, const PageReader& reader);

  batt::Status register_page_reader(const PageLayoutId& layout_id, const char* file, int line,
                                    const PageReader& reader);

  void close();

  void halt();

  void join();

  std::unique_ptr<PageCacheJob> new_job();

  StatusOr<PinnedPage> allocate_page(const PageAllocateOptions& options, u64 callers, u64 job_id);

  ExternalAllocation allocate_external(usize byte_size, PageCacheOvercommit& overcommit)
  {
    return this->cache_slot_pool_->allocate_external(byte_size, overcommit);
  }

  // Returns a page allocated via `allocate_page` to the free pool.  This MUST be done before the
  // page is written to the `PageDevice`.
  //
  void deallocate_page(PageId page_id, u64 callers, u64 job_id);

  Status attach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset);

  Status detach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset);

  Slice<PageDeviceEntry* const> devices_with_page_size_log2(usize size_log2) const;

  Slice<PageDeviceEntry* const> devices_with_page_size(usize size) const;

  Slice<PageDeviceEntry* const> all_devices() const;

  const std::vector<std::unique_ptr<PageDeviceEntry>>& devices_by_id() const;

  const PageArena& arena_for_page_id(PageId id_val) const;

  const PageArena& arena_for_device_id(page_device_id_int device_id_val) const;

  void async_write_new_page(PinnedPage&& pinned_page, PageDevice::WriteHandler&& handler) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Paired PageDevice interface.
  //

  /** \brief Attempts to pair two PageDevice instances in the pool.
   */
  Status assign_paired_device(PageSize src_page_size, const PageDevicePairing& pairing,
                              PageSize paired_page_size) noexcept;

  /** \brief Returns the PageId for the paired page for `page_id`, under the specified `pairing`
   * relationship.
   *
   * If no PageDevice has been assigned as the paired device for `page_id`'s device, returns None.
   */
  Optional<PageId> paired_page_id_for(PageId page_id, const PageDevicePairing& pairing) const;

  /** \brief Allocates a PageBuffer for the page paired to `page_id` under the specified pairing
   * relationship; pins the new page to the cache (with the specified priority) and returns the
   * resulting PinnedPage.
   */
  StatusOr<PinnedPage> allocate_paired_page_for(PageId page_id, const PageDevicePairing& pairing,
                                                const PageAllocateOptions& options);

  /** \brief Writes the specified paired page.  This happens outside the normal transactional page
   * creation workflow.
   */
  void async_write_paired_page(const PinnedPage& new_paired_page, const PageDevicePairing& pairing,
                               PageDevice::WriteHandler&& handler);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // PageLoader interface
  //

  PageCache* page_cache() const override
  {
    return const_cast<PageCache*>(this);
  }

  // Gives a hint to the cache to fetch the pages for the given ids in the background because we are
  // going to need them soon.
  //
  void prefetch_hint(PageId page_id) override;

  // Attempt to pin the page without loading it.
  //
  StatusOr<PinnedPage> try_pin_cached_page(PageId page_id, const PageLoadOptions& options) override;

  // Loads the specified page or retrieves from cache.
  //
  StatusOr<PinnedPage> load_page(PageId page_id, const PageLoadOptions& options) override;
  //
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  //----- --- -- -  -  -   -
  /** \brief Removes all cached data for the specified page.
   */
  void purge(PageId id_val, u64 callers, u64 job_id);

  bool page_might_contain_key(PageId id, const KeyView& key) const;

#if LLFS_TRACK_NEW_PAGE_EVENTS

  BoxedSeq<NewPageTracker> find_new_page_events(PageId page_id) const;

  void track_new_page_event(const NewPageTracker& tracker);

#endif  // LLFS_TRACK_NEW_PAGE_EVENTS

  PageCacheMetrics& metrics()
  {
    return this->metrics_;
  }

  PageCacheSlot::Pool& slot_pool()
  {
    return *this->cache_slot_pool_;
  }

  const boost::intrusive_ptr<PageCacheSlot::Pool>& shared_slot_pool()
  {
    return this->cache_slot_pool_;
  }

  const PageCacheSlot::Pool::Metrics& slot_pool_metrics() const
  {
    return this->cache_slot_pool_->metrics();
  }

  usize clear_all_slots()
  {
    return this->cache_slot_pool_->clear_all();
  }

  /** \brief Returns the PageDeviceEntry for the device that owns the given page.
   *
   * If the specified device (in the most-significant bits of `page_id`) isn't known by this
   * PageCache, returns nullptr.
   */
  PageDeviceEntry* get_device_for_page(PageId page_id);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  using PageLayoutReaderMap =
      std::unordered_map<PageLayoutId, PageReaderFromFile, PageLayoutId::Hash>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the maximum-valued id for the devices in `storage_pool`.
   */
  static page_device_id_int find_max_page_device_id(const std::vector<PageArena>& storage_pool);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageCache(std::vector<PageArena>&& storage_pool,
                     const PageCacheOptions& options) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Adds PageDeviceEntry objects for all arenas in `storage_pool`.
   *
   * `storage_pool` must be passed via move because its elements will be consumed/moved by this
   * function.
   */
  void initialize_page_device_entries(std::vector<PageArena>&& storage_pool) noexcept;

  /** \brief Creates and cross-links sharded view devices for each existing entry in
   * `this->page_devices_` joined with `options.sharded_views`.
   *
   * Should be called from the ctor, after `initialize_page_device_entries`.
   */
  void create_sharded_views(const PageCacheOptions& options) noexcept;

  /** \brief (Re-)Builds the index from page size (log2) to slice of PageDeviceEntry, for fast
   * lookup from allocation routines.
   */
  void index_device_entries_by_page_size() noexcept;

  //----- --- -- -  -  -   -
  StatusOr<PinnedPage> pin_allocated_page_to_cache(PageDeviceEntry* device_entry, PageId page_id,
                                                   const PageAllocateOptions& options);

  //----- --- -- -  -  -   -
  /** \brief Attempts to find the specified page (`page_id`) in the cache; if successful, the cache
   * slot is pinned (so it can't be evicted) and a pinned reference is returned.  Otherwise, we
   * attempt to load the page.
   *
   * If the given page is not in-cache and a cache slot can't be evicted/allocated (because there
   * are too many pinned pages), then this function returns llfs::StatusCode::kCacheSlotsFull.
   *
   * \param page_id The page to load
   *
   * \param required_layout If specified, then the layout of the page is checked and if it doesn't
   * match the given identifier, llfs::StatusCode::kPageHeaderBadLayoutId is returned.
   *
   * \param ok_if_not_found Controls whether page-not-found log messages (WARNING) are emitted if
   * the page isn't found; ok_if_not_found == false -> emit log warnings, ... == true -> don't
   */
  batt::StatusOr<PageCacheSlot::PinnedRef> find_page_in_cache(PageId page_id,
                                                              const PageLoadOptions& options);

  //----- --- -- -  -  -   -
  /** \brief Populates the passed PageCacheSlot asynchronously by attempting to read the page from
   * storage and setting the Latch value of the slot.
   *
   * \param required_layout If specified, then the layout of the page is checked and if it doesn't
   * match the given identifier, the Latch is set to llfs::StatusCode::kPageHeaderBadLayoutId.
   *
   * \param ok_if_not_found Controls whether page-not-found log messages (WARNING) are emitted if
   * the page isn't found; ok_if_not_found == false -> emit log warnings, ... == true -> don't
   */
  void async_load_page_into_slot(const PageCacheSlot::PinnedRef& pinned_slot,
                                 const Optional<PageLayoutId>& required_layout,
                                 OkIfNotFound ok_if_not_found);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // The configuration passed in at creation time.
  //
  const PageCacheOptions options_;

  // Metrics for this cache.
  //
  PageCacheMetrics metrics_;

  // The arenas backing up this cache, indexed by device id int.
  //
  std::vector<std::unique_ptr<PageDeviceEntry>> page_devices_;

  // The contents of `storage_pool_`, sorted by non-decreasing page size.
  //
  std::vector<PageDeviceEntry*> page_devices_by_page_size_;

  // Slices of `this->storage_pool_` that group arenas by page size (log2).  For example,
  // `this->arenas_by_size_log2_[12]` is the slice of `this->storage_pool_` comprised of
  // PageArenas whose page size is 4096.
  //
  std::array<Slice<PageDeviceEntry* const>, kMaxPageSizeLog2> page_devices_by_page_size_log2_;

  // The pool of cache slots.
  //
  boost::intrusive_ptr<PageCacheSlot::Pool> cache_slot_pool_;

  // A thread-safe shared map from PageLayoutId to PageReader function; layouts must be registered
  // with the PageCache so that we trace references during page recycling (aka garbage collection).
  //
  std::shared_ptr<batt::Mutex<PageLayoutReaderMap>> page_readers_;

#if LLFS_TRACK_NEW_PAGE_EVENTS
  std::array<NewPageTracker, 16384> history_;
  std::atomic<isize> history_end_{0};
#endif  // LLFS_TRACK_NEW_PAGE_EVENTS

};  // class PageCache

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline PageDeviceEntry* PageCache::get_device_for_page(PageId page_id)
{
  const page_device_id_int device_id = PageIdFactory::get_device_id(page_id);
  if (BATT_HINT_FALSE(device_id >= this->page_devices_.size())) {
    return nullptr;
  }

  return this->page_devices_[device_id].get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline Optional<PageId> PageCache::paired_page_id_for(
    PageId src_page_id, const PageDevicePairing& pairing) const
{
  const auto device_id = PageIdFactory::get_device_id(src_page_id);
  if (device_id >= this->page_devices_.size()) {
    return None;
  }

  PageDeviceEntry& src_entry = *this->page_devices_[device_id];
  if (!src_entry.paired_device_entry[pairing.value()]) {
    return None;
  }

  return PageIdFactory::change_device_id(src_page_id, src_entry.paired_device_id[pairing.value()]);
}

}  // namespace llfs
