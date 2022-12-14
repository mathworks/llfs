//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_RECYCLER_HPP
#define LLFS_PAGE_RECYCLER_HPP

#include <llfs/config.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_cache_options.hpp>
#include <llfs/page_deleter.hpp>
#include <llfs/page_recycler_events.hpp>
#include <llfs/page_recycler_options.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/slot_writer.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/small_vec.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/uuid/uuid.hpp>

#include <array>
#include <string>
#include <unordered_set>

namespace llfs {

class PageCache;

// TODO [tastolfi 2021-04-07] provide a way to notify when the recycler is done so that we don't
// block waiting for pages to become available for allocation.

// Decrements refcounts for "dead" pages so their resources can be returned to the free pool for
// reuse.
//
class PageRecycler
{
 public:
  struct Metrics {
    CountMetric<u64> insert_count{0};
    CountMetric<u64> remove_count{0};
    CountMetric<u64> page_drop_ok_count{0};
    CountMetric<u64> page_drop_error_count{0};
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static PageCount default_max_buffered_page_count(MaxRefsPerPage max_refs_per_page);

  static u64 calculate_log_size(MaxRefsPerPage max_refs_per_page,
                                Optional<PageCount> max_buffered_page_count = None);

  static PageCount calculate_max_buffered_page_count(MaxRefsPerPage max_refs_per_page,
                                                     u64 log_size);

  static StatusOr<std::unique_ptr<PageRecycler>> recover(batt::TaskScheduler& scheduler,
                                                         std::string_view name,
                                                         MaxRefsPerPage max_refs_per_page,
                                                         PageDeleter& page_deleter,
                                                         LogDeviceFactory& log_device_factory);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageRecycler(const PageRecycler&) = delete;
  PageRecycler& operator=(const PageRecycler&) = delete;

  ~PageRecycler() noexcept;

  const boost::uuids::uuid& uuid() const
  {
    return this->state_.no_lock().uuid;
  }

  const Metrics& metrics() const noexcept
  {
    return this->metrics_;
  }

  void start();

  // Request shutdown.
  //
  void halt();

  // Wait for all background tasks to stop.
  //
  void join();

  // Schedule a page to be recycled; returns once the WAL has been appended, not necessarily flushed
  // (see `await_flush`).
  //
  StatusOr<slot_offset_type> recycle_pages(const Slice<const PageId>& page_ids,
                                           batt::Grant* grant = nullptr, i32 depth = 0);

  // Waits for the given slot to be flushed to durable storage.
  //
  Status await_flush(Optional<slot_offset_type> min_upper_bound);

  slot_offset_type slot_upper_bound(LogReadMode mode) const
  {
    return this->wal_device_->slot_range(mode).upper_bound;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Design Summary
  //
  // Goal: implement a depth-first traversal of dead pages to achieve a logarithmic bound on space
  // (WAL + memory).
  //
  // The state machine for the recycler centers around a stack of linked lists of dead page ids:
  //
  //  | DEPTH | PAGES
  //  +-------+-----------------------
  //  |   0   | {[id1] -> [id2] -> ... }
  //  |   1   | {[id3 -> [id4] -> ... }
  //  |   2   | {[id5]}
  //  |  ...
  //
  // Some levels of the stack may be empty.  The recycle task runs in the background, picking the
  // next page id at the highest depth level and atomically recycling it in a PageCache::Job
  // transaction.  This action may generate more dead page ids, which are accordingly inserted into
  // the stack at level (`depth` + 1), `depth` being the stack level of the just-recycled page.
  //
  // Changes to the state machine are serialized into a write-ahead log.  The size of the WAL is
  // bounded at C * sizeof(State), where C is the amortization constant (C >= 2).  Once the log has
  // reached capacity, for every (C-1) pages added to the state machine, one page record (the oldest
  // active one) must be "refreshed" by re-writing it to the end of the WAL.  This causes write load
  // to be amplified by a factor of C/(C-1) in the steady state.  We can use C to optimally
  // trade-off between write amplification and space amplification: lower C -> less space, more
  // writing; higher C -> more space, less writing.  This technique requires that we keep a LRU list
  // in addition to the per-stack level lists.
  //
  // Analysis:
  //
  // Because B-tree like structures bound the maximum number of indirections from root to leaf
  // (logarithmic on the total collection size; currently compile-time limited in
  // <llfs/config.hpp>, kMaxPageRefDepth), the stack is similarly bounded.  At the highest
  // stack depth, we know that recycling can't discover any new references.  Therefore the size of
  // the maximum stack level is bounded by the size of a Node page; this bound is given by
  // `kMaxPageRefsPerNode`.  Further, this bound applies to all stack levels except the lowest one,
  // where incoming pages (`PageRecycler::recycle_page`) are placed, since the only way a level can
  // grow is if a page at the next lower level is recycled.  Because this only happens when all
  // levels at higher depth are empty, we conclude that overall space is bounded by:
  //
  //   O(kMaxPageRefDepth * kMaxPageRefsPerNode)
  //
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  struct Batch {
    i32 depth;
    std::vector<PageToRecycle> to_recycle;
    slot_offset_type slot_offset;
  };

  using PageListHook = boost::intrusive::list_base_hook<boost::intrusive::tag<struct PageListTag>>;
  using LRUHook = boost::intrusive::list_base_hook<boost::intrusive::tag<struct LRUTag>>;

  struct WorkItem
      : PageListHook
      , LRUHook {
    WorkItem() = default;

    WorkItem(const WorkItem&) = delete;
    WorkItem& operator=(const WorkItem&) = delete;

    PageToRecycle to_recycle;
  };

  using PageList = boost::intrusive::list<WorkItem, boost::intrusive::base_hook<PageListHook>>;
  using LRUList = boost::intrusive::list<WorkItem, boost::intrusive::base_hook<LRUHook>>;

  struct NoLockState {
    explicit NoLockState(const boost::uuids::uuid& uuid_arg,
                         slot_offset_type latest_info_refresh_slot_arg,
                         const PageRecyclerOptions& options_arg) noexcept
        : uuid{uuid_arg}
        , latest_info_refresh_slot{latest_info_refresh_slot_arg}
        , options{options_arg}
    {
    }

    // The total number of pending WorkItem objects at all levels of the stack.
    //
    batt::Watch<usize> pending_count{0};

    // The unique identifier for this recycler (to prevent double-committing refcount changes).
    //
    const boost::uuids::uuid uuid;

    // The lower bound of the slot where the recycler's info was last refreshed.
    //
    batt::Watch<slot_offset_type> latest_info_refresh_slot;

    // Parameter values for this recycler.
    //
    const PageRecyclerOptions options;
  };

  class State : public NoLockState
  {
   public:
    using ThreadSafeBase = NoLockState;

    explicit State(const boost::uuids::uuid& uuid, slot_offset_type latest_info_refresh_slot,
                   const PageRecyclerOptions& options, usize wal_capacity,
                   const SlotRange& initial_wal_range);

    void bulk_load(Slice<const PageToRecycle> pages);

    batt::SmallVec<PageToRecycle, 2> insert(const PageToRecycle& p);

    PageToRecycle remove();

    Optional<PageToRecycle> try_remove(i32 required_depth);

    Optional<slot_offset_type> get_lru_slot() const;

    usize max_page_count() const
    {
      return this->arena_size_;
    }

    std::vector<PageToRecycle> collect_batch(usize max_page_count, Metrics& metrics);

   private:
    WorkItem& new_work_item(const PageToRecycle& p);

    WorkItem& alloc_work_item();

    WorkItem* refresh_oldest_work_item();

    void init_work_item(WorkItem& item, const PageToRecycle& p);

    void delete_work_item(WorkItem& item);

    void deinit_work_item(WorkItem& item);

    void free_work_item(WorkItem& item);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    usize arena_used_;
    const usize arena_size_;
    std::unique_ptr<WorkItem[]> arena_;
    std::unordered_set<PageId, PageId::Hash> pending_;
    std::array<PageList, kMaxPageRefDepth + 1> stack_;
    PageList free_pool_;
    LRUList lru_;
  };

  u64 total_log_bytes_flushed() const
  {
    u64 total = 0;
    total += this->wal_device_->slot_range(LogReadMode::kDurable).upper_bound;
    return total;
  }

 private:
  explicit PageRecycler(batt::TaskScheduler& scheduler, const std::string& name,
                        PageDeleter& page_deleter, std::unique_ptr<LogDevice>&& wal_device,
                        Optional<Batch>&& recovered_batch,
                        std::unique_ptr<PageRecycler::State>&& state) noexcept;

  void start_recycle_task();

  void recycle_task_main();

  // MUST be called only on the recycle task or the ctor.
  //
  void refresh_grants();

  StatusOr<slot_offset_type> insert_to_log(batt::Grant& grant, PageId page_id, i32 depth,
                                           batt::Mutex<std::unique_ptr<State>>::Lock& locked_state);

  StatusOr<Batch> prepare_batch(std::vector<PageToRecycle>&& to_recycle);

  Status commit_batch(const Batch& batch);

  Status trim_log();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::TaskScheduler& scheduler_;

  const std::string name_;

  PageDeleter& page_deleter_;

  std::atomic<bool> stop_requested_{false};

  std::unique_ptr<LogDevice> wal_device_;

  TypedSlotWriter<PageRecycleEvent> slot_writer_;

  batt::Grant recycle_task_grant_;

  batt::Grant insert_grant_pool_;

  batt::Mutex<std::unique_ptr<State>> state_;

  Status recycle_task_status_;

  Optional<batt::Task> recycle_task_;

  Metrics metrics_;

  Optional<Batch> prepared_batch_;

  Optional<slot_offset_type> latest_batch_upper_bound_;
};

inline std::ostream& operator<<(std::ostream& out, const PageRecycler::Batch& t)
{
  return out << "PageRecycler::Batch{.to_recycle=" << batt::dump_range(t.to_recycle)
             << ", .slot_offset=" << t.slot_offset << ",}";
}

}  // namespace llfs

#endif  // LLFS_PAGE_RECYCLER_HPP
