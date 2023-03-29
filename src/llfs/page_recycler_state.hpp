//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_RECYCLER_STATE_HPP
#define LLFS_PAGE_RECYCLER_STATE_HPP

#include <llfs/config.hpp>
//

namespace llfs {

struct PageRecycler::NoLockState {
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

class PageRecycler::State : public NoLockState
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

}  // namespace llfs

#endif  // LLFS_PAGE_RECYCLER_STATE_HPP
