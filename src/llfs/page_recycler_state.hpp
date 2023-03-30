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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Base class of PageRecycler::State that is safe to access concurrently without holding a
 * lock.
 */
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
/** \brief The state of the PageRecycler.
 */
class PageRecycler::State : public PageRecycler::NoLockState
{
 public:
  using ThreadSafeBase = NoLockState;

  explicit State(const boost::uuids::uuid& uuid, slot_offset_type latest_info_refresh_slot,
                 const PageRecyclerOptions& options, usize wal_capacity,
                 const SlotRange& initial_wal_range);

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  /** \brief Inserts all the pages in the passed slice, which must be in `refresh_slot`-sorted
   * order.  `pages` must not have any records with an assigned `batch_slot` (or we will panic).
   *
   * This function is used to restore the recycler state on recovery.
   */
  void bulk_load(Slice<const PageToRecycle> pages);

  /** \brief Inserts the given page into the queue and returns a list of pages to append to the
   * log.  If `p` has an invalid PageId, then returns an empty list.  Otherwise, the returned list
   * of records will always include `p`.
   */
  batt::StatusOr<slot_offset_type> insert_and_refresh(
      const PageToRecycle& p,
      std::function<batt::StatusOr<slot_offset_type>(const batt::SmallVecBase<PageToRecycle*>&)>&&
          append_to_log_fn);

  /** \brief Removes a single page from the highest discovery depth available.  If no pages are
   * queued, returns a record with an invalid PageId.
   */
  PageToRecycle remove();

  Optional<PageToRecycle> try_remove(i32 required_depth);

  /** \brief Returns the oldest refresh slot value for any page currently tracked by the recycler;
   * if there are none, returns None.
   */
  Optional<slot_offset_type> get_lru_slot() const;

  /** \brief Returns the maximum number of pages that can be queued in this object for deletion.
   */
  usize max_page_count() const
  {
    return this->arena_size_;
  }

  /** \brief Removes and returns up to `max_page_count` pages at the highest discovery depth
   * available.
   *
   * All the returned pages will be at the same depth.
   */
  std::vector<PageToRecycle> collect_batch(usize max_page_count, Metrics& metrics);

  //+++++++++++-+-+--+----- --- -- -  -  -   --

 private:
  /** \brief Allocates and initializes a WorkItem.  This is like calling new.
   */
  WorkItem& new_work_item(const PageToRecycle& p);

  /** \brief Allocates an uninitialized WorkItem.  This is like calling malloc.
   */
  WorkItem& alloc_work_item();

  /** \brief Initializes an already-allocated WorkItem.  This is like calling a constructor.
   */
  void init_work_item(WorkItem& item, const PageToRecycle& p);

  /** \brief Cleans up and frees a WorkItem.  This is like calling delete.
   */
  void delete_work_item(WorkItem& item);

  /** \brief Cleans up a WorkItem without freeing its memory.  This is like calling a destructor.
   */
  void deinit_work_item(WorkItem& item);

  /** \brief Frees the memory used by a WorkItem to the local pool, without cleaning it up.  This is
   * like calling free.
   */
  void free_work_item(WorkItem& item);

  /** \brief Returns the highest discovery depth value that currently has at least one page.
   */
  i32 get_active_depth() const;

  /** \brief Removes a single page record from the specified discovery depth.  If none are
   * available, returns None.  If `active_depth` is past the maximum depth (kMaxPageRefDepth),
   * behavior is undefined.
   */
  Optional<PageToRecycle> remove_at_depth(i32 active_depth);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The number of WorkItems in `arena_` that have *ever* been initialized/allocated.
   */
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
