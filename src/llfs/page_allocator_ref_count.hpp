//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_REF_COUNT_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_REF_COUNT_HPP

#include <llfs/page_allocator_free_pool.hpp>
#include <llfs/page_allocator_lru.hpp>

#include <llfs/int_types.hpp>
#include <llfs/page_id_factory.hpp>

#include <atomic>

namespace llfs {

/** \brief Copyable summary of the current ref count state of a page tracked by a PageAllocator.
 */
struct PageAllocatorRefCountStatus {
  /** \brief The current page_id; includes the highest known generation.
   */
  PageId page_id;

  /** \brief The reference count for this page.
   */
  i32 ref_count;

  /** \brief The generation number embedded within `this->page_id`.
   */
  page_generation_int generation;

  /** \brief The user_index (attachment number) of the most recent client to update this page.
   */
  u32 user_index;

  /** \brief The learned upper bound slot offset of the PageAllocator at the point when this info
   * was read.
   */
  slot_offset_type learned_upper_bound;
};

inline std::ostream& operator<<(std::ostream& out, const PageAllocatorRefCountStatus& t)
{
  return out << "PageAllocatorRefCountStatus{.page_id=" << t.page_id
             << ", .ref_count=" << t.ref_count << ", .generation=" << t.generation
             << ", .user_index=" << t.user_index
             << ", .learned_upper_bound=" << t.learned_upper_bound << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// PageAllocatorRefCount - the current ref count value for a single page, with last modified slot
// and hooks for the LRU list and free pool.
//
class PageAllocatorRefCount
    : public PageAllocatorLRUBase
    , public PageAllocatorFreePoolHook
{
 public:
  static constexpr u32 kInvalidUserIndex = ~u32{0};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using Self = PageAllocatorRefCount;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new reference count object with count and generation both set to 0.
   */
  PageAllocatorRefCount() = default;

  /** \brief PageAllocatorRefCount is not copyable/movable.
   */
  PageAllocatorRefCount(const PageAllocatorRefCount&) = delete;

  /** \brief PageAllocatorRefCount is not copyable/movable.
   */
  PageAllocatorRefCount& operator=(const PageAllocatorRefCount&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the current reference count value.
   */
  i32 get_count() const noexcept
  {
    return this->count_.load();
  }

  /** \brief Returns the current generation number.
   */
  page_generation_int get_generation() const noexcept
  {
    return this->generation_.load();
  }

  /** \brief Atomically sets the reference count value, returning the previous value.
   */
  i32 set_count(i32 value) noexcept
  {
    return this->count_.exchange(value);
  }

  /** \brief Atomically sets the generation number, returning the previous value.
   */
  page_generation_int set_generation(page_generation_int generation) noexcept
  {
    return this->generation_.exchange(generation);
  }

  /** \brief Atomically updates the `last_modified_by` attachment num for this page.
   */
  void set_last_modified_by(u32 user_index) noexcept
  {
    this->last_modified_by_user_index_.store(user_index);
  }

  /** \brief Returns the current `last_modified_by` attachment num for this page.
   */
  u32 get_last_modified_by() const noexcept
  {
    return this->last_modified_by_user_index_.load();
  }

  /** \brief Atomically increments the generation number by 1, returning the new generation value.
   */
  page_generation_int advance_generation() noexcept
  {
    return this->generation_.fetch_add(1) + 1;
  }

  /** \brief Atomically decrements the generation number by 1, returning the new generation value.
   */
  page_generation_int revert_generation() noexcept
  {
    return this->generation_.fetch_sub(1) - 1;
  }

 private:
  std::atomic<page_generation_int> generation_{0};
  std::atomic<i32> count_{0};
  std::atomic<u32> last_modified_by_user_index_{Self::kInvalidUserIndex};
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline std::ostream& operator<<(std::ostream& out, const PageAllocatorRefCount& t)
{
  return out << "PageAllocatorRefCount{.count=" << t.get_count()
             << ", .generation=" << t.get_generation()
             << ", .last_modified_by=" << t.get_last_modified_by() << ",}";
}

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_REF_COUNT_HPP
