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

#include <llfs/page_allocator/page_allocator_free_pool.hpp>
#include <llfs/page_allocator/page_allocator_object_base.hpp>

#include <llfs/int_types.hpp>
#include <llfs/page_id_factory.hpp>

#include <atomic>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// PageAllocatorRefCount - the current ref count value for a single page, with last modified slot
// and hooks for the LRU list and free pool.
//
class PageAllocatorRefCount
    : public PageAllocatorObjectBase
    , public PageAllocatorFreePoolHook
{
 public:
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

  /** \brief Atomically performs a weak compare-and-swap operation on the reference count value.
   *
   * \see std::atomic<int>::compare_exchange_weak
   */
  bool compare_exchange_weak(i32& expected, i32 desired) noexcept
  {
    return this->count_.compare_exchange_weak(expected, desired);
  }

  /** \brief Atomically increments the reference count by `delta`, returning the previous value.
   */
  i32 fetch_add(i32 delta) noexcept
  {
    return this->count_.fetch_add(delta);
  }

  /** \brief Atomically decrements the reference count by `delta`, returning the previous value.
   */
  i32 fetch_sub(i32 delta) noexcept
  {
    return this->count_.fetch_sub(delta);
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
  std::atomic<u32> last_modified_by_user_index_{~u32{0}};
};

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_REF_COUNT_HPP
