//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_OBJECT_BASE_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_OBJECT_BASE_HPP

#include <llfs/page_allocator/page_allocator_lru.hpp>

#include <llfs/slot.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
/** \brief Base class for all objects tracked by the PageAllocator; remembers when it was last
 * updated (and its place in an LRU list).
 */
class PageAllocatorObjectBase : public PageAllocatorLRUHook
{
 public:
  void set_last_update(slot_offset_type slot) noexcept
  {
    this->last_update_ = slot;
  }

  slot_offset_type last_update() const noexcept
  {
    return this->last_update_;
  }

 private:
  slot_offset_type last_update_ = 0;
};

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_OBJECT_BASE_HPP
