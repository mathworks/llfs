//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_LRU_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_LRU_HPP

#include <llfs/slot.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>

namespace llfs {

// Forward-declare.
//
class PageAllocatorLRUBase;

/** \brief Base hook that allows PageAllocatorLRUBase instances to be linked together to for a
 * least-recently-updated list.
 */
using PageAllocatorLRUHook =
    boost::intrusive::list_base_hook<boost::intrusive::tag<struct PageAllocatorLRUTag>>;

/** \brief A double-linked list of objects tracked by the PageAllocator, maintained in
 * least-recently-used order.
 */
using PageAllocatorLRUList =
    boost::intrusive::list<PageAllocatorLRUBase, boost::intrusive::base_hook<PageAllocatorLRUHook>>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
/** \brief Base class for all objects tracked by the PageAllocator; remembers when it was last
 * updated (and its place in an LRU list).
 */
class PageAllocatorLRUBase : public PageAllocatorLRUHook
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

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_LRU_HPP
