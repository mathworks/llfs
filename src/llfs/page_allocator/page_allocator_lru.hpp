#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_LRU_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_LRU_HPP

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>

namespace llfs {

// Forward-declare.
//
class PageAllocatorObjectBase;

/** \brief Base hook that allows PageAllocatorObjectBase instances to be linked together to for a
 * least-recently-updated list.
 */
using PageAllocatorLRUHook =
    boost::intrusive::list_base_hook<boost::intrusive::tag<struct PageAllocatorLRUTag>>;

/** \brief A double-linked list of objects tracked by the PageAllocator, maintained in
 * least-recently-used order.
 */
using PageAllocatorLRUList =
    boost::intrusive::list<PageAllocatorObjectBase,
                           boost::intrusive::base_hook<PageAllocatorLRUHook>>;

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_LRU_HPP
