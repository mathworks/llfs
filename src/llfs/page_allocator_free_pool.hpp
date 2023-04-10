//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_FREE_POOL_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_FREE_POOL_HPP

namespace llfs {

// Forward-declare.
//
class PageAllocatorRefCount;

/** \brief Base list node hook that allows a PageAllocatorRefCount to be added to the free pool.
 */
using PageAllocatorFreePoolHook =
    boost::intrusive::list_base_hook<boost::intrusive::tag<struct PageAllocatorFreePoolTag>>;

/** \brief A double-linked list of ref count objects that form a pool of available (unused) physical
 * pages.
 */
using PageAllocatorFreePoolList =
    boost::intrusive::list<PageAllocatorRefCount,
                           boost::intrusive::base_hook<PageAllocatorFreePoolHook>>;

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_FREE_POOL_HPP
