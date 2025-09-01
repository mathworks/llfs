//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_DEVICE_ENTRY_HPP

#include <llfs/config.hpp>
//
#include <llfs/no_outgoing_refs_cache.hpp>
#include <llfs/page_arena.hpp>
#include <llfs/page_device_cache.hpp>

#include <batteries/math.hpp>

namespace llfs {

/** \brief All the per-PageDevice state for a single device in the storage pool.
 */
struct PageDeviceEntry {
  explicit PageDeviceEntry(PageArena&& arena,
                           boost::intrusive_ptr<PageCacheSlot::Pool>&& slot_pool) noexcept;

  /** \brief The PageDevice and PageAllocator.
   */
  PageArena arena;

  /** \brief Is this device available for `new_page` requests?
   */
  bool can_alloc;

  /** \brief A per-device page cache; shares a PageCacheSlot::Pool with all other PageDeviceEntry
   * objects that have the same page size.
   */
  PageDeviceCache cache;

  /** \brief A per-device tracker of the outgoing reference status of each physical page in the
   * device.
   */
  NoOutgoingRefsCache no_outgoing_refs_cache;

  /** \brief The log2 of the page size.
   */
  i32 page_size_log2;

  /** \brief Set to true iff this device is a sharded view of some other device.
   */
  bool is_sharded_view;

  /** \brief Precalculated device_id portion of PageIds owned by this device.
   */
  page_device_id_int device_id_shifted;

  /** \brief The sharded views associated with this device.
   */
  std::array<PageDeviceEntry*, kMaxPageSizeLog2> sharded_views;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the page size of this device.
   */
  PageSize page_size() const
  {
    return PageSize{u32{1} << this->page_size_log2};
  }
};

}  // namespace llfs
