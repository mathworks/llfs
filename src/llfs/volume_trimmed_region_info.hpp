//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_TRIMMED_REGION_INFO_HPP
#define LLFS_VOLUME_TRIMMED_REGION_INFO_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_id.hpp>
#include <llfs/slot.hpp>

#include <vector>

namespace llfs {

struct VolumeTrimmedRegionInfo {
  /** \brief The slot offset range of the trimmed region.
   */
  SlotRange slot_range;

  /** \brief The root page references in this trimmed region.
   */
  std::vector<PageId> obsolete_roots;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns whether a PackedVolumeTrimEvent needs to be written before trimming this
   * region.
   *
   * PackedVolumeTrimEvent is only necesary when we are going to be using a PageCacheJob to drop
   * root refs within the trimmed region.  In this case, we must make sure that all page ref count
   * updates are repeatable in case the trim is interrupted and needs to be completed during
   * recovery.
   */
  bool requires_trim_event_slot() const
  {
    return !this->obsolete_roots.empty();
  }
};

}  //namespace llfs

#endif  // LLFS_VOLUME_TRIMMED_REGION_INFO_HPP
