//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_RECYCLER_OPTIONS_HPP
#define LLFS_PAGE_RECYCLER_OPTIONS_HPP

#include <llfs/config.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/int_types.hpp>
#include <llfs/log_device.hpp>

namespace llfs {

class PageRecyclerOptions
{
 public:
  static constexpr usize kDefaultInfoRefreshRate = 4;
  static constexpr usize kDefaultMaxRefsPerPage = 1 * kMiB;
  static constexpr usize kDefaultBatchSize = 20;
  static constexpr usize kDefaultRefreshFactor = 2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using Self = PageRecyclerOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  //----- --- -- -  -  -   -
  // (setters - begin)

  Self& set_info_refresh_rate(usize value) noexcept;

  Self& set_max_refs_per_page(usize value) noexcept;

  Self& set_batch_size(usize value) noexcept;

  Self& set_refresh_factor(usize value) noexcept;

  // (setters - end)
  //----- --- -- -  -  -   -

  usize info_refresh_rate() const noexcept
  {
    return this->info_refresh_rate_;
  }

  MaxRefsPerPage max_refs_per_page() const noexcept
  {
    return MaxRefsPerPage{this->max_refs_per_page_};
  }

  usize batch_size() const noexcept
  {
    return this->batch_size_;
  }

  usize refresh_factor() const noexcept
  {
    return this->refresh_factor_;
  }

  // The log space needed to insert a single page.
  //
  usize insert_grant_size() const;

  // The log space needed to remove a single page.
  //
  usize remove_grant_size() const;

  // The log space needed to insert and remove a single page.
  //
  usize total_page_grant_size() const;

  // Calculates the total required grant size for a single page at a given discovery depth.
  // (Discovery depth is the number of page references followed to find out that a page is now
  // recyclable; it is not necessarily the same as tree depth, though tree depth provides an upper
  // bound for discovery depth.)
  //
  usize total_grant_size_for_depth(u32 depth) const;

  // The size of a PageRecycler info slot.
  //
  usize info_slot_size() const;

  // The size of a PageRecycler batch commit slot.
  //
  usize commit_slot_size() const;

  // The target grant for the recycle task to maintain.
  //
  u64 recycle_task_target() const;

  // Returns true iff the recycler info slot needs to be refreshed in the log.
  //
  bool info_needs_refresh(slot_offset_type last_info_refresh_slot_lower_bound,
                          LogDevice& log_device) const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  // How many times per total log size to refresh the page recycler info.
  //
  usize info_refresh_rate_ = kDefaultInfoRefreshRate;

  // Name says it all.
  //
  usize max_refs_per_page_ = kDefaultMaxRefsPerPage;

  // The maximum number of pages to recycle at a time.  Batching records together helps amortize the
  // various fixed costs of page recycling (e.g., writing log pages to the recycler log and the page
  // allocator logs).
  //
  usize batch_size_ = kDefaultBatchSize;

  // The log amplification factor to target when writing refresh records so we can trim the log. For
  // example, refresh_factor = 2 means that for each page inserted, one is inserted.
  //
  usize refresh_factor_ = kDefaultRefreshFactor;
};

}  // namespace llfs

#endif  // LLFS_PAGE_RECYCLER_OPTIONS_HPP
