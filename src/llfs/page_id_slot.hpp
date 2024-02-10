//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ID_SLOT_HPP
#define LLFS_PAGE_ID_SLOT_HPP

#include <llfs/api_types.hpp>
#include <llfs/page_cache_slot.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout_id.hpp>

#include <llfs/metrics.hpp>

#include <batteries/async/latch.hpp>
#include <batteries/status.hpp>

#include <memory>

namespace llfs {

class PageView;
class PinnedPage;
class PageLoader;

enum struct PinPageToJob : u8 {
  kFalse = 0,
  kTrue = 1,
  kDefault = 2,
};

// Convert `pin_page` to a boolean value.
//
bool bool_from(PinPageToJob pin_page, bool default_value);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// A PageId and a weak cache slot reference; speeds up lookup for pages that are in-cache.
//
struct PageIdSlot {
  using Self = PageIdSlot;

  using AtomicCacheSlotRefT = PageCacheSlot::AtomicRef;

  struct Metrics {
    CountMetric<usize> load_total_count;
    CountMetric<usize> load_slot_hit_count;
    CountMetric<usize> load_slot_miss_count;
  };

  static Metrics& metrics()
  {
    static Metrics m_;
    return m_;
  }

  static Self from_page_id(PageId id)
  {
    return Self{
        .page_id = id,
        .cache_slot_ref = {},
    };
  }

  static Self from_pinned_page(const PinnedPage& pinned);

  PageId page_id;
  mutable AtomicCacheSlotRefT cache_slot_ref;

  operator PageId() const
  {
    return this->page_id;
  }

  page_id_int int_value() const
  {
    return this->page_id.int_value();
  }

  bool is_valid() const
  {
    return this->page_id.is_valid();
  }

  Self& operator=(PageId id)
  {
    if (BATT_HINT_TRUE(id != this->page_id)) {
      this->page_id = id;
      this->cache_slot_ref = AtomicCacheSlotRefT{};
    }
    return *this;
  }

  batt::StatusOr<PinnedPage> load_through(PageLoader& loader,
                                          const Optional<PageLayoutId>& required_layout,
                                          PinPageToJob pin_page_to_job,
                                          OkIfNotFound ok_if_not_found) const noexcept;

  batt::StatusOr<PinnedPage> try_pin() const noexcept;
};

}  // namespace llfs

#endif  // LLFS_PAGE_ID_SLOT_HPP
