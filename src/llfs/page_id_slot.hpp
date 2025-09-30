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
#include <llfs/page_loader.hpp>
#include <llfs/pin_page_to_job.hpp>

#include <llfs/metrics.hpp>

#include <batteries/async/latch.hpp>
#include <batteries/status.hpp>

#include <memory>

namespace llfs {

class PageView;
class PinnedPage;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// A PageId and a weak cache slot reference; speeds up lookup for pages that are in-cache.
//
struct PageIdSlot {
  using Self = PageIdSlot;

  struct Metrics {
    FastCountMetric<usize> load_total_count;
    FastCountMetric<usize> load_slot_hit_count;
    FastCountMetric<usize> load_slot_miss_count;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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

  /** \brief Pass-though to the non-slot version, for PinnedPageT != PinnedPage.
   */
  template <typename PinnedPageT>
  static batt::StatusOr<PinnedPageT> load_through_impl(PageCacheSlot::AtomicRef& cache_slot_ref
                                                       [[maybe_unused]],
                                                       BasicPageLoader<PinnedPageT>& loader,
                                                       PageId page_id,
                                                       const PageLoadOptions& load_options)
  {
    return loader.load_page(page_id, load_options);
  }

  /** \brief Attempts to pin the passed cache slot using the specified `page_id`; if this fails,
   * then falls back on loading the page from the `loader`, updating `cache_slot_ref` if successful.
   */
  static batt::StatusOr<PinnedPage> load_through_impl(PageCacheSlot::AtomicRef& cache_slot_ref,
                                                      PageLoader& loader, PageId page_id,
                                                      const PageLoadOptions& load_options);

  /** \brief Attempts to pin the slot using the specified page_id.
   *
   * If pin succeeded, but the page failed to load into the slot when it was originally added to
   * the cache, then the page load error status code is returned.
   *
   * \return The PinnedPage if successful, llfs::StatusCode::kPinFailedPageEvicted otherwise
   * (unless load error; see above)
   */
  static batt::StatusOr<PinnedPage> try_pin_impl(PageCacheSlot::AtomicRef& cache_slot_ref,
                                                 PageId page_id);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageId page_id;
  mutable PageCacheSlot::AtomicRef cache_slot_ref;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
      this->cache_slot_ref = PageCacheSlot::AtomicRef{};
    }
    return *this;
  }

  template <typename PinnedPageT>
  batt::StatusOr<PinnedPageT> load_through(BasicPageLoader<PinnedPageT>& loader,
                                           const PageLoadOptions& load_options) const;

  batt::StatusOr<PinnedPage> try_pin_through(BasicPageLoader<PinnedPage>& loader,
                                             const PageLoadOptions& load_options) const;

  batt::StatusOr<PinnedPage> try_pin() const;
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename PinnedPageT>
inline batt::StatusOr<PinnedPageT> PageIdSlot::load_through(
    BasicPageLoader<PinnedPageT>& loader, const PageLoadOptions& load_options) const
{
  return Self::load_through_impl(this->cache_slot_ref, loader, this->page_id, load_options);
}

}  // namespace llfs

#endif  // LLFS_PAGE_ID_SLOT_HPP
