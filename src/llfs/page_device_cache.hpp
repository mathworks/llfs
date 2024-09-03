//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_DEVICE_CACHE_HPP
#define LLFS_PAGE_DEVICE_CACHE_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/page_cache_slot.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_view.hpp>

#include <batteries/async/latch.hpp>

#include <boost/intrusive_ptr.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <vector>

namespace llfs {

/** \brief A lock-free cache for a single PageDevice.
 *
 * This cache is populated with slots from a pool passed in at construction time.  This pool may be
 * shared among many different per-device caches.  If there is memory pressure, cached data may be
 * evicted (i.e. stolen) from a cache that hasn't accessed it in a while and given to another cache
 * that is using the same pool.  If the data is pinned, however, this will never happen.
 */
class PageDeviceCache
{
 public:
  static constexpr usize kInvalidIndex = ~usize{0};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageDeviceCache(const PageIdFactory& page_ids,
                           boost::intrusive_ptr<PageCacheSlot::Pool>&& slot_pool) noexcept;

  PageDeviceCache(const PageDeviceCache&) = delete;
  PageDeviceCache& operator=(const PageDeviceCache&) = delete;

  ~PageDeviceCache() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheSlot::Pool::Metrics& metrics()
  {
    return this->slot_pool_->metrics();
  }

  /** \brief Returns the PageDevice id factory passed in at construction time.
   */
  const PageIdFactory& page_ids() const noexcept;

  /** \brief Returns a PinnedRef to the cache slot for the given page.
   *
   * If the specified page is was not present in the cache, then the initialize function will be
   * called to start the process of loading the page data into the slot.
   */
  batt::StatusOr<PageCacheSlot::PinnedRef> find_or_insert(
      PageId key, const std::function<void(const PageCacheSlot::PinnedRef&)>& initialize);

  /** \brief Removes the specified key from this cache, if it is currently present.
   */
  void erase(PageId key);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Returns a reference to the atomic cache slot index integer for the given physical page
   * on the device for this cache.
   */
  std::atomic<usize>& get_slot_index_ref(i64 physical_page);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PageIdFactory page_ids_;
  boost::intrusive_ptr<PageCacheSlot::Pool> slot_pool_;
  std::vector<usize> cache_;
};

}  //namespace llfs

#endif  // LLFS_PAGE_DEVICE_CACHE_HPP
