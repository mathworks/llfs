//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#ifndef LLFS_PAGE_CACHE_SLOT_HPP
#error This file must be included from/after page_cache_slot.hpp!
#endif

#include <llfs/metrics.hpp>

namespace llfs {

class PageCacheSlot::Pool : public boost::intrusive_ref_counter<Pool>
{
 public:
  using Self = Pool;

  static constexpr usize kDefaultEvictionCandidates = 8;

  using SlotStorage = std::aligned_storage_t<sizeof(batt::CpuCacheLineIsolated<PageCacheSlot>),
                                             alignof(batt::CpuCacheLineIsolated<PageCacheSlot>)>;

  struct Metrics {
    CountMetric<u64> max_slots{0};
    CountMetric<u64> indexed_slots{0};
    CountMetric<u64> query_count{0};
    CountMetric<u64> hit_count{0};
    CountMetric<u64> stale_count{0};
    CountMetric<u64> alloc_count{0};
    CountMetric<u64> evict_count{0};
    CountMetric<u64> insert_count{0};
    CountMetric<u64> erase_count{0};
    CountMetric<u64> full_count{0};
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit Pool(usize n_slots,
                usize eviction_candidates = Self::kDefaultEvictionCandidates) noexcept;

  ~Pool() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheSlot* get_slot(usize i) noexcept;

  PageCacheSlot* allocate() noexcept;

  usize index_of(const PageCacheSlot* slot) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  batt::CpuCacheLineIsolated<PageCacheSlot>* slots() noexcept;

  /** \brief Tries to find a slot that hasn't been used in a while to evict.
   *
   * Will keep on looping until it has made one attempt for each slot in the cache.  At that point,
   * we just give up and return nullptr.
   */
  PageCacheSlot* evict_lru();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const usize n_slots_;
  const usize eviction_candidates_;
  const std::string name_;
  std::unique_ptr<SlotStorage[]> slot_storage_;
  std::atomic<usize> n_allocated_{0};
  batt::Watch<usize> n_constructed_{0};
  Metrics metrics_;
};

}  //namespace llfs
