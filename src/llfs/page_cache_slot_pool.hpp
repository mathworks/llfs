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

/** \brief A pool of PageCacheSlot objects.
 *
 * Used to construct a PageDeviceCache.
 */
class PageCacheSlot::Pool : public boost::intrusive_ref_counter<Pool>
{
 public:
  using Self = Pool;

  /** \brief The default number of randomly-selected slots to consider when trying to evict a slot
   * that hasn't been accessed recently.
   */
  static constexpr usize kDefaultEvictionCandidates = 8;

  /** \brief Aligned storage type for a single PageCacheSlot.  We allocate an array of this type
   * when constructing a Pool object, then construct the individual slots via placement-new as they
   * are needed.
   */
  using SlotStorage = std::aligned_storage_t<sizeof(batt::CpuCacheLineIsolated<PageCacheSlot>),
                                             alignof(batt::CpuCacheLineIsolated<PageCacheSlot>)>;

  /** \brief Observability metrics for a cache slot pool.
   */
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

  /** \brief Creates a new PageCacheSlot::Pool.
   *
   * Objects of this type MUST be managed via boost::intrusive_ptr<Pool>.
   */
  template <typename... Args>
  static boost::intrusive_ptr<Pool> make_new(Args&&... args)
  {
    return boost::intrusive_ptr<Pool>{new Pool(BATT_FORWARD(args)...)};
  }

  /** \brief Destroys a PageCacheSlot pool.
   *
   * Will panic if there are any pinned slots.
   */
  ~Pool() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the slot at the specified index (`i`).
   *
   * The passed index must refer to a slot that was previously returned by this->allocate(), or
   * behavior is undefined!
   */
  PageCacheSlot* get_slot(usize i) noexcept;

  /** \brief Returns a cache slot in the `Invalid` state, ready to be filled by the caller.
   *
   * This function is guaranteed to return an available slot the first `n_slots` times it is called.
   * Thereafter, it will attempt to evict an unpinned slot that hasn't been used recently.  If no
   * such slot can be found, `nullptr` will be returned.
   */
  PageCacheSlot* allocate() noexcept;

  /** \brief Returns the index of the specified slot object.
   *
   * If `slot` does not belong to this pool, behavior is undefined!
   */
  usize index_of(const PageCacheSlot* slot) noexcept;

  /** \brief Returns the metrics for this pool.
   */
  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  /** \brief Returns the metrics for this pool.
   */
  Metrics& metrics()
  {
    return this->metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Constructs a new Pool with capacity for `n_slots` cached pages.
   */
  explicit Pool(usize n_slots, std::string&& name,
                usize eviction_candidates = Self::kDefaultEvictionCandidates) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
