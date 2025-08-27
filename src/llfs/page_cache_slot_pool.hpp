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
#include <llfs/optional.hpp>

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>

#include <tuple>

namespace llfs {

/** \brief A pool of PageCacheSlot objects.
 *
 * Used to construct a PageDeviceCache.
 */
class PageCacheSlot::Pool : public boost::intrusive_ref_counter<Pool>
{
 public:
  friend class PageCacheSlot;

  using Self = Pool;

  /** \brief Aligned storage type for a single PageCacheSlot.  We allocate an array of this type
   * when constructing a Pool object, then construct the individual slots via placement-new as they
   * are needed.
   */
  using SlotStorage = std::aligned_storage_t<sizeof(batt::CpuCacheLineIsolated<PageCacheSlot>),
                                             alignof(batt::CpuCacheLineIsolated<PageCacheSlot>)>;

  /** \brief Observability metrics for a cache slot pool.
   */
  struct Metrics {
    FastCountMetric<i64> indexed_slots{0};
    FastCountMetric<i64> query_count{0};
    FastCountMetric<i64> hit_count{0};
    FastCountMetric<i64> stale_count{0};
    FastCountMetric<i64> allocate_count{0};
    FastCountMetric<i64> allocate_free_queue_count{0};
    FastCountMetric<i64> allocate_construct_count{0};
    FastCountMetric<i64> allocate_evict_count{0};
    FastCountMetric<i64> construct_count{0};
    FastCountMetric<i64> free_queue_insert_count{0};
    FastCountMetric<i64> free_queue_remove_count{0};
    FastCountMetric<i64> evict_count{0};
    FastCountMetric<i64> evict_prior_generation_count{0};
    FastCountMetric<i64> admit_count{0};
    FastCountMetric<i64> insert_count{0};
    FastCountMetric<i64> miss_count{0};
    FastCountMetric<i64> erase_count{0};
    FastCountMetric<i64> full_count{0};

    FastCountMetric<i64> admit_byte_count{0};
    FastCountMetric<i64> erase_byte_count{0};
    FastCountMetric<i64> evict_byte_count{0};

    CountMetric<i64> total_capacity_allocated{0};
    CountMetric<i64> total_capacity_freed{0};

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static Metrics& instance();

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    double hit_rate() const
    {
      const double query_count = this->query_count.get();
      const double non_miss_count = query_count - this->miss_count.get();

      return (query_count == 0) ? -1 : (non_miss_count / query_count);
    }

    i64 estimate_cache_bytes() const
    {
      return this->admit_byte_count.get() - this->evict_byte_count.get();
    }

    i64 estimate_total_limit() const
    {
      return this->total_capacity_allocated.get() - this->total_capacity_freed.get();
    }

   private:
    Metrics() = default;
  };

  using ExternalAllocation = PageCacheSlot::ExternalAllocation;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the default number of random eviction candidates to consider.
   *
   * Read from env var LLFS_CACHE_EVICTION_CANDIDATES if defined; otherwise
   * kDefaultEvictionCandidates is returned.
   */
  static usize default_eviction_candidate_count();

  /** \brief Creates a new PageCacheSlot::Pool.
   *
   * Objects of this type MUST be managed via boost::intrusive_ptr<Pool>.
   */
  static boost::intrusive_ptr<Pool> make_new(SlotCount n_slots, MaxCacheSizeBytes max_byte_size,
                                             std::string&& name)
  {
    return boost::intrusive_ptr<Pool>{new Pool{n_slots, max_byte_size, std::move(name)}};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Destroys a PageCacheSlot pool.
   *
   * Will panic if there are any pinned slots.
   */
  ~Pool() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void halt();

  void join();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the slot at the specified index (`i`).
   *
   * The passed index must refer to a slot that was previously returned by this->allocate(), or
   * behavior is undefined!
   */
  PageCacheSlot* get_slot(usize i);

  /** \brief Returns a cache slot in the `Invalid` state, ready to be filled by the caller.
   *
   * This function is guaranteed to return an available slot the first `n_slots` times it is called.
   * Thereafter, it will attempt to evict an unpinned slot that hasn't been used recently.  If no
   * such slot can be found, `nullptr` will be returned.
   */
  auto allocate(PageSize size_needed) -> std::tuple<PageCacheSlot*, ExternalAllocation>;

  /** \brief Returns the index of the specified slot object.
   *
   * If `slot` does not belong to this pool, behavior is undefined!
   */
  usize index_of(const PageCacheSlot* slot);

  /** \brief Attempts to evict and clear all allocated slots; returns the number of non-pinned,
   * cleared slots.
   */
  usize clear_all();

  /** \brief Adds the passed slot to the free queue.
   *
   * \return true if the queue had space and the slot was successfully pushed.
   */
  bool push_free_slot(PageCacheSlot* slot);

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

  i64 get_resident_size() const
  {
    return this->resident_size_.load();
  }

  i64 get_max_byte_size() const
  {
    return this->max_byte_size_;
  }

  Status set_max_byte_size(usize new_size_limit);

  ExternalAllocation allocate_external(usize byte_size);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Constructs a new Pool with capacity for `n_slots` cached pages.
   */
  explicit Pool(SlotCount n_slots, MaxCacheSizeBytes max_byte_size, std::string&& name) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::CpuCacheLineIsolated<PageCacheSlot>* slots();

  /** \brief Attempts to construct a new slot; returns non-null if successful.
   */
  PageCacheSlot* construct_new_slot();

  /** \brief Steps the clock hand forward by 1, returning the new value.
   *
   * Automatically handles wrap-around; may update n_slots_constructed if possible wrap-around
   * detected.
   */
  usize advance_clock_hand(usize& n_slots_constructed);

  /** \brief If the passed observed size is over the limit, evict pages until resident size is at or
   * below the maximum size.
   */
  Status enforce_max_size(i64 observed_resident_size, usize max_steps = 0);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const usize n_slots_;
  std::atomic<usize> max_byte_size_;
  const std::string name_;
  std::unique_ptr<SlotStorage[]> slot_storage_;
  std::atomic<i64> resident_size_{0};
  std::atomic<usize> clock_hand_{0};
  std::atomic<usize> n_allocated_{0};
  std::atomic<usize> n_constructed_{0};
  std::atomic<bool> halt_requested_;
  Metrics& metrics_ = Metrics::instance();
  boost::lockfree::queue<PageCacheSlot*, boost::lockfree::capacity<32768>,
                         boost::lockfree::fixed_sized<true>>
      free_queue_;
};

}  //namespace llfs
