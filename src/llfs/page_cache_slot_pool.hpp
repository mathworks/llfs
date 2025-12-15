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

    /** \brief The total size of all pages that have ever been pinned to the cache.
     */
    FastCountMetric<i64> pinned_byte_count{0};

    /** \brief The total size of all pages that have ever been unpinned from the cache.
     */
    FastCountMetric<i64> unpinned_byte_count{0};

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

    /** \brief Returns an estimate of the total byte size of page data in the cache.
     *
     * This may not be accurate since we do not synchronize across threads or across the two
     * monotonic counters whose difference is the true value; because we observe the negative
     * counter first, the estimate is likely to be higher than the true value.
     */
    i64 estimate_cache_bytes() const
    {
      const i64 observed_evict_byte_count = this->evict_byte_count.get();
      return this->admit_byte_count.get() - observed_evict_byte_count;
    }

    i64 estimate_total_limit() const
    {
      const i64 observed_freed = this->total_capacity_freed.get();
      return this->total_capacity_allocated.get() - observed_freed;
    }

    /** \brief Returns an estimate of the total byte size of pinned page data in the cache.
     *
     * This may not be accurate since we do not synchronize across threads or across the two
     * monotonic counters whose difference is the true value; because we observe the negative
     * counter first, the estimate is likely to be higher than the true value.
     */
    i64 estimate_pinned_bytes() const
    {
      const i64 observed_unpinned = this->unpinned_byte_count.get();
      return this->pinned_byte_count.get() - observed_unpinned;
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

  /** \brief Requests shutdown on this object; does not block (see join).
   */
  void halt();

  /** \brief Waits for shutdown to complete on this object; does not request shutdown (see halt).
   */
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

  /** \brief Returns the current size (bytes) of the page data in the cache, including all external
   * allocations (see allocate_external).
   */
  i64 get_resident_size() const
  {
    return this->resident_size_.load();
  }

  /** \brief Returns the maximum allowed size (bytes) of all page data in the cache, including all
   * external allocations (see allocate_external).
   */
  i64 get_max_byte_size() const
  {
    return this->max_byte_size_;
  }

  /** \brief Dynamically adjusts the size of the cache; this may cause data to be evicted, if the
   * new size limit is smaller than the current one.  If too much page data is pinned, we may be
   * unable to shrink the cache to the desired limit.  If this happens, the size limit will not be
   * changed, and `batt::StatusCode::kResourceExhausted` will be returned.
   */
  Status set_max_byte_size(usize new_size_limit);

  /** \brief Allocates against the maximum byte size limit, without explicitly allocating or filling
   * any cache slots.
   *
   * This is a mechanism for limiting overall memory usage in an allocation by claiming some of the
   * page cache quota for external use.  The returned object is a move-only RAII type that returns
   * the allocated amount to the cache when the last (moved) copy is destructed.
   */
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

  // The number of items to pre-allocate in the free queue.
  //
  static constexpr usize kFreeQueueStaticCapacity = 32768;

  // The maximum number of configured cache slots for this pool.
  //
  const usize n_slots_;

  // The current maximum allowed total bytes for all pages in all slots of this pool; can be
  // dynamically adjusted (see set_max_byte_size).
  //
  std::atomic<usize> max_byte_size_;

  // A human-readable debug name for this cache slot pool.
  //
  const std::string name_;

  // The backing storage for the slots in this pool; this memory is lazily initialized as new slots
  // are constructed.
  //
  std::unique_ptr<SlotStorage[]> slot_storage_;

  // The current total bytes in use by pages in the cache and external allocations.
  //
  std::atomic<i64> resident_size_{0};

  // The position of the CLOCK hand; this is the next candidate for eviction if there is memory
  // pressure.
  //
  std::atomic<usize> clock_hand_{0};

  // The number of elements at the front of this->slot_storage_ which have been allocated, ever (not
  // necessarily the number currently occupied).
  //
  std::atomic<usize> n_allocated_{0};

  // The number of elements at the front of this->slot_storage_ which have been constructed, ever;
  // this number trails this->n_allocated_ as storage for slots is lazily initialized.
  //
  std::atomic<usize> n_constructed_{0};

  // Set by this->halt(); indicates the pool has been requested to shut down.
  //
  std::atomic<bool> halt_requested_;

  // Diagnostic metrics for this pool.
  //
  Metrics& metrics_ = Metrics::instance();

  // A concurrent FIFO queue of free slots; this is consulted first when a new allocation request
  // comes in.  This is because sometimes cache slots are freed explicitly (e.g., when a page is
  // de-allocated in storage), so we want an inexpensive way to reuse these slots without needlessly
  // evicting other pages.
  //
  boost::lockfree::queue<PageCacheSlot*, boost::lockfree::capacity<kFreeQueueStaticCapacity>,
                         boost::lockfree::fixed_sized<true>>
      free_queue_;
};

}  //namespace llfs
