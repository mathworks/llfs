//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_JOB_HPP
#define LLFS_PAGE_CACHE_JOB_HPP

#include <llfs/finalized_page_cache_job.hpp>
#include <llfs/method_binder.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/page_size.hpp>
#include <llfs/page_tracer.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/async/backoff.hpp>

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#define JOB_DEBUG(job)                                                                             \
  if (::llfs::PageCache::job_debug_on())                                                           \
  (job).debug()

namespace llfs {

class PageCacheJob : public PageLoader
{
 public:
  friend class PageCache;

  // A function to build a page when it is first requested.
  //
  using DeferredNewPageFn = std::function<std::shared_ptr<PageView>()>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // A lazily-built new page in the context of this job.
  //
  class NewPage
  {
   public:
    NewPage() = default;

    explicit NewPage(PinnedPage&& pinned_page, IsRecoveredPage is_recovered) noexcept;

    bool set_view(std::shared_ptr<const PageView>&& v);

    bool has_view() const;

    std::shared_ptr<const PageView> view() const;

    std::shared_ptr<PageBuffer> buffer() const;

    std::shared_ptr<const PageBuffer> const_buffer() const
    {
      return this->pinned_page_.get_page_buffer();
    }

    const PackedPageHeader& const_page_header() const
    {
      return this->pinned_page_->header();
    }

    StatusOr<PinnedPage> get_pinned_page() const
    {
      return {this->pinned_page_};
    }

   private:
    PinnedPage pinned_page_;
    bool has_view_ = false;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static std::atomic<u64>& counter()
  {
    static std::atomic<u64> counter_{0};
    return counter_;
  }

  static usize n_jobs_count();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const u64 job_id = counter().fetch_add(1);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheJob(const PageCacheJob&) = delete;
  PageCacheJob& operator=(const PageCacheJob&) = delete;

  explicit PageCacheJob(PageCache* cache) noexcept;

  ~PageCacheJob();

  std::atomic<i32> get_latency_sample_rate_spec{64};
  std::atomic<bool> count_get_calls{false};

  PageCache& cache() const noexcept
  {
    return *cache_;
  }

  // Set `base_job` as the job immediately prior to this one; any changes in `base_job` can be seen
  // by this one (even if `base_job` is not yet durably committed) and proper storage ordering
  // between `this` and `base_job` on commmit will be guaranteed to preserve consistency on crash
  // recovery.
  //
  void set_base_job(const FinalizedPageCacheJob& base_job);

  Status await_base_job_durable() const;

  // Returns true iff the given page id refers to a new page allocated within the scope of this job
  // via `new_page`.
  //
  bool is_page_new(PageId page_id) const;

  // Returns true iff `this->is_new_page(page_id)` and `pin_new` has been invoked to pin the fully
  // built page view to the job.
  //
  bool is_page_new_and_pinned(PageId page_id) const;

  // Allocate a new page and return a buffer to be filled by the caller with the contents of page.
  //
  // The page will not be committed to storage when this job is committed unless a PageView
  // implementation is created for the formatted page and added to the job via `pin_new`.  Otherwise
  // the job will have no way of tracing page references from the new page to determine "liveness".
  //
  StatusOr<std::shared_ptr<PageBuffer>> new_page(PageSize size,
                                                 batt::WaitForResource wait_for_resource,
                                                 llfs::PageCacheOvercommit& overcommit,
                                                 const PageLayoutId& layout_id,
                                                 LruPriority lru_priority, u64 callers,
                                                 const batt::CancelToken& cancel_token);

  StatusOr<std::shared_ptr<PageBuffer>> new_page(PageSize size,
                                                 batt::WaitForResource wait_for_resource,
                                                 const PageLayoutId& layout_id,
                                                 LruPriority lru_priority, u64 callers,
                                                 const batt::CancelToken& cancel_token)
  {
    return this->new_page(size, wait_for_resource, llfs::PageCacheOvercommit::not_allowed(),
                          layout_id, lru_priority, callers, cancel_token);
  }

  // Inserts a new page into the cache.  The passed PageView must have been created using a
  // PageBuffer returned by `new_page` for this job, or we will panic.
  //
  // TODO [tastolfi 2025-08-27] Is this still needed, after pin-on-allocate?
  StatusOr<PinnedPage> pin_new(std::shared_ptr<PageView>&& page_view, LruPriority lru_priority,
                               u64 callers);

  // TODO [tastolfi 2025-08-27] Is this still needed, after pin-on-allocate?
  StatusOr<PinnedPage> pin_new_impl(
      std::shared_ptr<PageView>&& page_view, LruPriority lru_priority, u64 callers,
      std::function<StatusOr<PinnedPage>(const std::function<StatusOr<PinnedPage>()>&)>&&
          put_with_retry);

  /** \brief Inserts a new page into the cache, retrying according to the passed policy if there are
   * no free cache slots.
   */
  // TODO [tastolfi 2025-08-27] Is this still needed, after pin-on-allocate?
  template <typename RetryPolicy>
  StatusOr<PinnedPage> pin_new_with_retry(std::shared_ptr<PageView>&& page_view,
                                          LruPriority lru_priority, u64 callers,
                                          RetryPolicy&& retry_policy)
  {
    StatusOr<PinnedPage> pinned_page = this->pin_new_impl(
        std::move(page_view), lru_priority, callers,
        [&retry_policy](const auto& op) -> StatusOr<PinnedPage> {
          return batt::with_retry_policy(
              retry_policy, /*op_name=*/"PageCacheJob::pin_new() - Cache::put_view", op,
              batt::TaskSleepImpl{},
              /*is_retryable_status=*/[](const batt::Status& status) {
                return batt::status_is_retryable(status) ||
                       (status == ::llfs::make_status(StatusCode::kCacheSlotsFull));
              });
        });

    return pinned_page;
  }

  // Register a previously allocated page (returned by `this->new_page`) to be pinned the first time
  // it is requested.
  //
  void pin_new_if_needed(PageId page_id, DeferredNewPageFn&& pin_page_fn, u64 callers);

  // Attempt to load the given page into the cache; if successful, mark the page as new, like it had
  // been created via `PageCacheJob::new_page`.  This method is used when attempting to do a
  // roll-back or roll-forward to resolve partial transactions in a WAL.
  //
  // We pass caller_uuid and slot here so we don't accidentally recover a bunch of pages thinking
  // they are what was written by a prepare checkpoint, when in fact they were all coincidentally
  // allocated and written by some other storage client.
  //
  Status recover_page(PageId page_id, const boost::uuids::uuid& caller_uuid,
                      slot_offset_type caller_slot);

  // Mark the page as deleted in this job.
  //
  Status delete_page(PageId page_id);

  // Save a pinned copy of the given page to the job, so it will stay valid for as long as the job
  // lives.
  //
  void pin(PinnedPage&& page_view);

  // Unpin a page from the job, possibly allowing it to be evicted from cache.
  //
  void unpin(PageId page_id);

  // Unpin all pages pinned to this job.
  //
  void unpin_all();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // PageLoader interface
  //

  PageCache* page_cache() const override
  {
    return std::addressof(this->cache());
  }

  // Hint to the cache that it is likely we will ask for this page in the near future.
  //
  void prefetch_hint(PageId page_id) override;

  // Attempt to pin the page without loading it.
  //
  // Should return kUnavailable if the page is not cached.
  //
  StatusOr<PinnedPage> try_pin_cached_page(PageId page_id, const PageLoadOptions& options) override;

  // Load the page, first checking the pinned pages and views in this job.
  //
  StatusOr<PinnedPage> load_page(PageId page_id, const PageLoadOptions& options) override;
  //
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<PinnedPage> get_already_pinned(PageId page_id) const;

  Optional<PinnedPage> get_already_pinned(PageId page_id, PinPageToJob pin_page_to_job);

  StatusOr<PinnedPage> get_new_pinned_page(PageId page_id);

  void const_prefetch_hint(PageId page_id) const;

  StatusOr<PinnedPage> const_try_pin_cached_page(PageId page_id,
                                                 const PageLoadOptions& options) const;

  StatusOr<PinnedPage> const_get(PageId page_id, const PageLoadOptions& options) const;

  std::ostream& debug()
  {
    return this->debug_;
  }

  std::string debug_info() const
  {
    return this->debug_.str();
  }

  // Adds a "root" level ref count to the specified page, ensuring it will not be recycled.
  //
  void new_root(PageId page_id);

  // Removes a "root" level ref count from the specified page, possibly allowing it to be recycled.
  //
  void delete_root(PageId page_id);

  // TODO [tastolfi 2022-01-04] document me
  //
  void update_root_set(const PageRefCount& prc);

  // unpin and purge all new pages that aren't reachable from the root set.  Return the number of
  // pages pruned from the job.
  //
  StatusOr<usize> prune(u64 callers);

  template <typename PageIdFn>
  Status trace_new_roots(PageLoader& page_loader, PageIdFn&& page_id_fn) const
  {
    LoadingPageTracer loading_tracer{page_loader};
    CachingPageTracer caching_tracer{this->cache().devices_by_id(), loading_tracer};
    return trace_refs_recursive(
        caching_tracer,

        // Trace all new pages in the root set.
        //
        as_seq(this->new_pages_.begin(), this->new_pages_.end())  //
            | seq::map([](const auto& kv_pair) -> PageId {
                return kv_pair.first;
              })  //
            | seq::filter([this](const PageId& id) {
                auto iter = this->root_set_delta_.find(id);
                return iter != this->root_set_delta_.end() && iter->second > 0;
              }),

        // Recursion predicate
        //
        [this](PageId page_id) {
          return this->is_page_new_and_pinned(page_id);
        },

        // Action per traced page id
        //
        BATT_FORWARD(page_id_fn));
  }

  /** \brief Returns the number of new pages added by this job.
   */
  usize new_page_count() const
  {
    return this->new_pages_.size();
  }

  /** \brief Returns the number of pages currently pinned to the job; this includes both new pages
   * and pre-existing pages.
   */
  usize pinned_page_count() const
  {
    return this->pinned_.size();
  }

  /** \brief Returns the number of pages deleted by this job.
   */
  usize deleted_page_count() const
  {
    return this->deleted_pages_.size();
  }

  bool is_pruned() const
  {
    return this->pruned_;
  }

  const std::unordered_map<PageId, NewPage, PageId::Hash>& get_new_pages() const
  {
    return this->new_pages_;
  }

  const std::unordered_set<PageId, PageId::Hash>& get_deleted_pages() const
  {
    return this->deleted_pages_;
  }

  const std::unordered_map<PageId, i32, PageId::Hash>& get_root_set_delta() const
  {
    return this->root_set_delta_;
  }

  bool is_recovered_page(PageId page_id) const
  {
    return this->recovered_pages_.count(page_id) != 0;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct PinState {
    PinnedPage pinned_page;
  };

  struct DeferredPinState {
    DeferredNewPageFn deferred_new_page_fn;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCache* const cache_;
  std::unordered_map<PageId, PinState, PageId::Hash> pinned_;
  std::unordered_map<PageId, NewPage, PageId::Hash> new_pages_;
  std::unordered_set<PageId, PageId::Hash> deleted_pages_;
  std::unordered_map<PageId, i32, PageId::Hash> root_set_delta_;
  std::unordered_map<PageId, DeferredPinState, PageId::Hash> deferred_new_pages_;
  std::unordered_set<PageId, PageId::Hash> recovered_pages_;
  bool pruned_ = false;
  std::ostringstream debug_;
  FinalizedPageCacheJob base_job_;
  u64 base_job_id_{0};
};

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_JOB_HPP
