#pragma once
#ifndef LLFS_PAGE_CACHE_JOB_HPP
#define LLFS_PAGE_CACHE_JOB_HPP

#include <llfs/finalized_page_cache_job.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/page_size.hpp>
#include <llfs/pinned_page.hpp>

#include <turtle/util/method_binder.hpp>

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#define JOB_DEBUG(job)                                                                             \
  if (::llfs::PageCache::job_debug_on())                                                           \
  (job).debug()

namespace llfs {

class PageCacheJob;

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

    explicit NewPage(std::shared_ptr<PageBuffer>&& buffer) noexcept;

    bool set_view(std::shared_ptr<const PageView>&& v);

    bool has_view() const;

    std::shared_ptr<const PageView> view() const;

    std::shared_ptr<PageBuffer> buffer() const;

    std::shared_ptr<const PageBuffer> const_buffer() const
    {
      return this->view()->data();
    }

    const PackedPageHeader& const_page_header() const
    {
      return get_page_header(*this->const_buffer());
    }

   private:
    std::shared_ptr<PageBuffer> buffer_;
    Optional<std::shared_ptr<const PageView>> view_;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static std::atomic<u64>& counter()
  {
    static std::atomic<u64> counter_{0};
    return counter_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const u64 job_id = counter().fetch_add(1);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheJob(const PageCacheJob&) = delete;
  PageCacheJob& operator=(const PageCacheJob&) = delete;

  explicit PageCacheJob(PageCache* cache) noexcept;

  ~PageCacheJob();

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
                                                 u64 callers);

  // Inserts a new page into the cache.  The passed PageView must have been created using a
  // PageBuffer returned by `new_page` for this job, or we will panic.
  //
  StatusOr<PinnedPage> pin_new(std::shared_ptr<PageView>&& page_view, u64 callers);

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

  // Mark the page as deleted in this job.  Will load the page as a side-effect, in order to access
  // the set of pages referenced by the page contents.  Returns `true` if the page is being deleted;
  // `false` if it doesn't exist (i.e., it was already deleted).
  //
  Status delete_page(PageId page_id);

  // Save a pinned copy of the given page to the job, so it will stay valid for as long as the job
  // lives.
  //
  void pin(PinnedPage&& page_view);

  // Unpin a page from the job, possibly allowing it to be evicted from cache.
  //
  void unpin(PageId page_id);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // PageLoader interface
  //
  using PageLoader::get;

  // Hint to the cache that it is likely we will ask for this page in the near future.
  //
  void prefetch_hint(PageId page_id) override;

  // Load the page, first checking the pinned pages and views in this job.
  //
  StatusOr<PinnedPage> get(PageId page_id, const Optional<PageLayoutId>& required_layout,
                           PinPageToJob pin_page_to_job) override;
  //
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<PinnedPage> get_already_pinned(PageId page_id) const;

  void const_prefetch_hint(PageId page_id) const;

  StatusOr<PinnedPage> const_get(PageId page_id,
                                 const Optional<PageLayoutId>& required_layout) const;

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
    return trace_refs_recursive(
        page_loader,

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

  usize new_page_count() const
  {
    return this->new_pages_.size();
  }

  usize pinned_page_count() const
  {
    return this->pinned_.size();
  }

  bool is_pruned() const
  {
    return this->pruned_;
  }

  const std::unordered_map<PageId, NewPage, PageId::Hash>& get_new_pages() const
  {
    return this->new_pages_;
  }

  const std::unordered_map<PageId, PinnedPage, PageId::Hash>& get_deleted_pages() const
  {
    return this->deleted_pages_;
  }

  const std::unordered_map<PageId, i32, PageId::Hash>& get_root_set_delta() const
  {
    return this->root_set_delta_;
  }

  TURTLE_DB_METHOD_BINDER(PageCacheJob, prefetch_hint, Prefetch);
  TURTLE_DB_METHOD_BINDER(PageCacheJob, get, Get);

  int binder_count = 0;

 private:
  PageCache* const cache_;
  std::unordered_map<PageId, PinnedPage, PageId::Hash> pinned_;
  std::unordered_map<PageId, NewPage, PageId::Hash> new_pages_;
  std::unordered_map<PageId, PinnedPage, PageId::Hash> deleted_pages_;
  std::unordered_map<PageId, i32, PageId::Hash> root_set_delta_;
  std::unordered_map<PageId, std::function<auto()->std::shared_ptr<PageView>>, PageId::Hash>
      deferred_new_pages_;
  bool pruned_ = false;
  std::ostringstream debug_;
  FinalizedPageCacheJob base_job_;
  u64 base_job_id_{0};
};

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_JOB_HPP
