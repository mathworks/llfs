#include <llfs/page_cache_job.hpp>
//

#include <llfs/trace_refs_recursive.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageCacheJob

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheJob::PageCacheJob(PageCache* cache) noexcept : cache_{cache}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheJob::~PageCacheJob()
{
  BATT_CHECK_EQ(0, binder_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::set_base_job(const FinalizedPageCacheJob& base_job)
{
  this->base_job_ = base_job;
  this->base_job_id_ = base_job.job_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCacheJob::await_base_job_durable() const
{
  return this->base_job_.await_durable();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheJob::is_page_new(PageId id) const
{
  return this->new_pages_.count(id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheJob::is_page_new_and_pinned(PageId page_id) const
{
  auto iter = this->new_pages_.find(page_id);
  return (iter != this->new_pages_.end()) &&
         (iter->second.has_view() || this->deferred_new_pages_.count(page_id));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> PageCacheJob::new_page(
    PageSize size, batt::WaitForResource wait_for_resource, u64 callers)
{
  // TODO [tastolfi 2021-04-07] instead of WaitForResource::kTrue, implement a backoff-and-retry
  // loop with a cancel token.
  //
  StatusOr<std::shared_ptr<PageBuffer>> buffer = this->cache_->allocate_page_of_size(
      size, wait_for_resource, callers | Caller::PageCacheJob_new_page, this->job_id);
  BATT_REQUIRE_OK(buffer);

  const PageId page_id = buffer->get()->page_id();

  this->pruned_ = false;
  this->new_pages_.emplace(page_id, NewPage{batt::make_copy(*buffer)});

  return buffer;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::pin(PinnedPage&& pinned_page)
{
  const PageId id = pinned_page->page_id();
  const bool inserted = this->pinned_.emplace(id, std::move(pinned_page)).second;
  (void)inserted;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCacheJob::pin_new(std::shared_ptr<PageView>&& page_view, u64 callers)
{
  BATT_CHECK_NOT_NULLPTR(page_view);

  const PageId id = page_view->page_id();

  // Find the `NewPage` object for this page.
  //
  auto iter = this->new_pages_.find(id);
  BATT_CHECK_NE(iter, this->new_pages_.end())
      << "pin_new called on a page that was not allocated by this job!";

  // Try to set the view, panicking if there is already a view for this page.
  //
  this->pruned_ = false;
  NewPage& new_page = iter->second;
  bool set_view_ok = new_page.set_view(batt::make_copy(page_view));
  BATT_CHECK(set_view_ok) << "pin_new called multiple times for the same page!";

  // Insert the page into the main cache.  Since page_ids are universally unique, there is no
  // problem doing this even before the page has been durably written to storage.
  //
  StatusOr<PinnedPage> pinned_page = this->cache_->put_view(
      std::move(page_view), callers | Caller::PageCacheJob_pin_new, this->job_id);

  BATT_REQUIRE_OK(pinned_page) << batt::LogLevel::kInfo << "Failed to pin page " << id
                               << ", reason: " << pinned_page.status();

  // Add to the pinned set.
  //
  this->pinned_.emplace(id, *pinned_page);

  return pinned_page;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::pin_new_if_needed(PageId page_id,
                                     std::function<std::shared_ptr<PageView>()>&& pin_page_fn,
                                     u64 /*callers - TODO [tastolfi 2021-12-03] */)
{
  BATT_CHECK(this->is_page_new(page_id));
  BATT_CHECK_EQ(this->deferred_new_pages_.count(page_id), 0u);

  this->pruned_ = false;
  this->deferred_new_pages_.emplace(page_id, std::move(pin_page_fn));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::unpin(PageId id)
{
  LOG(INFO) << "PageCacheJob::unpin(" << id << ")";
  this->pinned_.erase(id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCacheJob::get(PageId page_id,
                                       const Optional<PageLayoutId>& required_layout,
                                       PinPageToJob pin_page_to_job)
{
  // First check in the pinned pages table.
  {
    Optional<PinnedPage> already_pinned = this->get_already_pinned(page_id);
    if (already_pinned) {
      return std::move(*already_pinned);
    }
  }

  // If not pinned, then check to see if its a new page that hasn't been built yet.
  {
    auto iter = this->new_pages_.find(page_id);
    if (iter != this->new_pages_.end()) {
      NewPage& new_page = iter->second;

      if (!new_page.has_view()) {
        auto iter2 = this->deferred_new_pages_.find(page_id);
        if (iter2 != this->deferred_new_pages_.end()) {
          auto build_page_fn = std::move(iter2->second);
          this->deferred_new_pages_.erase(iter2);
          return this->pin_new(std::move(build_page_fn)(), Caller::Unknown);
        }
      }

      BATT_CHECK(new_page.has_view()) << "If the page has a view associated with it, then it "
                                         "should have been pinned to the job already "
                                         "inside `pin_new`; possible race condition?  (remember, "
                                         "PageCacheJob is not thread-safe)";

      LOG(WARNING) << "The specified page has not yet been built/pinned to the job."
                   << BATT_INSPECT(page_id);

      return Status{batt::StatusCode::kUnavailable};  // TODO [tastolfi 2021-10-20]
    }
  }

  // Fall back on the cache or base job if it is available.
  //
  auto pinned_page = this->const_get(page_id, required_layout);

  // If successful and the caller has asked us to do so, pin the page to the job.
  //
  if (pinned_page.ok() && bool_from(pin_page_to_job, /*default_value=*/true)) {
    this->pin(batt::make_copy(*pinned_page));
  }

  return pinned_page;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<PinnedPage> PageCacheJob::get_already_pinned(PageId page_id) const
{
  auto iter = this->pinned_.find(page_id);
  if (iter != this->pinned_.end()) {
    return iter->second;
  }
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCacheJob::const_get(PageId page_id,
                                             const Optional<PageLayoutId>& required_layout) const
{
  StatusOr<PinnedPage> pinned_page = this->base_job_.finalized_get(page_id, required_layout);
  if (pinned_page.status() != batt::StatusCode::kUnavailable) {
    return pinned_page;
  }
  return this->cache_->get(page_id, required_layout);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::prefetch_hint(PageId page_id)
{
  this->const_prefetch_hint(page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::const_prefetch_hint(PageId page_id) const
{
  if (!this->pinned_.count(page_id) && !this->new_pages_.count(page_id) &&
      !this->deleted_pages_.count(page_id)) {
    BATT_DEBUG_INFO(BATT_INSPECT(page_id) << std::dec << BATT_INSPECT(this->job_id));
    this->base_job_.finalized_prefetch_hint(page_id, this->cache());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCacheJob::recover_page(PageId page_id, const boost::uuids::uuid& caller_uuid,
                                  slot_offset_type caller_slot)
{
  const PageArena& arena = this->cache_->arena_for_page_id(page_id);
  PageAllocator& page_allocator = arena.allocator();

  Status allocator_recovered = page_allocator.recover_page(page_id);
  BATT_REQUIRE_OK(allocator_recovered);

  StatusOr<PinnedPage> pinned_page =
      this->get(page_id, /*required_layout=*/None, PinPageToJob::kTrue);

  if (!pinned_page.ok()) {
    page_allocator.deallocate_page(page_id);
    return pinned_page.status();
  }

  const PackedPageHeader& page_header = get_page_header(*pinned_page->get_page_buffer());
  if (page_header.user_slot.user_id != caller_uuid ||
      page_header.user_slot.slot_offset != caller_slot) {
    return StatusCode::kRecoverFailedPageReallocated;
  }

  // TODO [tastolfi 2022-01-03] FIX nullptr below!!!
  const auto& [iter, inserted] = this->new_pages_.emplace(page_id, NewPage{/*buffer=*/nullptr});
  if (inserted) {
    iter->second.set_view(pinned_page->get_shared_view());
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCacheJob::delete_page(PageId page_id)
{
  if (!page_id) {
    return OkStatus();
  }
  StatusOr<PinnedPage> page_view = this->get(page_id);
  if (page_view.ok()) {
    this->pruned_ = false;
    this->deleted_pages_.emplace(page_id, *page_view);
    this->root_set_delta_[page_id] = kRefCount_1_to_0;
    return OkStatus();
  }
  if (page_view.status() == batt::StatusCode::kNotFound) {
    return OkStatus();
  }
  return page_view.status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::new_root(PageId page_id)
{
  this->update_root_set(PageRefCount{
      .page_id = page_id.int_value(),
      .ref_count = +1,
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::delete_root(PageId page_id)
{
  this->update_root_set(PageRefCount{
      .page_id = page_id.int_value(),
      .ref_count = -1,
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheJob::update_root_set(const PageRefCount& prc)
{
  PageId page_id{prc.page_id};
  if (page_id.is_valid()) {
    this->pruned_ = false;
    i32& delta = this->root_set_delta_[page_id];
    delta += prc.ref_count;
    if (delta == 0) {
      this->root_set_delta_.erase(page_id);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> PageCacheJob::prune(u64 callers)
{
  if (this->pruned_) {
    return 0;
  }

  // Initially all new pages are in the `to_prune` set.
  //
  std::unordered_set<PageId, PageId::Hash> to_prune;
  for (const auto& p : this->new_pages_) {
    if (p.first) {
      to_prune.emplace(p.first);
    }
  }

  // Remove all pages from the root set; these won't be traced below since they don't appear
  // _within_ any other new pages.
  //
  for (const auto& p : this->root_set_delta_) {
    if (p.second > 0) {
      to_prune.erase(p.first);
    }
  }

  // Now trace references recursively
  //
  Status trace_status = this->trace_new_roots(/*page_loader=*/*this, /*page_id_fn=*/
                                              [&to_prune](PageId page_id) {
                                                to_prune.erase(page_id);
                                              });
  BATT_REQUIRE_OK(trace_status);

  const usize pruned_count = to_prune.size();

  // Prune all the new pages that weren't reachable by tracing from the root set.
  //
  for (PageId id : to_prune) {
    this->new_pages_.erase(id);
    this->pinned_.erase(id);
    this->deferred_new_pages_.erase(id);
    this->cache_->deallocate_page(id, callers | Caller::PageCacheJob_prune, this->job_id);
  }

  BATT_CHECK(this->deferred_new_pages_.empty());

  this->pruned_ = true;

  return pruned_count;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageCacheJob::NewPage
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheJob::NewPage::NewPage(std::shared_ptr<PageBuffer>&& buffer) noexcept
    : buffer_{std::move(buffer)}
    , view_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheJob::NewPage::set_view(std::shared_ptr<const PageView>&& v)
{
  if (this->view_) {
    return false;
  }
  this->view_.emplace(std::move(v));
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheJob::NewPage::has_view() const
{
  return this->view_ && (*this->view_ != nullptr);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::shared_ptr<const PageView> PageCacheJob::NewPage::view() const
{
  return this->view_.value_or(nullptr);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::shared_ptr<PageBuffer> PageCacheJob::NewPage::buffer() const
{
  return this->buffer_;
}

}  // namespace llfs
