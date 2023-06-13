//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_recycler.hpp>
//

namespace llfs {

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
// class PageRecycler::State
//
namespace {
usize max_pages_for_wal_capacity(const PageRecyclerOptions& options, usize wal_capacity)
{
  return wal_capacity / options.total_page_grant_size();
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageRecycler::State::State(const boost::uuids::uuid& uuid,
                           slot_offset_type latest_info_refresh_slot,
                           const PageRecyclerOptions& options, usize wal_capacity)
    : NoLockState{uuid, latest_info_refresh_slot, options}
    , arena_used_{0}
    , arena_size_{max_pages_for_wal_capacity(options, wal_capacity)}
    , arena_{}
    , pending_{}
    , stack_{}
    , free_pool_{}
    , lru_{}
{
  BATT_CHECK_EQ(2, options.refresh_factor()) << "implement generic refresh factors";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageRecycler::State::bulk_load(Slice<const PageToRecycle> pages)
{
  Optional<slot_offset_type> prev_slot;

  usize pending_count_delta = 0;
  for (const PageToRecycle& to_recycle : pages) {
    BATT_CHECK(to_recycle.page_id.is_valid());
    BATT_CHECK(to_recycle.refresh_slot);
    if (prev_slot) {
      BATT_CHECK_LE(*prev_slot, *to_recycle.refresh_slot)
          << "arg `pages` must be sorted by `PageToRecycle::slot_offset`";
    }
    BATT_CHECK(!to_recycle.batch_slot) << "Pages with a batch slot are no longer pending!";

    const auto& [iter, inserted] = this->pending_.emplace(to_recycle.page_id);
    if (inserted == true) {
      ++pending_count_delta;
      (void)this->new_work_item(to_recycle);
    }

    prev_slot = to_recycle.refresh_slot;
  }

  this->pending_count.fetch_add(pending_count_delta);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<slot_offset_type> PageRecycler::State::insert_and_refresh(
    const PageToRecycle& p,
    std::function<batt::StatusOr<slot_offset_type>(const batt::SmallVecBase<PageToRecycle*>&)>&&
        append_to_log_fn)
{
  batt::SmallVec<PageToRecycle*, 2> to_append;

  // If the page is already pending, do nothing.
  //
  {
    const auto result = this->pending_.emplace(p.page_id);
    if (!p || result.second == false) {
      return append_to_log_fn(to_append);
    }
  }

  // Bump the pending counter to let the recycler task know it has some work to do.
  //
  auto notify_pending = batt::finally([&] {
    this->pending_count.fetch_add(1);
  });

  WorkItem& latest = this->new_work_item(p);
  BATT_CHECK(latest.PageListHook::is_linked());

  // Because C=2, we must refresh one slot for every slot we write (ampFactor=2).  Find the least
  // recently used record (if not the same as `latest`) and refresh that one.
  //
  auto* const oldest = [&]() -> WorkItem* {
    if (this->lru_.size() == 1) {
      return nullptr;
    }
    return &(this->lru_.front());
  }();

  // Save oldest->refresh_slot in case we need to revert.
  //
  Optional<slot_offset_type> oldest_refresh_slot;

  // Build the list of slots to write and pass it to the append fn.
  //
  to_append.push_back(&latest.to_recycle);
  if (oldest) {
    std::swap(oldest_refresh_slot, oldest->to_recycle.refresh_slot);
    to_append.push_back(&oldest->to_recycle);
  }

  batt::StatusOr<slot_offset_type> result = append_to_log_fn(to_append);

  // If log append succeeded, we update the LRU; else revert.
  //
  if (!result.ok()) {
    if (oldest) {
      oldest->to_recycle.refresh_slot = oldest_refresh_slot;
    }
    BATT_CHECK(latest.PageListHook::is_linked());
    this->stack_[p.depth].erase(this->stack_[p.depth].iterator_to(latest));
    this->delete_work_item(latest);
    notify_pending.cancel();
    return result;
  } else {
    // Make sure the append fn updated all the refresh slots.
    //
    for (const PageToRecycle* page : to_append) {
      BATT_CHECK(page->refresh_slot);
    }

    // `latest` and `oldest` should not have moved.
    //
    BATT_CHECK_EQ(&latest, &(this->lru_.back()));
    if (oldest) {
      BATT_CHECK_EQ(oldest, &(this->lru_.front()));

      // Move oldest to the front.
      //
      this->lru_.pop_front();
      this->lru_.push_back(*oldest);
    }
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageToRecycle PageRecycler::State::remove()
{
  // Enforce depth-first discipline: search for the highest non-empty stack level.
  //
  i32 active_depth = this->get_active_depth();

  return this->remove_at_depth(active_depth).or_else([] {
    return PageToRecycle::make_invalid();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<PageToRecycle> PageRecycler::State::try_remove(i32 required_depth)
{
  BATT_CHECK_GE(required_depth, 0);

  // Enforce depth-first discipline: search for the highest non-empty stack level.
  //
  i32 active_depth = this->get_active_depth();

  if (active_depth != required_depth) {
    return None;
  }

  return this->remove_at_depth(active_depth);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 PageRecycler::State::get_active_depth() const
{
  // Enforce depth-first discipline: search for the highest non-empty stack level.
  //
  i32 active_depth = BATT_CHECKED_CAST(i32, this->stack_.size()) - 1;
  for (; active_depth > 0 && this->stack_[active_depth].empty(); active_depth -= 1) {
    continue;
  }
  BATT_CHECK_GE(active_depth, 0);

  return active_depth;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<PageToRecycle> PageRecycler::State::remove_at_depth(i32 active_depth)
{
  if (this->stack_[active_depth].empty()) {
    return None;
  }

  WorkItem& item = this->stack_[active_depth].back();
  this->stack_[active_depth].pop_back();
  BATT_CHECK_EQ(item.to_recycle.depth, active_depth);

  const auto on_return = batt::finally([&] {
    this->pending_.erase(item.to_recycle.page_id);
    this->delete_work_item(item);
    this->pending_count.fetch_sub(1);
  });

  return item.to_recycle;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageRecycler::WorkItem& PageRecycler::State::new_work_item(const PageToRecycle& p)
{
  WorkItem& item = this->alloc_work_item();
  this->init_work_item(item, p);
  return item;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageRecycler::WorkItem& PageRecycler::State::alloc_work_item()
{
  // Check to see whether we need to expand the free pool (which is lazily initialized).
  //
  if (this->free_pool_.empty()) {
    BATT_CHECK_LT(this->arena_used_, this->arena_size_);

    this->arena_.emplace_back(std::make_unique<ArenaExtent>());
    auto& extent_items = this->arena_.back()->items;
    for (WorkItem& item : extent_items) {
      this->free_pool_.push_back(item);
    }
    this->arena_used_ += extent_items.size();
  }

  // Allocate from the free pool here.
  //
  BATT_CHECK(!this->free_pool_.empty());
  WorkItem& item = this->free_pool_.back();
  this->free_pool_.pop_back();

  BATT_CHECK(!item.PageListHook::is_linked());
  BATT_CHECK(!item.LRUHook::is_linked());

  return item;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageRecycler::State::init_work_item(WorkItem& item, const PageToRecycle& p)
{
  BATT_CHECK(p);

  item.to_recycle = p;

  // The new item needs to go onto the stack and the LRU list.
  //
  BATT_CHECK_LT(p.depth, BATT_CHECKED_CAST(i32, this->stack_.size()));
  BATT_CHECK(!item.PageListHook::is_linked());
  this->stack_[p.depth].push_back(item);

  BATT_CHECK(!item.LRUHook::is_linked());
  this->lru_.push_back(item);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageRecycler::State::delete_work_item(WorkItem& item)
{
  this->deinit_work_item(item);
  this->free_work_item(item);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageRecycler::State::deinit_work_item(WorkItem& item)
{
  BATT_CHECK(item.LRUHook::is_linked());
  this->lru_.erase(this->lru_.iterator_to(item));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageRecycler::State::free_work_item(WorkItem& item)
{
  BATT_CHECK(!item.PageListHook::is_linked());
  BATT_CHECK(!item.LRUHook::is_linked());
  this->free_pool_.push_back(item);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<slot_offset_type> PageRecycler::State::get_lru_slot() const
{
  if (this->lru_.empty()) {
    return None;
  }

  const Optional<slot_offset_type>& lru_slot = this->lru_.front().to_recycle.refresh_slot;
  BATT_CHECK(lru_slot) << BATT_INSPECT(this->lru_.front().to_recycle);

  return lru_slot;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<PageToRecycle> PageRecycler::State::collect_batch(usize max_page_count,
                                                              Metrics& metrics)
{
  std::vector<PageToRecycle> to_recycle;

  for (usize i = 0; i < max_page_count; ++i) {
    if (i == 0) {
      to_recycle.emplace_back(this->remove());
      BATT_CHECK_NE(to_recycle.back().page_id, PageId{kInvalidPageId});
    } else {
      Optional<PageToRecycle> next =
          this->try_remove(/*required_depth=*/BATT_CHECKED_CAST(i32, to_recycle.back().depth));
      if (!next) {
        break;
      }
      BATT_CHECK(!to_recycle.empty());
      BATT_CHECK_EQ(next->depth, to_recycle.front().depth);
      to_recycle.emplace_back(*next);
    }
    metrics.remove_count.fetch_add(1);
  }

  return to_recycle;
}

}  // namespace llfs
