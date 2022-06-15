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
                           const PageRecyclerOptions& options, usize wal_capacity,
                           const SlotRange& initial_wal_range)
    : NoLockState{uuid, latest_info_refresh_slot, options}
    , arena_used_{0}
    , arena_size_{max_pages_for_wal_capacity(options, wal_capacity)}
    , arena_{new WorkItem[this->arena_size_]}
    , pending_{}
    , stack_{}
    , free_pool_{}
    , lru_{}
{
  BATT_CHECK_EQ(2, options.refresh_factor) << "implement generic refresh factors";
}

void PageRecycler::State::bulk_load(Slice<const PageToRecycle> pages)
{
  Optional<slot_offset_type> prev_slot;

  usize pending_count_delta = 0;
  for (const PageToRecycle& to_recycle : pages) {
    if (prev_slot) {
      BATT_CHECK_LE(*prev_slot, to_recycle.slot_offset)
          << "arg `pages` must be sorted by `PageToRecycle::slot_offset`";
    }

    const auto& [iter, inserted] = this->pending_.emplace(to_recycle.page_id);
    if (inserted == true) {
      ++pending_count_delta;
      (void)this->new_work_item(to_recycle);
    }

    prev_slot = to_recycle.slot_offset;
  }

  this->pending_count.fetch_add(pending_count_delta);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::SmallVec<PageToRecycle, 2> PageRecycler::State::insert(const PageToRecycle& p)
{
  // If the page is already pending, do nothing.
  //
  const auto result = this->pending_.emplace(p.page_id);
  if (result.second == false) {
    return {};
  }

  // Bump the pending counter to let the recycler task know it has some work to do.
  //
  const auto on_return = batt::finally([&] {
    this->pending_count.fetch_add(1);
  });

  WorkItem& latest = this->new_work_item(p);

  // Because C=2, we must refresh one slot for every slot we write (ampFactor=2).  Find the least
  // recently used record (if not the same as `latest`) and refresh that one.
  //
  WorkItem* oldest = this->refresh_oldest_work_item();
  if (oldest) {
    return {{latest.to_recycle, oldest->to_recycle}};
  }
  return {{latest.to_recycle}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageToRecycle PageRecycler::State::remove()
{
  // Enforce depth-first discipline: search for the highest non-empty stack level.
  //
  usize active_depth = this->stack_.size() - 1;
  for (; active_depth > 0 && this->stack_[active_depth].empty(); active_depth -= 1) {
    continue;
  }

  if (this->stack_[active_depth].empty()) {
    return PageToRecycle::make_invalid();
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
// TODO [tastolfi 2021-06-21] factor out duplicate code between this and remove() above.
//
Optional<PageToRecycle> PageRecycler::State::try_remove(u32 required_depth)
{
  // Enforce depth-first discipline: search for the highest non-empty stack level.
  //
  usize active_depth = this->stack_.size() - 1;
  for (; active_depth > 0 && this->stack_[active_depth].empty(); active_depth -= 1) {
    continue;
  }

  if (active_depth != required_depth) {
    return None;
  }

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
    this->free_pool_.push_back(this->arena_[this->arena_used_]);
    this->arena_used_ += 1;
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
  item.to_recycle = p;

  // The new item needs to go onto the stack and the LRU list.
  //
  BATT_CHECK_LT(p.depth, this->stack_.size());
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
  return this->lru_.front().to_recycle.slot_offset;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageRecycler::WorkItem* PageRecycler::State::refresh_oldest_work_item()
{
  if (this->lru_.size() == 1) {
    return nullptr;
  }

  // Move to the back of the LRU list and set the slot offset to the current slot.
  //
  WorkItem& oldest = this->lru_.front();
  const WorkItem& newest = this->lru_.back();
  this->lru_.pop_front();
  this->lru_.push_back(oldest);
  oldest.to_recycle.slot_offset = newest.to_recycle.slot_offset;

  return &oldest;
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
      Optional<PageToRecycle> next = this->try_remove(/*required_depth=*/to_recycle.back().depth);
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
