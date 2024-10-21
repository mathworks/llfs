//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator_state.hpp>
//

#include <llfs/logging.hpp>

namespace llfs {

using Metrics = PageAllocatorMetrics;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

PageAllocatorState::PageAllocatorState(const PageIdFactory& page_ids, u64 max_attachments) noexcept
    : PageAllocatorStateNoLock{page_ids}
    , attachment_by_index_(max_attachments)
{
  for (PageAllocatorRefCount& ref_count_obj : this->page_ref_counts()) {
    BATT_CHECK_EQ(ref_count_obj.get_count(), 0);
    this->free_pool_.push_back(ref_count_obj);
  }
  this->free_pool_size_.set_value(this->free_pool_.size());

  for (u32 i = 0; i < max_attachments; ++i) {
    this->free_attach_nums_.emplace(i);
  }

  BATT_CHECK_LT(this->attachment_by_index_.size(), kInvalidUserIndex);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::~PageAllocatorState() noexcept
{
  LLFS_VLOG(1) << "~PageAllocatorState() active_objects=" << this->lru_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> PageAllocatorState::write_checkpoint_slice(
    TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer, batt::Grant& slice_grant)
{
  const usize n_active = this->lru_.size();
  usize n_refreshed = 0;

  while (!this->lru_.empty() && n_refreshed < n_active) {
    PageAllocatorLRUBase* oldest_object = &this->lru_.front();

    batt::StatusOr<SlotRange> slot_range;

    if (this->is_ref_count(oldest_object)) {
      PageAllocatorRefCount* const ref_count_obj =
          static_cast<PageAllocatorRefCount*>(oldest_object);

      const page_id_int physical_page = this->index_of(ref_count_obj);
      const page_generation_int generation = ref_count_obj->get_generation();
      const u32 user_index = ref_count_obj->get_last_modified_by();

      slot_range = slot_writer.append(
          slice_grant,
          PackedPageRefCountRefresh{
              .page_id =
                  {
                      .id_val = this->page_ids_.make_page_id(physical_page, generation).int_value(),
                  },
              .ref_count = ref_count_obj->get_count(),
              .user_index = user_index,
          });

    } else {
      PageAllocatorAttachment* const attachment =
          static_cast<PageAllocatorAttachment*>(oldest_object);

      slot_range =
          slot_writer.append(slice_grant, PackedPageAllocatorAttach{
                                              .user_slot =
                                                  PackedPageUserSlot{
                                                      .user_id = attachment->get_user_id(),
                                                      .slot_offset = attachment->get_user_slot(),
                                                  },
                                              .user_index = attachment->get_user_index(),
                                          });
    }

    if (!slot_range.ok() &&
        slot_range.status() == ::llfs::make_status(StatusCode::kSlotGrantTooSmall)) {
      break;
    }
    BATT_REQUIRE_OK(slot_range);

    n_refreshed += 1;

    // Do this after the refresh so we don't think an object has been updated when there is no
    // record of the update in the log.
    //
    this->set_last_update(oldest_object, *slot_range);
  }

  const slot_offset_type new_trim_pos = [&] {
    if (this->lru_.empty()) {
      return this->learned_upper_bound_.get_value();
    }
    return this->lru_.front().last_update();
  }();

  LLFS_VLOG(1) << "wrote checkpoint slice (new_trim_pos=" << new_trim_pos << ")";

  return new_trim_pos;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<PageAllocatorRefCount*> PageAllocatorState::page_ref_counts()
{
  return boost::make_iterator_range(&this->page_ref_counts_[0],  //
                                    &this->page_ref_counts_[this->page_device_capacity()]);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<const PageAllocatorRefCount*> PageAllocatorState::page_ref_counts() const
{
  return boost::make_iterator_range(&this->page_ref_counts_[0],  //
                                    &this->page_ref_counts_[this->page_device_capacity()]);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<PageId> PageAllocatorState::allocate_page()
{
  if (this->free_pool_.empty()) {
    return None;
  }
  PageAllocatorRefCount& ref_count_obj = [&]() -> decltype(auto) {
    if (kPageAllocPolicy == kFirstInFirstOut) {
      PageAllocatorRefCount& ref_count_obj = this->free_pool_.front();
      this->free_pool_.pop_front();
      return ref_count_obj;
    } else if (kPageAllocPolicy == kFirstInLastOut) {
      PageAllocatorRefCount& ref_count_obj = this->free_pool_.back();
      this->free_pool_.pop_back();
      return ref_count_obj;
    } else {
      BATT_PANIC() << "undefined kPageAllocPolicy";
      BATT_UNREACHABLE();
    }
  }();
  this->free_pool_size_.fetch_sub(1);

  const isize physical_page = this->index_of(&ref_count_obj);
  const page_generation_int generation = ref_count_obj.advance_generation();
  const PageId page_id = this->page_ids_.make_page_id(physical_page, generation);

  BATT_CHECK_EQ(ref_count_obj.get_count(), 0)
      << BATT_INSPECT(physical_page) << BATT_INSPECT(generation) << BATT_INSPECT(page_id);

  BATT_CHECK_EQ(physical_page, this->page_ids_.get_physical_page(page_id))
      << std::hex << BATT_INSPECT(page_id)
      << BATT_INSPECT(this->page_ids_.get_physical_page(page_id)) << BATT_INSPECT(physical_page)
      << BATT_INSPECT(generation) << BATT_INSPECT(this->page_device_capacity());

  BATT_CHECK_EQ(generation, this->page_ids_.get_generation(page_id));

  return page_id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::deallocate_page(PageId page_id)
{
  const page_id_int physical_page = this->page_ids_.get_physical_page(page_id);

  BATT_CHECK_LT(physical_page, this->page_device_capacity());

  PageAllocatorRefCount& ref_count_obj = this->page_ref_counts_[physical_page];

  BATT_CHECK_EQ(ref_count_obj.get_count(), 0);
  BATT_CHECK_GT(ref_count_obj.get_generation(), 0);
  BATT_CHECK(!ref_count_obj.PageAllocatorFreePoolHook::is_linked());

  // It should be safe to revert the generation count increment we did when allocating this page
  // because no one is allowed to reference a page once it is deallocated, so the invariant that
  // PageId and durable page data are 1-to-1 is maintained.  This also allows us to make some
  // helpful assumptions about what must be true when generation is >0, i.e., we can assume that the
  // page header has been written at least once, so during recovery it is safe to try to read the
  // pages in a half-committed Volume transaction instead of automatically invaliding the
  // transaction, forcing the application layer to retry.
  //
  // IMPORTANT: the implementation of `recover_page` and the initialization algorithms for certain
  // PageDevice types depend on this line, and vice-versa!  Consider the "big-picture" implications
  // before changing!!
  //
  ref_count_obj.revert_generation();

  this->free_pool_.push_back(ref_count_obj);
  this->free_pool_size_.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::ProposalStatus PageAllocatorState::propose_exactly_once(
    const PackedPageUserSlot& user_slot, AllowAttach attach) const
{
  auto iter = this->attachments_.find(user_slot.user_id);
  if (iter == this->attachments_.end()) {
    LLFS_VLOG(1) << "[propose_exactly_once] did not find attachment for user";

    if (attach == AllowAttach::kTrue) {
      return ProposalStatus::kValid;
    }
    return ProposalStatus::kInvalid_NotAttached;
  }

  LLFS_VLOG(1) << "[propose_exactly_once] last seen user slot=" << iter->second->get_user_slot()
               << "; event user slot=" << user_slot.slot_offset << ", uuid=" << user_slot.user_id;

  if (slot_less_than(iter->second->get_user_slot(), user_slot.slot_offset)) {
    return ProposalStatus::kValid;
  }

  LLFS_VLOG(1) << "skipping slot; no change (learned=" << iter->second->get_user_slot()
               << ", proposed=" << user_slot.slot_offset << ")";

  return ProposalStatus::kNoChange;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::update_learned_upper_bound(slot_offset_type offset)
{
  LLFS_VLOG(1) << "updating learned upper_bound: " << this->learned_upper_bound_.get_value()
               << " -> " << offset;

  clamp_min_slot(this->learned_upper_bound_, offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::ProposalStatus PageAllocatorState::propose(PackedPageAllocatorAttach* attach)
{
  BATT_CHECK_EQ(attach->user_index, PageAllocatorState::kInvalidUserIndex);

  ProposalStatus status = this->propose_exactly_once(
      const_cast<const PackedPageAllocatorAttach*>(attach)->user_slot, AllowAttach::kTrue);

  if (status == ProposalStatus::kValid) {
    StatusOr<u32> user_index = this->allocate_attachment(attach->user_slot.user_id);
    if (!user_index.ok()) {
      return ProposalStatus::kInvalid_OutOfAttachments;
    }
    attach->user_index = *user_index;

  } else if (status == ProposalStatus::kNoChange) {
    Optional<u32> user_index = this->get_attachment_num(attach->user_slot.user_id);
    if (user_index) {
      attach->user_index = *user_index;
    }
  }

  return status;
}

//----- --- -- -  -  -   -

void PageAllocatorState::learn(const SlotRange& slot_offset,
                               const PackedPageAllocatorAttach& attach, Metrics&)
{
  LLFS_VLOG(1) << "learning " << attach;

  const bool success =
      this->update_attachment(slot_offset, attach.user_slot, attach.user_index, AllowAttach::kTrue);

  BATT_CHECK(success);

  this->update_learned_upper_bound(slot_offset.upper_bound);
}

//----- --- -- -  -  -   -

bool PageAllocatorState::update_attachment(const SlotRange& slot_offset,
                                           const PackedPageUserSlot& user_slot, u32 user_index,
                                           AllowAttach attach)
{
  PageAllocatorAttachment* p_attachment = nullptr;

  auto iter = this->attachments_.find(user_slot.user_id);
  if (iter != this->attachments_.end()) {
    p_attachment = iter->second.get();
    p_attachment->clamp_min_user_slot(user_slot.slot_offset);

  } else {
    if (attach != AllowAttach::kTrue) {
      return false;
    }
    BATT_CHECK_NE(user_index, PageAllocatorState::kInvalidUserIndex);

    auto attachment = std::make_unique<PageAllocatorAttachment>(user_slot.user_id,
                                                                user_slot.slot_offset, user_index);

    p_attachment = attachment.get();
    this->attachments_.emplace(user_slot.user_id, std::move(attachment));
  }

  if (p_attachment) {
    this->set_last_update(p_attachment, slot_offset);
  }

  return true;
}

//----- --- -- -  -  -   -

Status PageAllocatorState::recover(const SlotRange& slot_offset,
                                   const PackedPageAllocatorAttach& attach)
{
  LLFS_VLOG(1) << "recovering[slot=" << slot_offset << "] " << attach;

  BATT_CHECK_LT(attach.user_index, this->attachment_by_index_.size());
  if (this->attachment_by_index_[attach.user_index] == batt::None) {
    BATT_CHECK_EQ(this->free_attach_nums_.count(attach.user_index), 1);
    this->attachment_by_index_[attach.user_index] = attach.user_slot.user_id;
    this->free_attach_nums_.erase(attach.user_index);
  } else {
    BATT_CHECK_EQ(this->free_attach_nums_.count(attach.user_index), 0);
    BATT_CHECK_EQ(*this->attachment_by_index_[attach.user_index], attach.user_slot.user_id);
  }

  const bool success =
      this->update_attachment(slot_offset, attach.user_slot, attach.user_index, AllowAttach::kTrue);

  BATT_CHECK(success);

  this->update_learned_upper_bound(slot_offset.upper_bound);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::ProposalStatus PageAllocatorState::propose(PackedPageAllocatorDetach* detach)
{
  return this->propose_exactly_once(const_cast<const PackedPageAllocatorDetach*>(detach)->user_slot,
                                    AllowAttach::kFalse);
}

//----- --- -- -  -  -   -

void PageAllocatorState::learn(const SlotRange& slot_offset,
                               const PackedPageAllocatorDetach& detach, Metrics&)
{
  LLFS_VLOG(1) << "learning " << detach;

  this->remove_attachment(detach.user_slot.user_id);

  this->update_learned_upper_bound(slot_offset.upper_bound);
}

//----- --- -- -  -  -   -

void PageAllocatorState::remove_attachment(const boost::uuids::uuid& user_id)
{
  auto iter = this->attachments_.find(user_id);
  BATT_CHECK_NE(iter, this->attachments_.end());

  PageAllocatorAttachment& attachment = *iter->second;

  this->deallocate_attachment(attachment.get_user_index(), user_id);
  this->lru_.erase(this->lru_.iterator_to(attachment));
  this->attachments_.erase(iter);
}

//----- --- -- -  -  -   -

Status PageAllocatorState::recover(const SlotRange& slot_offset,
                                   const PackedPageAllocatorDetach& detach)
{
  LLFS_VLOG(1) << "recovering[slot=" << slot_offset << "] " << detach;

  auto iter = this->attachments_.find(detach.user_slot.user_id);
  if (iter != this->attachments_.end()) {
    const PageAllocatorAttachment& attachment = *iter->second;

    if (!slot_greater_than(detach.user_slot.slot_offset, attachment.get_user_slot())) {
      LLFS_VLOG(1) << " -- attachment slot (" << attachment.get_user_slot()
                   << ") is newer than detach event (" << detach.user_slot.slot_offset
                   << "); ignoring";
    } else {
      this->lru_.erase(this->lru_.iterator_to(*iter->second));
      this->attachments_.erase(iter);
    }
  }

  this->update_learned_upper_bound(slot_offset.upper_bound);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::recover(const SlotRange& slot_offset,
                                   const PackedPageRefCountRefresh& packed)
{
  LLFS_VLOG(1) << "recovering[slot=" << slot_offset << "] "
               << "PackedPageRefCountRefresh{.user_index=" << packed.user_index << ",}";

  const PageId page_id = packed.page_id.unpack();
  const page_id_int physical_page = this->page_ids_.get_physical_page(page_id);
  const page_id_int generation = this->page_ids_.get_generation(page_id);

  PageAllocatorRefCount* obj = &this->page_ref_counts_[physical_page];

  BATT_CHECK_GE(packed.ref_count, 0)
      << "PageRef checkpoint slices should never store negative values!";

  obj->set_last_modified_by(packed.user_index);

  const i32 old_count = obj->set_count(packed.ref_count);
  const page_generation_int old_generation = obj->set_generation(generation);

  this->update_free_pool_status(obj);

  LLFS_VLOG(1) << "rR[" << slot_offset.lower_bound << "] -- page_id: " << page_id  //
               << ", ref_count: " << old_count << "->" << packed.ref_count         //
               << ", generation: " << old_generation << "->" << generation;

  this->set_last_update(obj, slot_offset);

  this->update_learned_upper_bound(slot_offset.upper_bound);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::ProposalStatus PageAllocatorState::propose(PackedPageAllocatorTxn* txn)
{
  // The txn will include the user_index to track which user was the last to update a given page ref
  // count, for deterministic detection of "dead" pages.
  //
  Optional<u32> user_index = this->get_attachment_num(txn->user_slot.user_id);
  if (!user_index) {
    return ProposalStatus::kInvalid_NotAttached;
  }
  BATT_CHECK_EQ(txn->user_index, PageAllocatorState::kInvalidUserIndex);
  txn->user_index = *user_index;

  const ProposalStatus status = this->propose_exactly_once(
      const_cast<const PackedPageAllocatorTxn*>(txn)->user_slot, AllowAttach::kFalse);

  // If this is a valid proposal that will cause state change, go through and change the deltas to
  // the new ref count values.
  //
  if (status == ProposalStatus::kValid) {
    for (PackedPageRefCount& prc : txn->ref_counts) {
      prc.ref_count = this->calculate_new_ref_count(prc);
    }
  }

  return status;
}

//----- --- -- -  -  -   -

void PageAllocatorState::learn(const SlotRange& slot_offset, const PackedPageAllocatorTxn& txn,
                               Metrics& metrics)
{
  this->process_txn(slot_offset, txn, &metrics, /*inside_recovery=*/std::false_type{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status PageAllocatorState::recover(const SlotRange& slot_offset,
                                         const PackedPageAllocatorTxn& txn)
{
  this->process_txn(slot_offset, txn, /*metrics=*/nullptr, /*inside_recovery=*/std::true_type{});

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kInsideRecovery>
void PageAllocatorState::process_txn(
    const SlotRange& slot_offset, const PackedPageAllocatorTxn& txn, PageAllocatorMetrics* metrics,
    std::integral_constant<bool, kInsideRecovery> /*inside_recovery*/)
{
  LLFS_VLOG(1) << "(device=" << this->page_ids_.get_device_id()  //
               << ") " << (kInsideRecovery ? "recovering" : "learning") << "[slot=" << slot_offset
               << "] " << txn;

  if (!kInsideRecovery) {
    BATT_CHECK_NOT_NULLPTR(metrics);
  }

  // Update the client attachment for this transaction so we don't double-commit.
  //
  {
    const bool success =
        this->update_attachment(slot_offset, txn.user_slot, txn.user_index,
                                kInsideRecovery ? AllowAttach::kTrue : AllowAttach::kFalse);

    BATT_CHECK(success);
  }

  const PageIdFactory& ids = this->page_ids_;

  // Apply all ref count updates in the txn.
  //
  for (const PackedPageRefCount& prc : txn.ref_counts) {
    const PageId page_id = prc.page_id.unpack();
    const page_id_int physical_page = ids.get_physical_page(page_id);
    const page_id_int new_generation = ids.get_generation(page_id);

    BATT_CHECK_LT(physical_page, this->page_device_capacity());
    PageAllocatorRefCount* const obj = &this->page_ref_counts_[physical_page];

    // IMPORTANT: `set_last_modified_by` must come first!  This is required because we detect dead
    // pages in a lock-free manner, and we don't want to read the updated ref count (which might be
    // 1) along with an *older* user_index value, because this will mistakenly credit a different
    // client with producing the dead page.
    //
    obj->set_last_modified_by(txn.user_index);

    const i32 old_count = obj->set_count(prc.ref_count);
    const u64 old_generation = obj->set_generation(new_generation);

    if (!kInsideRecovery) {
      // Update metrics for alloc/free.
      //
      if (old_count == 0 && prc.ref_count >= 2 && old_generation < new_generation) {
        metrics->pages_allocated.fetch_add(1);
      } else if (prc.ref_count == 0 && old_count > 0) {
        metrics->pages_freed.fetch_add(1);
      }
    }

    this->set_last_update(obj, slot_offset);
    this->update_free_pool_status(obj);
  }

  this->update_learned_upper_bound(slot_offset.upper_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
namespace {

void run_ref_count_update_sanity_checks(const PageIdFactory& id_factory,
                                        const PackedPageRefCount& delta,
                                        const PageAllocatorRefCount& obj, i32 old_count,
                                        i32 new_count)
{
  const auto debug_info = [&](std::ostream& out) {
    const page_generation_int delta_generation = id_factory.get_generation(delta.page_id.unpack());

    out << "(" << BATT_INSPECT(delta) << BATT_INSPECT(obj) << BATT_INSPECT(old_count)
        << BATT_INSPECT(new_count) << ")" << BATT_INSPECT(delta_generation);
  };

  LLFS_VLOG(2) << debug_info;

  BATT_CHECK_GE(old_count, 0) << "ref counts must be non-negative" << debug_info;
  BATT_CHECK_GE(new_count, 0) << "ref counts must be non-negative" << debug_info;

  if (old_count == new_count) {
    BATT_CHECK_EQ(delta.ref_count, 0)
        << "delta was non-zero but count did not change" << debug_info;
    return;
  }

  if (delta.ref_count == 0) {
    BATT_CHECK_EQ(old_count, new_count)
        << "zero delta must leave ref count unchanged" << debug_info;
    return;
  }
  //
  // From here on we know that old_count != new_count and that delta != 0...

  if (new_count == 0) {
    BATT_CHECK_EQ(old_count, 1) << "ref count can only change to 0 if old count was 1"
                                << debug_info;

    BATT_CHECK_EQ(delta.ref_count, kRefCount_1_to_0)
        << "ref count can only change to 0 via kRefCount_1_to_0" << debug_info;

    return;
  }

  if (old_count == 1) {
    BATT_CHECK_EQ(new_count, 0) << "ref count of 1 can only change to 0" << debug_info;

    BATT_CHECK_EQ(delta.ref_count, kRefCount_1_to_0)
        << "ref count of 1 can only change via kRefCount_1_to_0" << debug_info;

    return;
  }

  if (delta.ref_count == kRefCount_1_to_0) {
    BATT_CHECK_EQ(old_count, 1) << "delta of kRefCount_1_to_0 requires old count of 1"
                                << debug_info;

    BATT_CHECK_EQ(new_count, 0) << "delta of kRefCount_1_to_0 requires new count of 0"
                                << debug_info;

    return;
  }
  //
  // From here on, we know we have an arithmetic delta (i.e., not kRefCount_1_to_0)...

  BATT_CHECK_EQ(old_count + delta.ref_count, new_count)
      << "ref count was not updated properly" << debug_info;

  if (delta.ref_count < 0) {
    BATT_CHECK_LT(new_count, old_count)
        << "negative delta did not decrease the ref count" << debug_info;

    BATT_CHECK_GT(new_count, 0) << "ref count can not become 0 via negative delta" << debug_info;

  } else if (delta.ref_count > 0) {
    BATT_CHECK_GT(new_count, old_count)
        << "positive delta did not increase the ref count" << debug_info;

    if (old_count == 0) {
      BATT_CHECK_GE(new_count, 2)
          << "initial ref count increase for page generation must be at least 2" << debug_info;
    }
  }
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 PageAllocatorState::calculate_new_ref_count(const PackedPageRefCount& delta) const
{
  const PageId page_id = delta.page_id.unpack();
  const page_id_int physical_page = this->page_ids_.get_physical_page(page_id);
  const PageAllocatorRefCount& obj = this->page_ref_counts_[physical_page];

  const page_generation_int new_generation = this->page_ids_.get_generation(page_id);
  const page_generation_int old_generation = obj.get_generation();

  BATT_CHECK_GE(new_generation, old_generation) << "page generation went backwards!";

  const i32 old_count = obj.get_count();
  i32 new_count = -1;  // set below...

  // Special case for 1 -> 0.
  //
  if (delta.ref_count == kRefCount_1_to_0) {
    BATT_CHECK_EQ(obj.get_generation(), new_generation);
    new_count = 0;
  } else {
    new_count = old_count + delta.ref_count;
  }

  run_ref_count_update_sanity_checks(this->page_ids_, delta, obj, old_count, new_count);

  return new_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::set_last_update(PageAllocatorLRUBase* obj, const SlotRange& slot_offset)
{
  if (obj->PageAllocatorLRUHook::is_linked()) {
    this->lru_.erase(this->lru_.iterator_to(*obj));
  }

  BATT_CHECK(!slot_less_than(slot_offset.lower_bound, obj->last_update()))
      << BATT_INSPECT(slot_offset) << BATT_INSPECT(obj->last_update());

  obj->set_last_update(slot_offset.lower_bound);
  this->lru_.push_back(*obj);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::is_ref_count(const PageAllocatorLRUBase* obj) const
{
  return &this->page_ref_counts_[0] <= obj &&
         obj < &this->page_ref_counts_[this->page_device_capacity()];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<PageAllocatorAttachmentStatus> PageAllocatorState::get_all_clients_attachment_status()
    const
{
  return as_seq(this->attachments_.begin(), this->attachments_.end())  //
         | seq::map([](const auto& kv_pair) {
             return PageAllocatorAttachmentStatus::from(kv_pair);
           })  //
         | seq::collect_vec();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<PageAllocatorAttachmentStatus> PageAllocatorState::get_client_attachment_status(
    const boost::uuids::uuid& uuid) const
{
  auto iter = this->attachments_.find(uuid);
  if (iter == this->attachments_.end()) {
    return None;
  }
  return PageAllocatorAttachmentStatus::from(*iter);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::update_free_pool_status(PageAllocatorRefCount* obj)
{
  if (obj->get_count() == 0) {
    if (!obj->PageAllocatorFreePoolHook::is_linked()) {
      this->free_pool_.push_back(*obj);
      this->free_pool_size_.fetch_add(1);
    }

  } else if (obj->PageAllocatorFreePoolHook::is_linked()) {
    this->free_pool_.erase(this->free_pool_.iterator_to(*obj));
    this->free_pool_size_.fetch_sub(1);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::check_post_recovery_invariants() const
{
  for (const PageAllocatorRefCount& ref_count_obj : this->page_ref_counts()) {
    // Negative ref counts are invalid.
    //
    BATT_CHECK_GE(ref_count_obj.get_count(), 0);

    // If this object has been updated, it should be on the LRU list.
    //
    if (ref_count_obj.last_update() != 0) {
      BATT_CHECK(ref_count_obj.PageAllocatorLRUHook::is_linked());
    }

    // Pages in the free pool must have 0 ref counts and vice versa.
    //
    BATT_CHECK_EQ((ref_count_obj.get_count() == 0),
                  ref_count_obj.PageAllocatorFreePoolHook::is_linked());
  }

  // Make sure the LRU list's `last_update()` fields are non-decreasing.
  //
  slot_offset_type last_slot = 0;
  for (const PageAllocatorLRUBase& obj : this->lru_) {
    BATT_CHECK(!slot_less_than(obj.last_update(), last_slot))
        << BATT_INSPECT(obj.last_update()) << BATT_INSPECT(last_slot) << [&](std::ostream& out) {
             if (this->is_ref_count(&obj)) {
               out << "obj=RefCount";
             } else {
               out << "obj=Attachment";
             }
           };
    last_slot = obj.last_update();
  }

  LLFS_VLOG(1) << "Recovery complete!" << BATT_INSPECT(this->lru_.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<u32> PageAllocatorState::allocate_attachment(const boost::uuids::uuid& uuid) noexcept
{
  // If the given uuid is already attached, just return the existing user_index.
  {
    auto iter = this->attachments_.find(uuid);
    if (iter != this->attachments_.end()) {
      return iter->second->get_user_index();
    }
  }

  // If there are no more attachments available, fail.
  //
  if (this->free_attach_nums_.empty()) {
    return ::llfs::make_status(StatusCode::kOutOfAttachments);
  }

  // Grab an arbitrary attachment number (we don't know what we'll get because `free_attach_nums_`
  // is an unordered_set).
  //
  auto iter = this->free_attach_nums_.begin();
  u32 n = *iter;
  this->free_attach_nums_.erase(iter);

  // Sanity checks: `n` must be under the max attachment limit, and must not be mapped to some other
  // uuid.
  //
  BATT_CHECK_LT(n, this->attachment_by_index_.size());
  BATT_CHECK_EQ(this->attachment_by_index_[n], batt::None);

  // Record the mapping from user_index to uuid.
  //
  this->attachment_by_index_[n] = uuid;

  return n;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::deallocate_attachment(
    u32 user_index, const Optional<boost::uuids::uuid>& expected_uuid) noexcept
{
  BATT_CHECK_LT(user_index, this->attachment_by_index_.size());

  // It's ok if we deallocate repeatedly, but make sure at least we don't deallocate someone else's
  // attachment by mistake!
  //
  if (expected_uuid && this->attachment_by_index_[user_index]) {
    BATT_CHECK_EQ(this->attachment_by_index_[user_index], *expected_uuid);
  }

  this->attachment_by_index_[user_index] = batt::None;
  this->free_attach_nums_.emplace(user_index);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<u32> PageAllocatorState::get_attachment_num(const boost::uuids::uuid& uuid) noexcept
{
  auto iter = this->attachments_.find(uuid);
  if (iter == this->attachments_.end()) {
    return None;
  }

  return iter->second->get_user_index();
}

}  // namespace llfs
