//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator/page_allocator_state.hpp>
//

#include <llfs/logging.hpp>

namespace llfs {

using Metrics = PageAllocatorMetrics;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

PageAllocatorState::PageAllocatorState(const PageIdFactory& page_ids) noexcept
    : PageAllocatorStateNoLock{page_ids}
{
  for (PageAllocatorRefCount& ref_count_obj : this->page_ref_counts()) {
    BATT_CHECK_EQ(ref_count_obj.get_count(), 0);
    this->free_pool_.push_back(ref_count_obj);
  }
  this->free_pool_size_.set_value(this->free_pool_.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> PageAllocatorState::write_checkpoint_slice(
    TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer, batt::Grant& slice_grant)
{
  const usize n_active = this->lru_.size();
  usize n_refreshed = 0;

  while (!this->lru_.empty() && n_refreshed < n_active) {
    PageAllocatorObjectBase* oldest_object = &this->lru_.front();

    batt::StatusOr<SlotRange> slot_range;

    if (this->is_ref_count(oldest_object)) {
      PageAllocatorRefCount* const ref_count_obj =
          static_cast<PageAllocatorRefCount*>(oldest_object);

      const page_id_int physical_page = this->index_of(ref_count_obj);
      const page_generation_int generation = ref_count_obj->get_generation();

      slot_range = slot_writer.append(
          slice_grant,
          PackedPageRefCount{
              .page_id = this->page_ids_.make_page_id(physical_page, generation).int_value(),
              .ref_count = ref_count_obj->get_count(),
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

  if (this->lru_.empty()) {
    return this->learned_upper_bound_.get_value();
  }
  return this->lru_.front().last_update();
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
PageAllocatorState::ProposalStatus PageAllocatorState::propose(
    const PackedPageAllocatorAttach& attach)
{
  return this->propose_exactly_once(attach.user_slot, AllowAttach::kTrue);
}

//----- --- -- -  -  -   -

void PageAllocatorState::learn(const SlotRange& slot_offset,
                               const PackedPageAllocatorAttach& attach, Metrics&)
{
  LLFS_VLOG(1) << "learning " << attach;

  this->update_attachment(slot_offset, attach.user_slot, AllowAttach::kTrue);

  this->update_learned_upper_bound(slot_offset.upper_bound);
}

//----- --- -- -  -  -   -

void PageAllocatorState::update_attachment(const SlotRange& slot_offset,
                                           const PackedPageUserSlot& user_slot, AllowAttach attach)
{
  PageAllocatorAttachment* p_attachment = nullptr;

  auto iter = this->attachments_.find(user_slot.user_id);
  if (iter != this->attachments_.end()) {
    p_attachment = iter->second.get();
    p_attachment->clamp_min_user_slot(user_slot.slot_offset);

  } else if (attach == AllowAttach::kTrue) {
    auto attachment =
        std::make_unique<PageAllocatorAttachment>(user_slot.user_id, user_slot.slot_offset);
    p_attachment = attachment.get();
    this->attachments_.emplace(user_slot.user_id, std::move(attachment));
  }

  if (p_attachment) {
    this->set_last_update(p_attachment, slot_offset);
  }
}

//----- --- -- -  -  -   -

Status PageAllocatorState::recover(const SlotRange& slot_offset,
                                   const PackedPageAllocatorAttach& attach)
{
  LLFS_VLOG(1) << "recovering " << attach;

  this->update_attachment(slot_offset, attach.user_slot, AllowAttach::kTrue);

  this->update_learned_upper_bound(slot_offset.upper_bound);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::ProposalStatus PageAllocatorState::propose(
    const PackedPageAllocatorDetach& detach)
{
  return this->propose_exactly_once(detach.user_slot, AllowAttach::kFalse);
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

  this->lru_.erase(this->lru_.iterator_to(*iter->second));
  this->attachments_.erase(iter);
}

//----- --- -- -  -  -   -

Status PageAllocatorState::recover(const SlotRange& slot_offset,
                                   const PackedPageAllocatorDetach& detach)
{
  LLFS_VLOG(1) << "recovering" << detach;

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
PageAllocatorState::ProposalStatus PageAllocatorState::propose(const PackedPageRefCount&)
{
  return ProposalStatus::kValid;
}

//----- --- -- -  -  -   -

void PageAllocatorState::learn(const SlotRange& slot_offset, const PackedPageRefCount& packed,
                               Metrics&)
{
  LLFS_VLOG(1) << "learning " << packed;

  const PageId page_id{packed.page_id.value()};
  const page_id_int physical_page = this->page_ids_.get_physical_page(page_id);
  const page_id_int generation = this->page_ids_.get_generation(page_id);

  PageAllocatorRefCount* obj = &this->page_ref_counts_[physical_page];

  BATT_CHECK_EQ(obj->get_count(), packed.ref_count)
      << "Checkpoint slices should never change the "
         "PageAllocator state after recovery completes!";

  BATT_CHECK_EQ(obj->get_generation(), generation)
      << "Checkpoint slices should never change the "
         "PageAllocator state after recovery completes!";

  this->set_last_update(obj, slot_offset);

  this->update_learned_upper_bound(slot_offset.upper_bound);
}

//----- --- -- -  -  -   -

Status PageAllocatorState::recover(const SlotRange& slot_offset, const PackedPageRefCount& packed)
{
  LLFS_VLOG(1) << "recovering " << packed;

  const PageId page_id{packed.page_id.value()};
  const page_id_int physical_page = this->page_ids_.get_physical_page(page_id);
  const page_id_int generation = this->page_ids_.get_generation(page_id);

  PageAllocatorRefCount* obj = &this->page_ref_counts_[physical_page];

  const i32 old_count = obj->set_count(packed.ref_count);
  const page_generation_int old_generation = obj->set_generation(generation);

  this->update_free_pool_status(obj);

  LLFS_VLOG(1) << " -- ref_count: " << old_count << "->" << packed.ref_count
               << ", generation: " << old_generation << "->" << generation;

  this->set_last_update(obj, slot_offset);

  this->update_learned_upper_bound(slot_offset.upper_bound);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::ProposalStatus PageAllocatorState::propose(const PackedPageAllocatorTxn& txn)
{
  return this->propose_exactly_once(txn.user_slot, AllowAttach::kFalse);
}

//----- --- -- -  -  -   -

void PageAllocatorState::learn(const SlotRange& slot_offset, const PackedPageAllocatorTxn& txn,
                               Metrics& metrics)
{
  LLFS_VLOG(1) << "(device=" << this->page_ids_.get_device_id() << ") learning " << txn;

  // Update the client attachment for this transaction so we don't double-commit.
  {
    auto iter = this->attachments_.find(txn.user_slot.user_id);
    BATT_CHECK_NE(iter, this->attachments_.end())
        << "Tried to learn txn from a detached client; this event should have been filtered out by "
           "propose!";

    PageAllocatorAttachment* const attachment = iter->second.get();
    BATT_CHECK(slot_less_than(attachment->get_user_slot(), txn.user_slot.slot_offset))
        << "Tried to learn a txn that we have already learned!  This should have been filtered out "
           "by propose_exactly_once!";

    attachment->set_user_slot(txn.user_slot.slot_offset);
    this->set_last_update(attachment, slot_offset);
  }

  // Apply all ref count updates in the txn.
  //
  for (const PackedPageRefCount& delta : txn.ref_counts) {
    const PageId page_id{delta.page_id.value()};
    const page_id_int physical_page = this->page_ids_.get_physical_page(page_id);
    PageAllocatorRefCount* const obj = &this->page_ref_counts_[physical_page];
    this->learn_ref_count_delta(delta, obj, metrics);
    this->set_last_update(obj, slot_offset);
  }

  this->update_learned_upper_bound(slot_offset.upper_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
namespace {

void run_ref_count_update_sanity_checks(const PackedPageRefCount& delta, i32 before_ref_count)
{
  BATT_CHECK_NE(delta.ref_count, kRefCount_1_to_0);

  const i32 after_ref_count = before_ref_count + delta.ref_count;

  LLFS_VLOG(2) << "page: " << std::hex << delta.page_id.value() << std::dec
               << " += " << delta.ref_count.value() << "; " << before_ref_count << " -> "
               << after_ref_count;

  BATT_CHECK_GE(before_ref_count, 0);
  BATT_CHECK_GE(after_ref_count, 0)
      << "before_ref_count= " << before_ref_count << " delta.ref_count= " << delta.ref_count
      << " page= " << std::hex << delta.page_id.value();

  if (delta.ref_count < 0) {
    BATT_CHECK_LT(after_ref_count, before_ref_count)
        << BATT_INSPECT(PageId{delta.page_id}) << BATT_INSPECT(delta.ref_count);
    BATT_CHECK_GT(after_ref_count, 0)
        << BATT_INSPECT(PageId{delta.page_id}) << BATT_INSPECT(delta.ref_count)
        << BATT_INSPECT(before_ref_count);
    if (after_ref_count == 0) {
      BATT_CHECK_NE(before_ref_count, 2)
          << BATT_INSPECT(delta.page_id) << BATT_INSPECT(delta.ref_count);
    }
  } else if (delta.ref_count > 0) {
    BATT_CHECK_GT(after_ref_count, before_ref_count) << BATT_INSPECT(delta.ref_count.value());
    if (before_ref_count == 0) {
      BATT_CHECK_GE(after_ref_count, 2)
          << BATT_INSPECT(delta.ref_count.value()) << BATT_INSPECT(before_ref_count);
    }
  }
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::learn_ref_count_delta(const PackedPageRefCount& delta,
                                               PageAllocatorRefCount* const obj, Metrics& metrics)
{
  const page_generation_int page_generation =
      this->page_ids_.get_generation(PageId{delta.page_id.value()});

  // Special case for 1 -> 0.
  //
  if (delta.ref_count == kRefCount_1_to_0) {
    this->learn_ref_count_1_to_0(delta, page_generation, obj, metrics);
    return;
  }

  const page_generation_int old_generation = obj->set_generation(page_generation);
  if (old_generation > page_generation) {
    LLFS_LOG_ERROR() << "page generation went backwards!  old=" << old_generation
                     << " current=" << page_generation;
    // TODO [tastolfi 2021-04-05] - stop?  panic?  recover?
  }

  const i32 prior_value = obj->fetch_add(delta.ref_count);
  LLFS_VLOG(2) << "  -- page_id=" << delta.page_id << "; ref_count=" << prior_value << " -> "
               << (prior_value + delta.ref_count) << " (stored=" << obj->get_count() << ")";

  // Sanity checks.
  //
  run_ref_count_update_sanity_checks(delta, prior_value);

  // Detect 0 -> 2+ transition.
  //
  if (prior_value == 0 && delta.ref_count > 0) {
    BATT_CHECK_GE(delta.ref_count, 2);
    if (obj->PageAllocatorFreePoolHook::is_linked()) {
      this->free_pool_.erase(this->free_pool_.iterator_to(*obj));
      this->free_pool_size_.fetch_sub(1);
      metrics.pages_allocated.fetch_add(1);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::learn_ref_count_1_to_0(const PackedPageRefCount& delta,
                                                page_generation_int page_generation,
                                                PageAllocatorRefCount* const obj, Metrics& metrics)
{
  BATT_CHECK_EQ(delta.ref_count, kRefCount_1_to_0);
  BATT_CHECK_EQ(obj->get_generation(), page_generation);

  i32 count = obj->get_count();
  while (count == 1) {
    if (obj->compare_exchange_weak(count, 0)) {
      LLFS_VLOG(1) << "page ref_count => 0 (adding to free pool): " << std::hex
                   << delta.page_id.value();
      if (!obj->PageAllocatorFreePoolHook::is_linked()) {
        BATT_CHECK_EQ(obj->get_count(), 0);

        this->free_pool_.push_back(*obj);
        this->free_pool_size_.fetch_add(1);
        metrics.pages_freed.fetch_add(1);
      }
      break;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status PageAllocatorState::recover(const SlotRange& slot_offset,
                                         const PackedPageAllocatorTxn& txn)
{
  LLFS_VLOG(1) << "(device=" << this->page_ids_.get_device_id() << ") recovering " << txn;

  // Assume that the txn was correctly deduplicated when it was originally appended to the log;
  // don't bother checking during recovery, just update the attachment slot.
  //
  this->update_attachment(slot_offset, txn.user_slot, AllowAttach::kTrue);

  auto& ids = this->page_ids_;

  // Apply all ref count updates in the txn.
  //
  for (const PackedPageRefCount& delta : txn.ref_counts) {
    const PageId page_id{delta.page_id.value()};
    const page_id_int physical_page = ids.get_physical_page(page_id);
    PageAllocatorRefCount* const obj = &this->page_ref_counts_[physical_page];
    const page_generation_int new_generation = ids.get_generation(page_id);
    const page_generation_int old_generation = obj->set_generation(new_generation);

    // Special case for 1 -> 0.
    //
    if (delta.ref_count == kRefCount_1_to_0) {
      obj->set_count(0);

      LLFS_VLOG(1) << " -- page_id: " << page_id << ", ref_count: 1->0"  //
                   << ", generation: " << old_generation << "->" << new_generation;
    } else {
      const i32 old_count = obj->fetch_add(delta.ref_count);
      const i32 new_count = old_count + delta.ref_count;

      LLFS_VLOG(1) << " -- page_id: " << page_id                         //
                   << ", ref_count: " << old_count << "->" << new_count  //
                   << ", generation: " << old_generation << "->" << new_generation;
    }

    this->update_free_pool_status(obj);
    this->set_last_update(obj, slot_offset);
  }

  this->update_learned_upper_bound(slot_offset.upper_bound);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorState::set_last_update(PageAllocatorObjectBase* obj, const SlotRange& slot_offset)
{
  if (obj->PageAllocatorLRUHook::is_linked()) {
    this->lru_.erase(this->lru_.iterator_to(*obj));
  }
  obj->set_last_update(slot_offset.lower_bound);
  this->lru_.push_back(*obj);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::is_ref_count(const PageAllocatorObjectBase* obj) const
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

    // Pages in the free pool must have 0 ref counts and vice versa.
    //
    BATT_CHECK_EQ((ref_count_obj.get_count() == 0),
                  ref_count_obj.PageAllocatorFreePoolHook::is_linked());
  }
}

}  // namespace llfs
