//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_STATE_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_STATE_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_allocator/page_allocator_attachment.hpp>
#include <llfs/page_allocator/page_allocator_events.hpp>
#include <llfs/page_allocator/page_allocator_metrics.hpp>
#include <llfs/page_allocator/page_allocator_recovery_visitor.hpp>
#include <llfs/page_allocator/page_allocator_ref_count.hpp>
#include <llfs/page_allocator/page_allocator_state_no_lock.hpp>

#include <llfs/page_id_factory.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_writer.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/uuid/uuid.hpp>

#include <unordered_map>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief The internal state of a PageDeviceIndex; must be externally synchronized for concurrent
 * access.
 */
class PageAllocatorState : public PageAllocatorStateNoLock
{
 public:
  // Define `ThreadSafeBase` type member alias for the benefit of Mutex<PageAllocatorState>.
  //
  using ThreadSafeBase = PageAllocatorStateNoLock;

  enum struct ProposalStatus : int {
    kNoChange = 0,
    kValid = 1,
    kInvalid_NotAttached = 2,
  };

  static bool is_valid(ProposalStatus proposal_status)
  {
    switch (proposal_status) {
      case ProposalStatus::kNoChange:
        return true;
      case ProposalStatus::kValid:
        return true;
      case ProposalStatus::kInvalid_NotAttached:
        return false;
    }
    LLFS_LOG_WARNING() << "Invalid ProposalStatus value: " << (int)proposal_status;
    return false;
  }

  enum struct AllowAttach : bool {
    kFalse = false,
    kTrue = true,
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageAllocatorState(const PageIdFactory& ids) noexcept;

  PageAllocatorState(const PageAllocatorState&) = delete;
  PageAllocatorState& operator=(const PageAllocatorState&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<PageId> allocate_page();

  void deallocate_page(PageId page_id);

  // Write index objects to the log in LRU order until we have written a minimum of
  // `min_byte_count`.
  //
  // Return the slot offset of the new least recently updated object (this is the new safe trim
  // offset).
  //
  StatusOr<slot_offset_type> write_checkpoint_slice(
      TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer, batt::Grant& slice_grant);

#define LLFS_PAGE_DEVICE_INDEX_OP(Type)                                                            \
  ProposalStatus propose(const Type& op);                                                          \
  void learn(const SlotRange& slot_offset, const Type& op, PageAllocatorMetrics&);                 \
  Status recover(const SlotRange& slot_offset, const Type& op)

  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageAllocatorAttach);
  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageAllocatorDetach);
  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageRefCount);
  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageAllocatorTxn);

#undef LLFS_PAGE_DEVICE_INDEX_OP

  void check_post_recovery_invariants() const;

  std::vector<PageAllocatorAttachmentStatus> get_all_clients_attachment_status() const;

  Optional<PageAllocatorAttachmentStatus> get_client_attachment_status(
      const boost::uuids::uuid& uuid) const;

  void refresh_client_attachment(const boost::uuids::uuid& uuid, slot_offset_type slot);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  ProposalStatus propose_exactly_once(const PackedPageUserSlot& user_slot,
                                      AllowAttach attach) const;

  /** \brief Unconditionally updates the user slot, attaching if necessary.
   */
  void update_attachment(const SlotRange& slot_offset, const PackedPageUserSlot& user_slot,
                         AllowAttach attach);

  /** \brief Unconditionally detaches the specified uuid.
   */
  void remove_attachment(const boost::uuids::uuid& user_id);

  // Sets the `last_update` field on `obj` to `slot_offset.lower_bound` and moves `obj` to the back
  // of the LRU list.
  //
  void set_last_update(PageAllocatorObjectBase* obj, const SlotRange& slot_offset);

  // Returns true iff `obj` is an PageAllocatorRefCount belonging to _this_ PageAllocator.
  //
  bool is_ref_count(const PageAllocatorObjectBase* obj) const;

  // Returns a range of all the page ref counts in this index.
  //
  boost::iterator_range<PageAllocatorRefCount*> page_ref_counts();

  // Returns a range of all the page ref counts in this index (const).
  //
  boost::iterator_range<const PageAllocatorRefCount*> page_ref_counts() const;

  // Advance the current learned upper bound.
  //
  void update_learned_upper_bound(slot_offset_type offset);

  void learn_ref_count_delta(const PackedPageRefCount& delta, PageAllocatorRefCount* const obj,
                             PageAllocatorMetrics& metrics);

  void learn_ref_count_1_to_0(const PackedPageRefCount& delta, page_generation_int page_generation,
                              PageAllocatorRefCount* const obj, PageAllocatorMetrics& metrics);

  void update_free_pool_status(PageAllocatorRefCount* obj);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The current active user attachments.
  //
  PageAllocatorAttachmentMap attachments_;

  // All indexed objects ordered by the index slot at which they were last updated.
  //
  PageAllocatorLRUList lru_;

  // All pages with ref count 0 that have not been allocated.
  //
  PageAllocatorFreePoolList free_pool_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_STATE_HPP
