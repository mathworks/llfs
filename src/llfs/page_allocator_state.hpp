//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_STATE_HPP
#define LLFS_PAGE_ALLOCATOR_STATE_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_allocator_attachment.hpp>
#include <llfs/page_allocator_events.hpp>
#include <llfs/page_allocator_metrics.hpp>
#include <llfs/page_allocator_ref_count.hpp>
#include <llfs/page_allocator_state_no_lock.hpp>

#include <llfs/page_id_factory.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_writer.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/uuid/uuid.hpp>

#include <unordered_map>
#include <unordered_set>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief The internal state of a PageDeviceIndex; must be externally synchronized for concurrent
 * access.
 */
class PageAllocatorState : public PageAllocatorStateNoLock
{
 public:
  static constexpr u32 kInvalidUserIndex = PageAllocatorRefCount::kInvalidUserIndex;

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

  explicit PageAllocatorState(const PageIdFactory& ids, u64 max_attachments) noexcept;

  PageAllocatorState(const PageAllocatorState&) = delete;
  PageAllocatorState& operator=(const PageAllocatorState&) = delete;

  ~PageAllocatorState() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<PageId> allocate_page();

  void deallocate_page(PageId page_id);

  // Returns Ok if the given page_id was successfully removed from the free pool.
  //
  Status recover_page(PageId page_id);

  // Write index objects to the log in LRU order until we have written a minimum of
  // `min_byte_count`.
  //
  // Return the slot offset of the new least recently updated object (this is the new safe trim
  // offset).
  //
  StatusOr<slot_offset_type> write_checkpoint_slice(
      TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer, batt::Grant& slice_grant);

  //----- --- -- -  -  -   -
  // PackedPageAllocatorAttach event handlers.
  //----- --- -- -  -  -   -

  ProposalStatus propose(const PackedPageAllocatorAttach& op);

  void learn(const SlotRange& slot_offset, const PackedPageAllocatorAttach& op,
             PageAllocatorMetrics&);

  Status recover(const SlotRange& slot_offset, const PackedPageAllocatorAttach& op);

  //----- --- -- -  -  -   -
  // PackedPageAllocatorDetach event handlers.
  //----- --- -- -  -  -   -

  ProposalStatus propose(const PackedPageAllocatorDetach& op);

  void learn(const SlotRange& slot_offset, const PackedPageAllocatorDetach& op,
             PageAllocatorMetrics&);

  Status recover(const SlotRange& slot_offset, const PackedPageAllocatorDetach& op);

  //----- --- -- -  -  -   -
  // PackedPageRefCountRefresh event handlers.
  //----- --- -- -  -  -   -

  Status recover(const SlotRange& slot_offset, const PackedPageRefCountRefresh& op);

  //----- --- -- -  -  -   -
  // PackedPageAllocatorTxn event handlers.
  //----- --- -- -  -  -   -

  ProposalStatus propose(const PackedPageAllocatorTxn& op);

  void learn(const SlotRange& slot_offset, const PackedPageAllocatorTxn& op, PageAllocatorMetrics&);

  Status recover(const SlotRange& slot_offset, const PackedPageAllocatorTxn& op);

  //----- --- -- -  -  -   -

  void check_post_recovery_invariants() const;

  std::vector<PageAllocatorAttachmentStatus> get_all_clients_attachment_status() const;

  Optional<PageAllocatorAttachmentStatus> get_client_attachment_status(
      const boost::uuids::uuid& uuid) const;

  StatusOr<u32> allocate_attachment(const boost::uuids::uuid& uuid) noexcept;

  void deallocate_attachment(u32 user_index,
                             const Optional<boost::uuids::uuid>& expected_uuid = None) noexcept;

  Optional<u32> get_attachment_num(const boost::uuids::uuid& uuid) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  ProposalStatus propose_exactly_once(const PackedPageUserSlot& user_slot,
                                      AllowAttach attach) const;

  /** \brief Unconditionally updates the user slot, attaching if necessary.
   */
  void update_attachment(const SlotRange& slot_offset, const PackedPageUserSlot& user_slot,
                         u32 user_index, AllowAttach attach);

  /** \brief Unconditionally detaches the specified uuid.
   */
  void remove_attachment(const boost::uuids::uuid& user_id);

  // Sets the `last_update` field on `obj` to `slot_offset.lower_bound` and moves `obj` to the back
  // of the LRU list.
  //
  void set_last_update(PageAllocatorLRUBase* obj, const SlotRange& slot_offset);

  // Returns true iff `obj` is an PageAllocatorRefCount belonging to _this_ PageAllocator.
  //
  bool is_ref_count(const PageAllocatorLRUBase* obj) const;

  // Returns a range of all the page ref counts in this index.
  //
  boost::iterator_range<PageAllocatorRefCount*> page_ref_counts();

  // Returns a range of all the page ref counts in this index (const).
  //
  boost::iterator_range<const PageAllocatorRefCount*> page_ref_counts() const;

  // Advance the current learned upper bound.
  //
  void update_learned_upper_bound(slot_offset_type offset);

  void learn_ref_count_delta(const SlotRange& slot_offset, const PackedPageRefCount& delta,
                             PageAllocatorRefCount* const obj, PageAllocatorMetrics& metrics);

  void learn_ref_count_1_to_0(const SlotRange& slot_offset, const PackedPageRefCount& delta,
                              page_generation_int page_generation, PageAllocatorRefCount* const obj,
                              PageAllocatorMetrics& metrics);

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

  // The set of attachment numbers (i.e., user_index) known to be free.
  //
  std::unordered_set<u32> free_attach_nums_;

  // The current assignment of attachment number (i.e. user_index) to uuid (user_id).
  //
  std::vector<batt::Optional<boost::uuids::uuid>> attachment_by_index_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_STATE_HPP
