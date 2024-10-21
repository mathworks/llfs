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
    kInvalid_OutOfAttachments = 3,
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
      case ProposalStatus::kInvalid_OutOfAttachments:
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

  /** \brief Determines the validity of `op` and whether it has already been applied to the state
   * machine.
   *
   * This function requires that `op->user_index` is set to PageAllocatorState::kInvalidUserIndex.
   * If the proposal is not invalid, then this member will be set to a newly allocated attachment
   * number, which is used to identify the last modifier of a given page.
   */
  ProposalStatus propose(PackedPageAllocatorAttach* op);

  void learn(const SlotRange& slot_offset, const PackedPageAllocatorAttach& op,
             PageAllocatorMetrics&);

  Status recover(const SlotRange& slot_offset, const PackedPageAllocatorAttach& op);

  //----- --- -- -  -  -   -
  // PackedPageAllocatorDetach event handlers.
  //----- --- -- -  -  -   -

  ProposalStatus propose(PackedPageAllocatorDetach* op);

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

  /** \brief Determines the validity of `op` and whether it has already been applied to the state
   * machine.
   *
   * This function requires op->user_index to be PageAllocatorState::kInvalidUserIndex initially.
   *
   * Side-effects (only if ProposalStatus::kValid is returned):
   *   - op->user_index is updated to the attachment num for the specified user_id
   *   - op->ref_counts are updated to change all ref count _delta_ (relative) values to absolute
   *     values, reflecting the post-transaction state.
   */
  ProposalStatus propose(PackedPageAllocatorTxn* op);

  void learn(const SlotRange& slot_offset, const PackedPageAllocatorTxn& op, PageAllocatorMetrics&);

  Status recover(const SlotRange& slot_offset, const PackedPageAllocatorTxn& op);

  //----- --- -- -  -  -   -

  void check_post_recovery_invariants() const;

  std::vector<PageAllocatorAttachmentStatus> get_all_clients_attachment_status() const;

  Optional<PageAllocatorAttachmentStatus> get_client_attachment_status(
      const boost::uuids::uuid& uuid) const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  /** \brief Finds an attachment number (aka `user_index`) not in use by any other uuid, and binds
   * it to `user_id`.
   *
   * \return the allocated user_index
   */
  StatusOr<u32> allocate_attachment(const boost::uuids::uuid& uuid) noexcept;

  /** \brief Returns the specified user_index (attachment number) to the set of available attachment
   * numbers.
   *
   * If `expected_uuid` is non-None, then this function also checks that `user_index`, if currently
   * in use, is associated with the given uuid (panicking otherwise).
   */
  void deallocate_attachment(u32 user_index,
                             const Optional<boost::uuids::uuid>& expected_uuid = None) noexcept;

  /** \brief Returns the `user_index` associated with a given user_id (uuid), if that user is
   * attached; None otherwise.
   */
  Optional<u32> get_attachment_num(const boost::uuids::uuid& uuid) noexcept;

  /** \brief Checks whether the specified user is attached, and if so, whether the current slot of
   the attachment is "caught-up" with the slot offset specified in `user_slot`.
   *
   * \return
   *   kValid
   *     If the user is attached and the user slot is greater than the last known update for user_id
   *   kNoChange
   *     If the user is attached but the user slot is not greater than the last known update,
   *     indicating that we have already seen this update
   *   kInvalid_NotAttached
   *     If the user is not attached (in which case we have no information on whether the update was
   *     processed)
   */
  ProposalStatus propose_exactly_once(const PackedPageUserSlot& user_slot,
                                      AllowAttach attach) const;

  /** \brief Unconditionally updates the user slot, attaching if necessary.
   *
   * \return true iff successful
   */
  [[nodiscard]] bool update_attachment(const SlotRange& slot_offset,
                                       const PackedPageUserSlot& user_slot, u32 user_index,
                                       AllowAttach attach);

  /** \brief Unconditionally detaches the specified uuid.
   */
  void remove_attachment(const boost::uuids::uuid& user_id);

  /** \brief The core implementation of `learn` and `recover` for PackedPageAllocatorTxn.
   */
  template <bool kInsideRecovery>
  void process_txn(const SlotRange& slot_offset, const PackedPageAllocatorTxn& txn,
                   PageAllocatorMetrics* metrics,
                   std::integral_constant<bool, kInsideRecovery> inside_recovery);

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

  // Returns the new ref count that will result from applying the delta to the passed obj.
  //
  i32 calculate_new_ref_count(const PackedPageRefCount& delta, const u32 index) const;

  /** \brief If the given ref count object has a positive ref count but *is* in the free pool, then
   * this function removes it; otherwise if the object has a zero ref count but is *not* in the free
   * pool, adds it.
   */
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
