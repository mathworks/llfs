#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_STATE_HPP
#define LLFS_PAGE_ALLOCATOR_STATE_HPP

#include <llfs/page_allocator_events.hpp>
#include <llfs/page_allocator_metrics.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_writer.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/uuid/uuid.hpp>

#include <unordered_map>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// PageAllocatorObject - base for all objects tracked by the PageAllocator; remembers when it was
// last updated (and its place in an LRU list).
//
using PageAllocatorLRUHook =
    boost::intrusive::list_base_hook<boost::intrusive::tag<struct PageAllocatorLRUTag>>;

class PageAllocatorObject : public PageAllocatorLRUHook
{
 public:
  void set_last_update(slot_offset_type slot)
  {
    this->last_update_ = slot;
  }

  slot_offset_type last_update() const
  {
    return this->last_update_;
  }

 private:
  slot_offset_type last_update_ = ~slot_offset_type{0};
};

using PageAllocatorLRUList =
    boost::intrusive::list<PageAllocatorObject, boost::intrusive::base_hook<PageAllocatorLRUHook>>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// An attachment from a unique user to the index; facilitates idempotent ("exactly-once") updates.
//
class PageAllocatorAttachment : public PageAllocatorObject
{
 public:
  explicit PageAllocatorAttachment(const boost::uuids::uuid& user_id,
                                   slot_offset_type user_slot) noexcept
      : user_id_{user_id}
      , user_slot_{user_slot}
  {
  }

  const boost::uuids::uuid& get_user_id() const
  {
    return this->user_id_;
  }

  void set_user_slot(slot_offset_type slot_offset)
  {
    this->user_slot_ = slot_offset;
  }

  slot_offset_type get_user_slot() const
  {
    return this->user_slot_;
  }

 private:
  const boost::uuids::uuid user_id_;
  slot_offset_type user_slot_;
};

struct PageAllocatorAttachmentStatus {
  boost::uuids::uuid user_id;
  slot_offset_type user_slot;
};

// (Hash) Map from client/user UUID to PageAllocatorAttachment.
//
using PageAllocatorAttachmentMap =
    std::unordered_map<boost::uuids::uuid, std::unique_ptr<PageAllocatorAttachment>,
                       boost::hash<boost::uuids::uuid>>;

// Base list node hook that allows a PageAllocatorRefCount to be added to the free pool.
//
using PageAllocatorFreePoolHook =
    boost::intrusive::list_base_hook<boost::intrusive::tag<struct PageAllocatorFreePoolTag>>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// PageAllocatorRefCount - the current ref count value for a single page, with last modified slot
// and hooks for the LRU list and free pool.
//
class PageAllocatorRefCount
    : public PageAllocatorObject
    , public PageAllocatorFreePoolHook
{
 public:
  PageAllocatorRefCount() = default;

  PageAllocatorRefCount(const PageAllocatorRefCount&) = delete;
  PageAllocatorRefCount& operator=(const PageAllocatorRefCount&) = delete;

  i32 get_count() const
  {
    return this->count_.load();
  }

  page_generation_int get_generation() const
  {
    return this->generation_.load();
  }

  bool compare_exchange_weak(i32& expected, i32 desired)
  {
    return this->count_.compare_exchange_weak(expected, desired);
  }

  i32 fetch_add(i32 delta)
  {
    return this->count_.fetch_add(delta);
  }

  i32 set_count(i32 value)
  {
    return this->count_.exchange(value);
  }

  page_generation_int set_generation(page_generation_int generation)
  {
    return this->generation_.exchange(generation);
  }

  page_generation_int advance_generation()
  {
    return this->generation_.fetch_add(1) + 1;
  }

  page_generation_int revert_generation()
  {
    return this->generation_.fetch_sub(1) - 1;
  }

 private:
  std::atomic<page_generation_int> generation_{0};
  std::atomic<i32> count_{0};
};

using PageAllocatorFreePoolList =
    boost::intrusive::list<PageAllocatorRefCount,
                           boost::intrusive::base_hook<PageAllocatorFreePoolHook>>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Base class of PageAllocatorState comprised of state that is safe to access without holding a
// mutex lock.
//
class PageAllocatorStateNoLock
{
 public:
  explicit PageAllocatorStateNoLock(const PageIdFactory& ids) noexcept;

  const PageIdFactory& page_ids() const
  {
    return this->page_ids_;
  }

  StatusOr<slot_offset_type> await_learned_slot(slot_offset_type min_learned_upper_bound);

  slot_offset_type learned_upper_bound() const
  {
    return this->learned_upper_bound_.get_value();
  }

  Status await_free_page();

  u64 page_device_capacity() const noexcept
  {
    return this->page_ids_.get_physical_page_count();
  }

  u64 free_pool_size()
  {
    return this->free_pool_size_.get_value();
  }

  std::pair<i32, slot_offset_type> get_ref_count(PageId id) const noexcept
  {
    const page_id_int physical_page = this->page_ids_.get_physical_page(id);
    BATT_CHECK_LT(physical_page, this->page_device_capacity());

    slot_offset_type slot = this->learned_upper_bound_.get_value();
    return std::make_pair(this->page_ref_counts_[physical_page].get_count(), slot);
  }

  PageRefCount get_ref_count_obj(PageId id) const noexcept
  {
    const page_id_int physical_page = this->page_ids_.get_physical_page(id);
    BATT_ASSERT_LT(physical_page, this->page_device_capacity());

    const auto& iprc = this->page_ref_counts_[physical_page];

    return PageRefCount{
        .page_id = this->page_ids_.make_page_id(physical_page, iprc.get_generation()).int_value(),
        .ref_count = iprc.get_count(),
    };
  }

  void halt() noexcept
  {
    this->learned_upper_bound_.close();
    this->free_pool_size_.close();
  }

 protected:
  // Returns the index of `ref_count` in the `page_ref_counts_` array (which is also the physical
  // page number for that page's device).  Panic if `ref_count` is not in our ref counts array.
  //
  isize index_of(const PageAllocatorRefCount* ref_count) const;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::Watch<slot_offset_type> learned_upper_bound_{0};

  // The size of the free pool; used to allow blocking on free pages becoming available.
  //
  batt::Watch<u64> free_pool_size_{0};

  // The number of pages addressable by the device.
  //
  const PageIdFactory page_ids_;

  // The array of page ref counts, indexed by the page id.
  //
  const std::unique_ptr<PageAllocatorRefCount[]> page_ref_counts_{
      new PageAllocatorRefCount[this->page_device_capacity()]};
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// The internal state of a PageDeviceIndex; must be synchronized for concurrent access.
//
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
    LOG(WARNING) << "Invalid ProposalStatus value: " << (int)proposal_status;
    return false;
  }

  enum struct AllowAttach : bool {
    kFalse = false,
    kTrue = true,
  };

  explicit PageAllocatorState(const PageIdFactory& ids) noexcept;

  PageAllocatorState(const PageAllocatorState&) = delete;
  PageAllocatorState& operator=(const PageAllocatorState&) = delete;

  void revert(const PageAllocatorState& prior);

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

#define LLFS_PAGE_DEVICE_INDEX_OP(Type)                                                            \
  ProposalStatus propose(const Type& op);                                                          \
  void learn(slot_offset_type index_slot, const Type& op, PageAllocatorMetrics&)

  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageAllocatorAttach);
  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageAllocatorDetach);
  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageRefCount);
  LLFS_PAGE_DEVICE_INDEX_OP(PackedPageAllocatorTxn);

#undef LLFS_PAGE_DEVICE_INDEX_OP

  void set_recovering(bool value)
  {
    this->recovering_.store(value);
  }

  std::vector<PageAllocatorAttachmentStatus> get_all_clients_attachment_status() const;

  Optional<PageAllocatorAttachmentStatus> get_client_attachment_status(
      const boost::uuids::uuid& uuid) const;

 private:
  ProposalStatus propose_exactly_once(const PackedPageUserSlot& user_slot,
                                      AllowAttach attach) const;

  // Sets the `last_update` field on `obj` to `index_slot` and moves `obj` to the back of the LRU
  // list.
  //
  void set_last_update(PageAllocatorObject* obj, slot_offset_type index_slot);

  // Returns true iff `obj` is an PageAllocatorRefCount belonging to _this_ PageAllocator.
  //
  bool is_ref_count(const PageAllocatorObject* obj) const;

  // Returns a range of all the page ref counts in this index.
  //
  boost::iterator_range<PageAllocatorRefCount*> page_ref_counts();

  // Advance the current learned upper bound.
  //
  void update_learned_upper_bound(slot_offset_type offset);

  void learn_ref_count_delta(const PackedPageRefCount& delta, PageAllocatorRefCount* const obj,
                             PageAllocatorMetrics& metrics);

  void learn_ref_count_1_to_0(const PackedPageRefCount& delta, page_generation_int page_generation,
                              PageAllocatorRefCount* const obj, PageAllocatorMetrics& metrics);

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

  // Flag to indicate whether we are in steady-state.
  //
  std::atomic<bool> recovering_{true};
};

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_STATE_HPP
