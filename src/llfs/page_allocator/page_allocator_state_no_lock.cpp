//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator/page_allocator_state_no_lock.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorStateNoLock::PageAllocatorStateNoLock(const PageIdFactory& ids) noexcept
    : page_ids_{ids}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageIdFactory& PageAllocatorStateNoLock::page_ids() const
{
  return this->page_ids_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> PageAllocatorStateNoLock::await_learned_slot(
    slot_offset_type min_learned_upper_bound)
{
  return await_slot_offset(min_learned_upper_bound, this->learned_upper_bound_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type PageAllocatorStateNoLock::learned_upper_bound() const
{
  return this->learned_upper_bound_.get_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorStateNoLock::await_free_page()
{
  return this->free_pool_size_
      .await_true([](u64 available) {
        return available > 0;
      })
      .status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 PageAllocatorStateNoLock::page_device_capacity() const noexcept
{
  return this->page_ids_.get_physical_page_count();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 PageAllocatorStateNoLock::free_pool_size() noexcept
{
  return this->free_pool_size_.get_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::pair<i32, slot_offset_type> PageAllocatorStateNoLock::get_ref_count(PageId id) const noexcept
{
  const page_id_int physical_page = this->page_ids_.get_physical_page(id);
  BATT_CHECK_LT(physical_page, this->page_device_capacity());

  slot_offset_type slot = this->learned_upper_bound_.get_value();
  return std::make_pair(this->page_ref_counts_[physical_page].get_count(), slot);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageRefCount PageAllocatorStateNoLock::get_ref_count_obj(PageId id) const noexcept
{
  const page_id_int physical_page = this->page_ids_.get_physical_page(id);
  BATT_ASSERT_LT(physical_page, this->page_device_capacity());

  const auto& iprc = this->page_ref_counts_[physical_page];

  // Load count then generation, to avoid A-B-A race condition where we think we are observing a
  // ref_count that goes down to 1 (which should indicate there are no races/concurrent updates
  // going on to this page count since the caller is the sole owner), but that count is from a
  // later generation.  If we load generation after count, then the caller observes the generation
  // *not* to have changed, that means count was accurate for that generation in this case.
  //
  const auto physical_page_ref_count = iprc.get_count();
  //  (^^^ count)  (generation vvv)
  const auto physical_page_generation = iprc.get_generation();

  return PageRefCount{
      .page_id = this->page_ids_.make_page_id(physical_page, physical_page_generation).int_value(),
      .ref_count = physical_page_ref_count,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorStateNoLock::halt() noexcept
{
  this->learned_upper_bound_.close();
  this->free_pool_size_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
isize PageAllocatorStateNoLock::index_of(const PageAllocatorRefCount* ref_count_obj) const
{
  return std::distance<const PageAllocatorRefCount*>(&this->page_ref_counts_[0], ref_count_obj);
}

}  // namespace llfs
