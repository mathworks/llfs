#include <llfs/page_recycler_options.hpp>
//

#include <llfs/page_recycler_events.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageRecyclerOptions::insert_grant_size() const
{
  return (packed_sizeof<PackedVariant<>>() + packed_sizeof<PackedRecyclePageInserted>() +
          kMaxSlotHeaderSize) *
         this->refresh_factor;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageRecyclerOptions::remove_grant_size() const
{
  return (packed_sizeof<PackedVariant<>>() + packed_sizeof<PackedRecyclePagePrepare>() +
          kMaxSlotHeaderSize);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageRecyclerOptions::total_page_grant_size() const
{
  return this->insert_grant_size() + this->remove_grant_size() + this->commit_slot_size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageRecyclerOptions::total_grant_size_for_depth(u32 depth) const
{
  return this->total_page_grant_size() *
         ((1 /*the page itself*/) + (kMaxPageRefDepth - depth) * this->max_refs_per_page) *
         this->batch_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageRecyclerOptions::info_slot_size() const
{
  return (packed_sizeof<PackedVariant<>>() + packed_sizeof<PackedPageRecyclerInfo>() +
          kMaxSlotHeaderSize);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageRecyclerOptions::commit_slot_size() const
{
  return (packed_sizeof<PackedVariant<>>() + packed_sizeof<PackedRecycleBatchCommit>() +
          kMaxSlotHeaderSize);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 PageRecyclerOptions::recycle_task_target() const
{
  return this->total_grant_size_for_depth(0) + this->info_slot_size() * 2;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageRecyclerOptions::info_needs_refresh(slot_offset_type last_info_refresh_slot_lower_bound,
                                             LogDevice& log_device) const
{
  return (slot_distance(last_info_refresh_slot_lower_bound,
                        log_device.slot_range(LogReadMode::kSpeculative).upper_bound) +
          this->info_slot_size()) >= (log_device.capacity() / this->info_refresh_rate);
}

}  // namespace llfs
