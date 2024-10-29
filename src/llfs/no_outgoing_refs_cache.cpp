//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -
#include <llfs/no_outgoing_refs_cache.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
NoOutgoingRefsCache::NoOutgoingRefsCache(const PageIdFactory& page_ids) noexcept
    : page_ids_{page_ids}
    , cache_(this->page_ids_.get_physical_page_count().value())
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void NoOutgoingRefsCache::set_page_state(PageId page_id,
                                         batt::BoolStatus new_has_outgoing_refs_state) noexcept
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(page_id), this->page_ids_.get_device_id());
  const u64 physical_page = this->page_ids_.get_physical_page(page_id);
  const page_generation_int generation = this->page_ids_.get_generation(page_id);
  BATT_CHECK_LT((usize)physical_page, this->cache_.size());

  u64 new_cache_entry = generation << this->kGenerationShift;

  // Set the "valid" bit to 1.
  //
  new_cache_entry |= this->kValidBitMask;

  // If new_has_outgoing_refs_state has value kFalse, page_id has no outgoing references.
  //
  if (new_has_outgoing_refs_state == batt::BoolStatus::kFalse) {
    // Set the "has no outgoing references" bit to 1.
    //
    new_cache_entry |= u64{1};
  }

  u64 old_cache_entry =
      this->cache_[physical_page].exchange(new_cache_entry, std::memory_order_acq_rel);

  // Sanity check: we are not setting the bits for the same generation more than once.
  //
  page_generation_int old_generation = old_cache_entry >> this->kGenerationShift;
  BATT_CHECK_NE(generation, old_generation);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::BoolStatus NoOutgoingRefsCache::has_outgoing_refs(PageId page_id) const noexcept
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(page_id), this->page_ids_.get_device_id());
  const u64 physical_page = this->page_ids_.get_physical_page(page_id);
  const page_generation_int generation = this->page_ids_.get_generation(page_id);
  BATT_CHECK_LT((usize)physical_page, this->cache_.size());

  u64 current_cache_entry = this->cache_[physical_page].load(std::memory_order_acquire);
  page_generation_int stored_generation = current_cache_entry >> this->kGenerationShift;
  OutgoingRefsStatus outgoing_refs_status =
      static_cast<OutgoingRefsStatus>(current_cache_entry & this->kOutgoingRefsBitMask);

  // If the generation that is currently stored in the cache is not the same as the generation we
  // are querying for, this cache entry is invalid. Thus, we return a "unknown" status.
  //
  if (stored_generation != generation || outgoing_refs_status == OutgoingRefsStatus::kNotTraced) {
    return batt::BoolStatus::kUnknown;
  }
  return batt::bool_status_from(outgoing_refs_status == OutgoingRefsStatus::kHasOutgoingRefs);
}
}  // namespace llfs