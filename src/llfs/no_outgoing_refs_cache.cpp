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
                                         HasOutgoingRefs new_has_outgoing_refs_state) noexcept
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(page_id), this->page_ids_.get_device_id());
  const u64 physical_page = this->page_ids_.get_physical_page(page_id);
  const page_generation_int generation = this->page_ids_.get_generation(page_id);
  BATT_CHECK_LT((usize)physical_page, this->cache_.size());

  u64 new_cache_entry = generation << this->kGenerationShift;

  // Set the "valid" bit to 1.
  //
  new_cache_entry |= this->kValidBitMask;

  // If new_has_outgoing_refs_state has value false, page_id has no outgoing references.
  //
  if (!new_has_outgoing_refs_state) {
    // Set the "has no outgoing references" bit to 1.
    //
    new_cache_entry |= this->kHasNoOutgoingRefsBitMask;
  }

  u64 old_cache_entry = this->cache_[physical_page].exchange(new_cache_entry);

  // Two sanity checks:
  //  1) We are not going backwards in generation.
  //  2) If the cache entry is set of the same generation multiple times, the same value should be
  //  set.
  //
  page_generation_int old_generation = old_cache_entry >> this->kGenerationShift;
  BATT_CHECK_GE(generation, old_generation);
  if (generation == old_generation) {
    BATT_CHECK_EQ(new_cache_entry, old_cache_entry);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::BoolStatus NoOutgoingRefsCache::has_outgoing_refs(PageId page_id) const noexcept
{
  BATT_CHECK_EQ(PageIdFactory::get_device_id(page_id), this->page_ids_.get_device_id());
  const u64 physical_page = this->page_ids_.get_physical_page(page_id);
  const page_generation_int generation = this->page_ids_.get_generation(page_id);
  BATT_CHECK_LT((usize)physical_page, this->cache_.size());

  u64 current_cache_entry = this->cache_[physical_page].load();
  page_generation_int stored_generation = current_cache_entry >> this->kGenerationShift;
  u64 outgoing_refs_status = current_cache_entry & this->kOutgoingRefsStatusBitsMask;

  // If the generation that is currently stored in the cache is not the same as the generation we
  // are querying for, this cache entry is invalid. Thus, we return a "unknown" status.
  //
  if (stored_generation != generation) {
    return batt::BoolStatus::kUnknown;
  }

  switch (outgoing_refs_status) {
    case 0:
      // Bit status 00, not traced yet.
      //
      return batt::BoolStatus::kUnknown;
    case 1:
      BATT_PANIC() << "The lower two outgoing refs bits in a cache entry can never be 01!";
      BATT_UNREACHABLE();
    case 2:
      // Bit status 10, has outgoing refs.
      //
      return batt::BoolStatus::kTrue;
    case 3:
      // Bit status 11, no outgoing refs.
      //
      return batt::BoolStatus::kFalse;
    default:
      BATT_PANIC() << "Impossible outgoing refs bits state: " << outgoing_refs_status;
      BATT_UNREACHABLE();
  }
}
}  // namespace llfs