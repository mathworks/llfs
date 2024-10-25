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
NoOutgoingRefsCache::NoOutgoingRefsCache(u64 physical_page_count) noexcept
    : cache_(physical_page_count)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void NoOutgoingRefsCache::set_page_bits(i64 physical_page_id, page_generation_int generation,
                                        HasNoOutgoingRefs has_no_out_going_refs)
{
  BATT_CHECK_LT((usize)physical_page_id, this->cache_.size());

  u64 new_state = generation << this->generation_shift_;
  // Set the "valid" bit to 1.
  //
  new_state |= (u64{1} << 1);
  if (has_no_out_going_refs) {
    // Set the "has no outgoing references" bit to 1.
    //
    new_state |= u64{1};
  }

  u64 old_state = this->cache_[physical_page_id].exchange(new_state, std::memory_order_acq_rel);
  // Sanity check: we are not setting the bits for the same generation more than once.
  //
  page_generation_int old_generation = old_state >> this->generation_shift_;
  BATT_CHECK_NE(generation, old_generation);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 NoOutgoingRefsCache::get_page_bits(i64 physical_page_id, page_generation_int generation) const
{
  BATT_CHECK_LT((usize)physical_page_id, this->cache_.size());

  u64 current_state = this->cache_[physical_page_id].load(std::memory_order_acquire);
  page_generation_int stored_generation = current_state >> this->generation_shift_;
  // If the generation that is currently stored in the cache is not the same as the generation we
  // are querying for, this cache entry is invalid. Thus, we return a "not traced" status.
  //
  if (stored_generation != generation) {
    return u64{0};
  }
  return current_state & this->bit_mask_;
}
}  // namespace llfs