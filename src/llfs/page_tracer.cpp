//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_tracer.hpp>

namespace llfs {
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageTracer
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status PageTracer::trace_refs_recursively(
    PageLoader& loader, batt::BoxedSeq<PageId>&& roots,
    std::function<bool(PageId)>&& recursion_pred,
    std::function<void(PageId)>&& traced_page_fn) noexcept
{
  std::unordered_set<PageId, PageId::Hash> pushed;
  std::vector<PageId> pending;

  BATT_FORWARD(roots) | seq::emplace_back(&pending);

  for (const PageId& page_id : pending) {
    pushed.insert(page_id);
  }

  // Perform DFS, recursing if the predicate function evaluates to true.
  //
  while (!pending.empty()) {
    const PageId next = pending.back();
    pending.pop_back();

    // If we already have information that this page has no outgoing refs, do not perform
    // the page load and call trace refs; there's nothing we need to do.
    //
    const OutgoingRefsStatus outgoing_refs_status = this->get_outgoing_refs_info(next);
    if (outgoing_refs_status == OutgoingRefsStatus::kNoOutgoingRefs) {
      LLFS_VLOG(2) << "[PageTracer::trace_refs_recursively] page " << next
                   << " has no outgoing refs.";
      continue;
    }
    batt::StatusOr<PinnedPage> status_or_page = loader.get_page(next, OkIfNotFound{false});
    BATT_REQUIRE_OK(status_or_page);

    PinnedPage& page = *status_or_page;
    BATT_CHECK_NOT_NULLPTR(page);

    batt::BoxedSeq<PageId> outgoing_refs = page->trace_refs();

    // If outgoing_refs_status has any other status, we must call trace_refs and set
    // the outgoing refs information if it hasn't ever been traced before.
    //
    OutgoingRefsStatus new_outgoing_refs_status = OutgoingRefsStatus::kNoOutgoingRefs;
    if (outgoing_refs.peek()) {
      LLFS_VLOG(2) << "[PageTracer::trace_refs_recursively] page " << next << "has outgoing refs.";

      outgoing_refs | seq::for_each([&](const PageId& id) {
        traced_page_fn(id);

        if (!pushed.count(id) && recursion_pred(id)) {
          pushed.insert(id);
          pending.push_back(id);
        }
      });

      new_outgoing_refs_status = OutgoingRefsStatus::kHasOutgoingRefs;
    }

    if (outgoing_refs_status == OutgoingRefsStatus::kNotTraced) {
      this->set_outgoing_refs_info(next, new_outgoing_refs_status);
    }
  }

  return batt::OkStatus();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class NoOutgoingRefsCache
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//

NoOutgoingRefsCache::NoOutgoingRefsCache(u64 physical_page_count) noexcept
    : cache_((physical_page_count + this->pages_per_index_ - 1) /
             this->pages_per_index_)  // Round up for the number of elements we need in the vevctor
                                      // when performing this division
{
}

void NoOutgoingRefsCache::set_page_bits(i64 physical_page_id, OutgoingRefsStatus has_out_going_refs)
{
  const auto [cache_index, bit_offset] = this->compute_cache_index_and_bit_offset(physical_page_id);

  // Set the two bits for the page. The upper of the two bits is always set to 1 in this function.
  //
  u64 mask = u64{1} << (bit_offset + 1);
  if (has_out_going_refs == OutgoingRefsStatus::kNoOutgoingRefs) {
    mask |= u64{1} << bit_offset;
  }
  u64 old_value = this->cache_[cache_index].fetch_or(mask, std::memory_order_acq_rel);
  // Sanity check: before setting, the bits should always be 00 for this page,
  // because it was either the first ever write or because they must have been
  // cleared by clear_page_bits when the previous generation page was dropped.
  //
  BATT_CHECK_EQ((old_value >> bit_offset) & this->bit_mask_, 0);
}

void NoOutgoingRefsCache::clear_page_bits(i64 physical_page_id)
{
  const auto [cache_index, bit_offset] = this->compute_cache_index_and_bit_offset(physical_page_id);

  const u64 mask = ~(this->bit_mask_ << bit_offset);
  this->cache_[cache_index].fetch_and(mask, std::memory_order_acq_rel);
}

u64 NoOutgoingRefsCache::get_page_bits(i64 physical_page_id) const
{
  const auto [cache_index, bit_offset] = this->compute_cache_index_and_bit_offset(physical_page_id);

  return (this->cache_[cache_index].load(std::memory_order_acquire) >> bit_offset) &
         this->bit_mask_;
}

}  // namespace llfs
