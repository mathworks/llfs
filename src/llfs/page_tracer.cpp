//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_tracer.hpp>
#include "page_tracer.hpp"

namespace llfs {
  //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
  // class PageTracer
  //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  batt::Status PageTracer::trace_refs_recursively(PageLoader& loader, batt::BoxedSeq<PageId>&& roots, std::function<bool(PageId)> &&recursion_pred, std::function<void(PageId)> &&traced_page_fn)
  {
    std::unordered_set<PageId, PageId::Hash> pushed;
    std::vector<PageId> pending;

    BATT_FORWARD(roots) | seq::emplace_back(&pending);

    for (const PageId& page_id : pending) {
      pushed.insert(page_id);
    }

    // Perform DFS, recursing id the predicate function evaluates to true.
    //
    while (!pending.empty()) {
      const PageId next = pending.back();
      pending.pop_back();

      // If we already have information that this page has no outgoing refs, do not perform
      // the page load and call trace refs; there's nothing we need to do.
      //
      OutgoingRefsStatus outgoing_refs_status = this->get_outgoing_refs_info(next);
      if (outgoing_refs_status == OutgoingRefsStatus::kNoOutgoingRefs) {
        continue;
      }
      batt::StatusOr<PinnedPage> status_or_page = loader.get_page(next, OkIfNotFound{false});
      BATT_REQUIRE_OK(status_or_page);

      PinnedPage& page = *status_or_page;
      BATT_CHECK_NOT_NULLPTR(page);

      auto outgoing_refs = page->trace_refs();

      // If outgoing_refs_status has any other status, we must call trace_refs and set
      // the outgoing refs information if it hasn't ever been traced before.
      //
      OutgoingRefsStatus new_outgoing_refs_status = OutgoingRefsStatus::kNoOutgoingRefs;
      if (outgoing_refs.peek()) {
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
  NoOutgoingRefsCache::NoOutgoingRefsCache(const std::vector<u64>& device_capacities) noexcept
  {
    this->base_device_indices_.resize(device_capacities.size());

    u64 current_index = 0;
    u64 total_elements_needed = 0;
    for (u64 i = 0; i < device_capacities.size(); ++i) {
      this->base_device_indices_[i] = current_index;
      u64 elements_required = (device_capacities[i] + 31) / 32;
      current_index += elements_required;
      total_elements_needed += elements_required;
    }

    this->cache_ = std::vector<std::atomic<u64>>(total_elements_needed);
  }

  u64 NoOutgoingRefsCache::compute_cache_index(page_device_id_int device_id, i64 physical_page_id)
  {
    BATT_CHECK_LT(device_id, this->base_device_indices_.size());
    return this->base_device_indices_[device_id] + (physical_page_id / this->pages_per_index_);
  }

  u64 NoOutgoingRefsCache::compute_bit_offset(i64 physical_page_id)
  {
    return (physical_page_id % this->pages_per_index_) * 2;
  }

  void NoOutgoingRefsCache::set_page_bits(page_device_id_int device_id, i64 physical_page_id, OutgoingRefsStatus has_out_going_refs)
  {
    u64 cache_index = this->compute_cache_index(device_id, physical_page_id);
    BATT_CHECK_LT(cache_index, this->cache_.size());
    u64 bit_offset = this->compute_bit_offset(physical_page_id);
    
    // Set the two bits for the page. The upper bit of the two is always set to 1 in this function.
    //
    u64 mask = u64{1} << (bit_offset + 1);
    if (has_out_going_refs == OutgoingRefsStatus::kNoOutgoingRefs) {
      mask |= u64{1} << bit_offset;
    }
    u64 old_value = this->cache_[cache_index].fetch_or(mask, std::memory_order_acq_rel);
    // Sanity check: before setting, the bits should always be 00 for this page,
    // because it was the first ever write (generation 0) or because they must have been
    // cleared by clear_page_bits when the previous generation page was dropped.
    //
    BATT_CHECK_EQ((old_value >> bit_offset) & u64{0b11}, 0);
  }

  void NoOutgoingRefsCache::clear_page_bits(page_device_id_int device_id, i64 physical_page_id)
  {
    u64 cache_index = this->compute_cache_index(device_id, physical_page_id);
    BATT_CHECK_LT(cache_index, this->cache_.size());
    u64 bit_offset = this->compute_bit_offset(physical_page_id);

    u64 mask = ~(u64{0b11} << bit_offset);
    this->cache_[cache_index].fetch_and(mask, std::memory_order_acq_rel);
  }

  u64 NoOutgoingRefsCache::get_page_bits(page_device_id_int device_id, i64 physical_page_id)
  {
    u64 cache_index = this->compute_cache_index(device_id, physical_page_id);
    BATT_CHECK_LT(cache_index, this->cache_.size());
    u64 bit_offset = this->compute_bit_offset(physical_page_id);

    return (this->cache_[cache_index].load(std::memory_order_acquire) >> bit_offset) & u64{0b11};
  }

} // namespace llfs
