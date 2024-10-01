//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_TRACER_HPP
#define LLFS_PAGE_TRACER_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/seq.hpp>
#include <batteries/status.hpp>
#include <batteries/utility.hpp>

#include <atomic>
#include <functional>
#include <vector>
#include <unordered_set>

namespace llfs {
// TODO [vsilai 2024-09-29] Go through all functions in all files to const correctness, check reference/rvalue stuff
enum struct OutgoingRefsStatus : u8 {
  kNotTraced = 0,
  kHasOutgoingRefs = 2,
  kNoOutgoingRefs = 3,
};

class NoOutgoingRefsCache
{
  public:
    explicit NoOutgoingRefsCache(const std::vector<u64>& device_capacities) noexcept;

    NoOutgoingRefsCache(const NoOutgoingRefsCache&) = delete;
    NoOutgoingRefsCache& operator=(const NoOutgoingRefsCache&) = delete;

    void set_page_bits(page_device_id_int device_id, i64 physical_page_id, OutgoingRefsStatus has_out_going_refs);

    void clear_page_bits(page_device_id_int device_id, i64 physical_page_id);

    u64 get_page_bits(page_device_id_int device_id, i64 physical_page_id);

  private:
    u64 compute_cache_index(page_device_id_int device_id, i64 physical_page_id);

    u64 compute_bit_offset(i64 physical_page_id);

    std::vector<std::atomic<u64>> cache_;
    std::vector<u64> base_device_indices_;
    static constexpr u8 pages_per_index_ = 32;
};

class PageTracer
{
  public:
    PageTracer(const PageTracer&) = delete;
    PageTracer& operator=(const PageTracer&) = delete;

    virtual ~PageTracer() = default;

    // TODO [vsilai 2024-09-28] Get rid of trace_refs_recursive.hpp?
    virtual batt::Status trace_refs_recursively(PageLoader& loader, batt::BoxedSeq<PageId>&& roots, std::function<bool(PageId)> &&recursion_pred, std::function<void(PageId)> &&traced_page_fn);

    virtual void set_outgoing_refs_info(PageId page_id, OutgoingRefsStatus has_out_going_refs) = 0;

    virtual void clear_outgoing_refs_info(PageId page_id) = 0;

    virtual OutgoingRefsStatus get_outgoing_refs_info(PageId page_id) = 0;

    virtual bool has_no_outgoing_refs(PageId page_id)
    {
      return this->get_outgoing_refs_info(page_id) == OutgoingRefsStatus::kNoOutgoingRefs;
    }

  protected:
    PageTracer() = default;
};

} // namespace llfs

#endif // LLFS_PAGE_TRACER_HPP
