//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TRACE_REFS_RECURSIVE_HPP
#define LLFS_TRACE_REFS_RECURSIVE_HPP

#include <llfs/page_id.hpp>
#include <llfs/page_tracer.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/seq.hpp>
#include <batteries/status.hpp>
#include <batteries/utility.hpp>

#include <type_traits>
#include <unordered_set>
#include <vector>

namespace llfs {

template <typename IdTypePairSeq, typename Pred, typename Fn>
inline batt::Status trace_refs_recursive(PageTracer& page_tracer, IdTypePairSeq&& roots,
                                         Pred&& should_recursively_trace, Fn&& fn)
{
  static_assert(std::is_convertible_v<std::decay_t<SeqItem<IdTypePairSeq>>, PageId>,
                "`roots` arg must be a Seq of PageViewId");

  std::unordered_set<PageId, PageId::Hash> pushed;
  std::vector<PageId> pending;

  BATT_FORWARD(roots) | seq::emplace_back(&pending);

  for (const PageId& page_id : pending) {
    pushed.insert(page_id);
  }

  while (!pending.empty()) {
    const PageId next = pending.back();
    pending.pop_back();

    batt::StatusOr<batt::BoxedSeq<PageId>> outgoing_refs_status = page_tracer.trace_page_refs(next);
    BATT_REQUIRE_OK(outgoing_refs_status);

    *outgoing_refs_status | seq::for_each([&](const PageId& id) {
      fn(id);
      if (!pushed.count(id) && should_recursively_trace(id)) {
        pushed.insert(id);
        pending.push_back(id);
      }
    });
  }

  return batt::OkStatus();
}

}  // namespace llfs

#endif  // LLFS_TRACE_REFS_RECURSIVE_HPP
