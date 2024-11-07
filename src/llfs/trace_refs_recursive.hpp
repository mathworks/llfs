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

/** \brief Do a depth-first search of the page reference graph starting at roots.
 *
 * Starting at roots, calls page_tracer.trace_page_refs to find the outgoing references (edges).
 * These are filtered by `should_recursively_trace` before being added to a stack of pages to
 * recursively trace.  Continues until the stack is empty.
 *
 * \return OkStatus if successful, otherwise the first error status code returned by
 * page_tracer.trace_page_refs.
 */
template <typename IdTypePairSeq, typename Pred = bool(PageId), typename Fn = void(PageId)>
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

/** \brief Convenience overload; creates a LoadingPageTracer to wrap the page_loader arg, then calls
 * the PageTracer variant of trace_refs_recursive.
 */
template <typename IdTypePairSeq, typename Pred, typename Fn>
inline batt::Status trace_refs_recursive(PageLoader& page_loader, IdTypePairSeq&& roots,
                                         Pred&& should_recursively_trace, Fn&& fn)
{
  LoadingPageTracer tracer_impl{page_loader};

  return trace_refs_recursive(tracer_impl, BATT_FORWARD(roots),
                              BATT_FORWARD(should_recursively_trace), BATT_FORWARD(fn));
}

}  // namespace llfs

#endif  // LLFS_TRACE_REFS_RECURSIVE_HPP
