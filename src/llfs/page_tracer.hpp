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
#include <unordered_set>
#include <vector>

namespace llfs {
enum struct OutgoingRefsStatus : u8 {
  kNotTraced = 0,        // bit state 00, invalid
  kHasOutgoingRefs = 2,  // bit state 10, valid with outgoing refs
  kNoOutgoingRefs = 3,   // bit state 11, valid with no outgoing refs
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An interface representing an entity that has the capability to trace and store data
 * regarding the outgoing reference counts of pages. How the outgoing refs info is stored is left up
 * to the implementer.
 */
class PageTracer
{
 public:
  PageTracer(const PageTracer&) = delete;
  PageTracer& operator=(const PageTracer&) = delete;

  virtual ~PageTracer() = default;

  // TODO [vsilai 2024-09-28] Get rid of trace_refs_recursive.hpp?
  //----- --- -- -  -  -   -
  /** \brief Recursively traces pages using a depth-first search starting with a given set of root
   * pages.
   *
   * \param loader The PageLoader used to retrieve PinnedPages. It is only used if a page has
   * outgoing refs that need to be traced, or if we don't know the status of a pge's outgoing refs.
   *
   * \param roots The set of root pages from which the depth-first search starts.
   *
   * \param recursion_pred The recursion predicate function which should take in a PageId and return
   * whter or not we should attempt to recursively trace it. When a page is traced from the outgoing
   * refs of another page, we call this function.
   *
   * \param traced_page_fn The function applied to a page that is traced from the outgoing
   * refs of another page.
   */
  virtual batt::Status trace_refs_recursively(
      PageLoader& loader, batt::BoxedSeq<PageId>&& roots,
      std::function<bool(PageId)>&& recursion_pred,
      std::function<void(PageId)>&& traced_page_fn) noexcept;

  //----- --- -- -  -  -   -
  /** \brief Sets the the outgoing refs information for the given page with id `page_id`.
   */
  virtual void set_outgoing_refs_info(PageId page_id, OutgoingRefsStatus has_out_going_refs) = 0;

  //----- --- -- -  -  -   -
  /** \brief Clears the the outgoing refs information for the given page with id `page_id`.
   */
  virtual void clear_outgoing_refs_info(PageId page_id) = 0;

  //----- --- -- -  -  -   -
  /** \brief Retrieves the outgoing refs information for the given page with id `page_id`.
   *
   * \return Returns one of three possible OutgoingRefsStatus values.
   */
  virtual OutgoingRefsStatus get_outgoing_refs_info(PageId page_id) const = 0;

  //----- --- -- -  -  -   -
  /** \brief Determines whether or not the given page with id `page_id` has no outgoing refs.
   */
  virtual bool has_no_outgoing_refs(PageId page_id) const
  {
    return this->get_outgoing_refs_info(page_id) == OutgoingRefsStatus::kNoOutgoingRefs;
  }

 protected:
  PageTracer() = default;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A cache to store outgoing refs information. Can be used by an implementer of PageTracer
 * as a way to organize and look up this information on the PageDevice level. This cache is
 * implemented as a bit vector, where every 2 bits in each 64-bit element of the vector represents
 * the outgoing refs state of a physical page in a PageDevice. The lower bit in a pair of "state
 * bits" represents if the page has no outgoing refs. The higher of the two bits represents the
 * validity of the page's state.
 */
class NoOutgoingRefsCache
{
 public:
  explicit NoOutgoingRefsCache(u64 physical_page_count) noexcept;

  NoOutgoingRefsCache(const NoOutgoingRefsCache&) = delete;
  NoOutgoingRefsCache& operator=(const NoOutgoingRefsCache&) = delete;

  //----- --- -- -  -  -   -
  /** \brief Sets the two state bits for the given `physical_page_id` based on whether the page has
   * outgoing refs or not, as indicated by `has_outgoing_refs`.
   */
  void set_page_bits(i64 physical_page_id, OutgoingRefsStatus has_out_going_refs);

  //----- --- -- -  -  -   -
  /** \brief Clears the two state bits for the given `physical_page_id`.
   */
  void clear_page_bits(i64 physical_page_id);

  //----- --- -- -  -  -   -
  /** \brief Retrieves the two state bits for the given `physical_page_id`.
   *
   * \return Can return a value of 0 (00), 2 (10), or 3 (11).
   */
  u64 get_page_bits(i64 physical_page_id) const;

 private:
  //----- --- -- -  -  -   -
  /** \brief Computes the index into the cache vector and the starting offset for the two state bits
   * in the 64-bit cache vector element for the given `physical_page_id`.
   *
   * \return A pair, where the first element is the index into the cache_ vector, and the second
   * element is the bit offset into that vector element. The bit offset is always a value between 0
   * and 63, inclusive.
   */
  std::pair<u64, u8> compute_cache_index_and_bit_offset(i64 physical_page_id) const
  {
    const u64 cache_index = physical_page_id / this->pages_per_index_;
    BATT_CHECK_LT(cache_index, this->cache_.size());
    return std::make_pair(cache_index, (physical_page_id % this->pages_per_index_) * 2);
  }

  std::vector<std::atomic<u64>> cache_;
  static constexpr u64 bit_mask_ = 0b11;
  static constexpr u8 pages_per_index_ = 32;
};

}  // namespace llfs

#endif  // LLFS_PAGE_TRACER_HPP
