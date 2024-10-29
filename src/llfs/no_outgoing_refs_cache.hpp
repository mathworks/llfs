//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_NO_OUTGOING_REFS_CACHE_HPP
#define LLFS_NO_OUTGOING_REFS_CACHE_HPP

#include <llfs/api_types.hpp>
#include <llfs/page_id_factory.hpp>

#include <batteries/bool_status.hpp>

#include <atomic>
#include <vector>

namespace llfs {
enum struct OutgoingRefsStatus : u8 {
  kNotTraced = 0,        // bit state 00, invalid
  kHasOutgoingRefs = 2,  // bit state 10, valid with outgoing refs
  kNoOutgoingRefs = 3,   // bit state 11, valid with no outgoing refs
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A cache to store information about a page's outgoing references to other pages. Can be
 * used by an implementer of PageTracer as a way to organize and look up this information on the
 * PageDevice level. This cache is implemented as a vector of unsigned 64-bit integers, where every
 * element of the vector represents the outgoing refs "state" of a physical page in a PageDevice.
 * The lowest bit in an element represents if the page has no outgoing refs. The second lowest bit
 * represents the validity of the page's state to help determine if a page's outgoing refs have ever
 * been traced. The remaining upper 62 bits are used to store the generation of the physical page.
 */
class NoOutgoingRefsCache
{
 public:
  explicit NoOutgoingRefsCache(const PageIdFactory& page_ids) noexcept;

  NoOutgoingRefsCache(const NoOutgoingRefsCache&) = delete;
  NoOutgoingRefsCache& operator=(const NoOutgoingRefsCache&) = delete;

  //----- --- -- -  -  -   -
  /** \brief Sets the two outgoing refs state bits for the given `page_id` based on
   * whether the page has outgoing refs or not, as indicated by `new_has_outgoing_refs_state`. This
   * function also updates the generation stored in the cache for the physical page associated with
   * `page_id`.
   *
   * @param page_id The id of the page whose outgoing refs information we are storing.
   *
   * @param new_has_outgoing_refs_state The status to store, indicating whether or not the page has
   * outgoing refs. A value of `kFalse` indicates that the page does not have any outgoing refs, and
   * a value of `kTrue` indicates that a page does have outgoing refs.
   */
  void set_page_state(PageId page_id, batt::BoolStatus new_has_outgoing_refs_state) noexcept;

  //----- --- -- -  -  -   -
  /** \brief Queries for whether or not the page with id `page_id` contains outgoing references to
   * other pages.
   *
   * \return Returns a `batt::BoolStatus` type, where `kFalse` indicates that the page has no
   * outgoing refs, `kTrue` indicates that the page does have outgoing refs, and `kUnknown`
   * indicates that the cache entry for the given page does not have any valid information stored in
   * it currently.
   */
  batt::BoolStatus has_outgoing_refs(PageId page_id) const noexcept;

 private:
  /** \brief A mask to retrieve the two outgoing refs state bits.
   */
  static constexpr u64 kOutgoingRefsBitMask = 0b11;

  /** \brief A mask to access the "valid" bit of the two outgoing refs state bits.
   */
  static constexpr u64 kValidBitMask = 0b10;

  /** \brief A constant representing the number of bits to shift a cache entry in order retrieve the
   * generation stored.
   */
  static constexpr u8 kGenerationShift = 2;

  /** \brief A PageIdFactory object to help resolve physical page and generation values from a
   * PageId object.
   */
  const PageIdFactory page_ids_;

  /** \brief The vector storing the cache entries, indexed by physical page id.
   */
  std::vector<std::atomic<u64>> cache_;
};
}  // namespace llfs

#endif  // LLFS_NO_OUTGOING_REFS_CACHE_HPP