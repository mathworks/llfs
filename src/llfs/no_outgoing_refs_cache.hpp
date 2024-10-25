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

#include <atomic>
#include <vector>

namespace llfs {
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A cache to store outgoing refs information. Can be used by an implementer of PageTracer
 * as a way to organize and look up this information on the PageDevice level. This cache is
 * implemented as a vector of unsigned 64-bit integers, where every element of the vector
 * represents the outgoing refs state of a physical page in a PageDevice. The lowest bit in an
 * element represents if the page has no outgoing refs. The second lowest bit represents the
 * validity of the page's state. the remaining upper 62 bits is used to store the generation of
 * the physical page.
 */
class NoOutgoingRefsCache
{
 public:
  explicit NoOutgoingRefsCache(u64 physical_page_count) noexcept;

  NoOutgoingRefsCache(const NoOutgoingRefsCache&) = delete;
  NoOutgoingRefsCache& operator=(const NoOutgoingRefsCache&) = delete;

  //----- --- -- -  -  -   -
  /** \brief Sets the two outgoing refs state bits for the given `physical_page_id` based on
   * whether the page has outgoing refs or not, as indicated by `has_outgoing_refs`.
   */
  void set_page_bits(i64 physical_page_id, page_generation_int generation,
                     HasNoOutgoingRefs has_no_outgoing_refs);

  //----- --- -- -  -  -   -
  /** \brief Retrieves the two outgoing refs state bits for the given `physical_page_id`.
   *
   * \return Can return a value of 0 (00), 2 (10), or 3 (11).
   */
  u64 get_page_bits(i64 physical_page_id, page_generation_int generation) const;

 private:
  std::vector<std::atomic<u64>> cache_;
  static constexpr u64 bit_mask_ = 0b11;
  static constexpr u8 generation_shift_ = 2;
};
}  // namespace llfs

#endif // LLFS_NO_OUTGOING_REFS_CACHE_HPP