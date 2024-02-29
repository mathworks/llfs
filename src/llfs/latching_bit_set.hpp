//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_LATCHING_BIT_SET_HPP
#define LLFS_LATCHING_BIT_SET_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>

#include <batteries/math.hpp>
#include <batteries/small_vec.hpp>

namespace llfs {

/** \brief A lock-free concurrent set of integers with a fixed upper bound, which supports insertion
 * but not erasure of items.
 */
class LatchingBitSet
{
 public:
  /** \brief Creates a new set with space for `n_bits` bits, allowing integers from 0 to n_bits-1 to
   * be inserted.
   */
  explicit LatchingBitSet(usize n_bits) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns one past the maximum integer that can be inserted into the set.
   *
   * Note: this is a property of the container that is fixed at construction time; it does not
   * report the current actual largest value in the set.
   */
  usize upper_bound() const noexcept;

  /** \brief Returns the index of the first unset (0) bit.
   */
  usize first_missing() const noexcept;

  /** \brief Returns true iff the set contains the given integer.
   *
   * \param i The integer to test for; MUST be less than this->upper_bound(), or we PANIC.
   */
  bool contains(usize i) noexcept;

  /** \brief Inserts the passed integer `i`, returning true iff it was not previously contained by
   * the set.
   */
  bool insert(usize i) noexcept;

  /** \brief Returns true iff all integers from 0 to this->upper_bound() - 1 (inclusive) are present
   * in the set.
   */
  bool is_full() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Returns the backing storage memory as an array of std::atomic<u64>.
   */
  std::atomic<u64>* data() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The number of bits stored in this set.
   */
  usize upper_bound_;

  /** \brief Precomputed/cached starts of the levels of the data structure, in index offsets into
   * this->data_; level 0 is the flat array of all per-integer bits, the next level is a 1/64-sized
   * summary of those (where a 1 bit means all 64 corresponding bits from the previous level are
   * also 1; 0 otherwise); the last level is always a single u64.
   */
  batt::SmallVec<usize, 12> start_of_level_;

  /** \brief The backing memory for the bit set.  This is allocated as u64 and then cast to
   * std::atomic<u64> so we can speed up initialization by just memset-ing the array to all zeros.
   */
  std::unique_ptr<u64[]> data_;
};

}  //namespace llfs

#endif  // LLFS_LATCHING_BIT_SET_HPP
