//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STRINGS_HPP
#define LLFS_STRINGS_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>

#include <string_view>

namespace llfs {

/** \brief Calculates the longest common prefix of the strings `a[skip_len:]` and `b[skip_len:]`.
 *
 * If `skip_len` is equal to or greater than the lengths of either input string, empty string is
 * return.
 *
 * Otherwise the returned std::string_view will be a prefix of `a`.
 */
std::string_view find_common_prefix(usize skip_len, const std::string_view& a,
                                    const std::string_view& b);

/** \brief Comparator between `char` and `string_view` that returns whether the byte at str[k]
 * is less than the passed char `ch` (or vice-versa, depending on the argument order).
 */
struct CompareKthByte {
  using result_type = bool;

  //----- --- -- -  -  -   -

  bool operator()(char ch, const std::string_view& str) const
  {
    if (this->k >= str.size()) {
      return false;
    }
    return ((u8)ch < (u8)str[this->k]);
  }

  bool operator()(const std::string_view& str, char ch) const
  {
    if (this->k >= str.size()) {
      return true;
    }
    return ((u8)str[this->k] < (u8)ch);
  }

  //----- --- -- -  -  -   -

  // The (0-based) index of the character to compare.
  //
  usize k;
};

}  //namespace llfs

#endif  // LLFS_STRINGS_HPP
