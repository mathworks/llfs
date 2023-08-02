//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLOT_PARSE_HPP
#define LLFS_SLOT_PARSE_HPP

#include <llfs/slot.hpp>

#include <batteries/stream_util.hpp>

#include <ostream>
#include <string_view>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct SlotParse {
  /** \brief The slot offset range of the parsed record.
   */
  SlotRange offset;

  /** \brief The slot payload data; this excludes only the var-int header portion.
   */
  std::string_view body;

  /** \brief If set, indicates a logical dependency on an earlier slot.
   */
  Optional<SlotRange> depends_on_offset;

  /** \brief The total grant size spent to append the logical entity represented by this parse. For
   * most records, this is just the number of bytes spanned by the slot (this->offset.size()); for
   * commit job events, it includes the `total_grant_spent` of the prepare job slot as well.
   */
  u64 total_grant_spent;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const SlotParse& t);

inline bool operator==(const SlotParse& l, const SlotParse& r)
{
  return l.offset == r.offset &&  //
         l.body == r.body &&      //
         l.depends_on_offset == r.depends_on_offset;
}

inline bool operator!=(const SlotParse& l, const SlotParse& r)
{
  return !(l == r);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
struct SlotParseWithPayload {
  SlotParse slot;
  T payload;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename T>
inline std::ostream& operator<<(std::ostream& out, const SlotParseWithPayload<T>& t)
{
  return out << "{.slot=" << t.slot << ", .payload=" << batt::make_printable(t.payload) << ",}";
}

}  // namespace llfs

#endif  // LLFS_SLOT_PARSE_HPP
