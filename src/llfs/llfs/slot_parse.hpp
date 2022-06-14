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
  SlotRange offset;
  std::string_view body;
  Optional<SlotRange> depends_on_offset;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const SlotParse& t);

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
