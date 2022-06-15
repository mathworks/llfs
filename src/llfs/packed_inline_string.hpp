//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_INLINE_STRING_HPP
#define LLFS_PACKED_INLINE_STRING_HPP

#include <llfs/int_types.hpp>

#include <ostream>
#include <string_view>

namespace llfs {

template <u32 kMaxLength>
struct PackedInlineString {
  little_u32 length;
  u8 chars[kMaxLength];

  PackedInlineString& operator=(std::string_view s)
  {
    this->length = std::min<usize>(kMaxLength, s.size());
    std::memcpy(this->chars, s.data(), this->length);
    return *this;
  }

  std::string_view as_str() const
  {
    return std::string_view{(const char*)this->chars, this->length};
  }
};

template <u32 kMaxLength>
inline std::ostream& operator<<(std::ostream& out, const PackedInlineString<kMaxLength>& t)
{
  return out << t.as_str();
}

}  // namespace llfs

#endif  // LLFS_PACKED_INLINE_STRING_HPP
