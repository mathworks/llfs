//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

// varint.hpp : variable-length encoded integers

#pragma once
#ifndef LLFS_VARINT_HPP
#define LLFS_VARINT_HPP

#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>

#include <tuple>

namespace llfs {

inline constexpr usize packed_sizeof_varint(u64 n)
{
  return (n == 0) ? 1 : (64 - __builtin_clzll(n) + 6) / 7;
}

constexpr usize kMaxVarInt32Size = 5;

// Packs the passed integer value `n` into the byte range specified by [first, last).  If there
// isn't enough space in the given destination range, then this function will return nullptr.
//
u8* pack_varint_to(u8* first, u8* last, u64 n);

// Attempts to decode a varint value from the given byte range.  If successful, this function will
// return the decoded integer as the first tuple element; else this element will be None.  The
// second element of the returned tuple will be a pointer to the byte after the last byte parsed as
// part of the varint.
//
std::tuple<Optional<u64>, const u8*> unpack_varint_from(const u8* first, const u8* last);

}  // namespace llfs

#endif  // LLFS_VARINT_HPP
