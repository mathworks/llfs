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

#include <batteries/static_assert.hpp>

#include <limits>
#include <tuple>

namespace llfs {

inline constexpr usize packed_sizeof_varint(u64 n)
{
  return (n == 0) ? 1 : (64 - __builtin_clzll(n) + 6) / 7;
}

inline constexpr usize max_packed_sizeof_varint(u8 bits)
{
  // To handle the case of bits=64, shift up by one fewer bits, then shift up and set the final bit
  // (otherwise we may get a compiler warning that we are shifting by more than the size of the
  // integer).
  //
  return packed_sizeof_varint((((u64{1} << (bits - 1)) - 1) << 1) | 1);
}

constexpr usize kMaxVarInt8Size = 2;
BATT_STATIC_ASSERT_EQ(kMaxVarInt8Size, packed_sizeof_varint(std::numeric_limits<u8>::max()));
BATT_STATIC_ASSERT_EQ(kMaxVarInt8Size, max_packed_sizeof_varint(8));

constexpr usize kMaxVarInt16Size = 3;
BATT_STATIC_ASSERT_EQ(kMaxVarInt16Size, packed_sizeof_varint(std::numeric_limits<u16>::max()));
BATT_STATIC_ASSERT_EQ(kMaxVarInt16Size, max_packed_sizeof_varint(16));

constexpr usize kMaxVarInt24Size = 4;
BATT_STATIC_ASSERT_EQ(kMaxVarInt24Size, packed_sizeof_varint((u64{1} << 24) - 1));
BATT_STATIC_ASSERT_EQ(kMaxVarInt24Size, max_packed_sizeof_varint(24));

constexpr usize kMaxVarInt32Size = 5;
BATT_STATIC_ASSERT_EQ(kMaxVarInt32Size, packed_sizeof_varint(std::numeric_limits<u32>::max()));
BATT_STATIC_ASSERT_EQ(kMaxVarInt32Size, max_packed_sizeof_varint(32));

constexpr usize kMaxVarInt64Size = 10;
BATT_STATIC_ASSERT_EQ(kMaxVarInt64Size, packed_sizeof_varint(std::numeric_limits<u64>::max()));
BATT_STATIC_ASSERT_EQ(kMaxVarInt64Size, max_packed_sizeof_varint(64));

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
