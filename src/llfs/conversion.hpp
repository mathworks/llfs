//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CONVERSION_HPP
#define LLFS_CONVERSION_HPP

#include <llfs/api_types.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

constexpr inline BitCount to_bits(BitCount bits)
{
  return bits;
}

constexpr inline ByteCount to_bytes(BitCount bits)
{
  return ByteCount{(bits + 7) / 8};
}

constexpr inline Word64Count to_word64(BitCount bits)
{
  return Word64Count{(bits + 63) / 64};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

constexpr inline BitCount to_bits(ByteCount bytes)
{
  return BitCount{bytes * 8};
}

constexpr inline ByteCount to_bytes(ByteCount bytes)
{
  return bytes;
}

constexpr inline Word64Count to_word64(ByteCount bytes)
{
  return Word64Count{(bytes + 7) / 8};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

constexpr inline BitCount to_bits(Word64Count words)
{
  return BitCount{words * 64};
}

constexpr inline ByteCount to_bytes(Word64Count words)
{
  return ByteCount{words * 8};
}

constexpr inline Word64Count to_word64(Word64Count words)
{
  return words;
}

}  // namespace llfs

#endif  // LLFS_CONVERSION_HPP
