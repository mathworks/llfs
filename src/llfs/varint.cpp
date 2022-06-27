//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/varint.hpp>
//

#include <batteries/assert.hpp>

namespace llfs {

namespace {
constexpr u64 kLowBitsMask = 0b01111111;
constexpr u8 kHighBitMask = 0b10000000;
}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u8* pack_varint_to(u8* first, u8* last, u64 n)
{
  const usize bytes_required = packed_sizeof_varint(n);
  if (static_cast<usize>(last - first) < bytes_required) {
    return nullptr;
  }

  switch (bytes_required) {
    case 10:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 9:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 8:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 7:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 6:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 5:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 4:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 3:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 2:
      *first = (n & kLowBitsMask) | kHighBitMask;
      ++first;
      n >>= 7;
      // fall-through
    case 1:
      *first = (n & kLowBitsMask);
      ++first;
      break;
    default:
      BATT_PANIC() << "bytes_required calculation was wrong! n=" << n
                   << " bytes_required=" << bytes_required;
      break;
  }

  return first;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::tuple<Optional<u64>, const u8*> unpack_varint_from(const u8* first, const u8* last)
{
  u64 n = 0;
  int shift = 0;
  for (;;) {
    if (first == last) {
      return {None, nullptr};
    }
    const u8 next_byte = *first;

    ++first;
    n |= (u64{next_byte} & kLowBitsMask) << shift;
    if (!(next_byte & kHighBitMask)) {
      break;
    }
    shift += 7;
  }
  return {{n}, first};
}

}  // namespace llfs
