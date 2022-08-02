//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_LOG_PAGE_HEADER_HPP
#define LLFS_PACKED_LOG_PAGE_HEADER_HPP

#include <llfs/config.hpp>
#include <llfs/int_types.hpp>

#include <batteries/static_assert.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Used internally to frame on-disk pages of log data.
//
struct PackedLogPageHeader {
  static constexpr u64 kMagic = 0xc3392dfb394e0349ull;

  // Must be `kMagic`.
  //
  big_u64 magic;

  // The slot offset of the first payload byte in this block.
  //
  little_u64 slot_offset;

  // The number of valid bytes in the payload section of this block.
  //
  little_u64 commit_size;

  // The crc64 of this page, with this field set to 0.
  //
  little_u64 crc64;  // TODO [tastolfi 2021-06-16] implement me

  // The current last known trim pos for the log.
  //
  little_u64 trim_pos;

  // The current last known flush pos for the log.
  //
  little_u64 flush_pos;

  // The current last known commit pos for the log - even though this only refers to ephemeral
  // state, it can help accelerate recovery by skipping ahead to a known slot boundary.
  //
  little_u64 commit_pos;

  // Reserved for future use.
  //
  little_u8 reserved_[8];

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void reset(u64 slot_offset = 0) noexcept;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLogPageHeader), 64);

}  // namespace llfs

#endif  // LLFS_PACKED_LOG_PAGE_HEADER_HPP
