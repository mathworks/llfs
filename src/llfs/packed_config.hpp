//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_CONFIG_HPP
#define LLFS_PACKED_CONFIG_HPP

#include <llfs/crc.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/version.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/suppress.hpp>

#include <boost/uuid/uuid.hpp>

namespace llfs {

struct PackedConfigSlotBase {
  struct Tag {
    static constexpr u16 kNone = 0;           //
    static constexpr u16 kPageArena = 1;      // PackedPageArenaConfig, <llfs/page_arena_config.hpp>
    static constexpr u16 kVolume = 2;         //
    static constexpr u16 kLogDevice = 3;      //
    static constexpr u16 kPageDevice = 4;     //
    static constexpr u16 kPageAllocator = 5;  //
  };

  // One of the values above; identifies the type of this slot.
  //
  little_u16 tag;

  // Which slot, if this is a multi-slot config.
  //
  little_u8 slot_i;

  // How many slots large is this config; 0 == it is not a multi-slot config (so 1 == 0 in this
  // case).
  //
  little_u8 n_slots;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigSlotBase), 4);

struct PackedConfigSlotHeader : PackedConfigSlotBase {
  // The globally unique identifier of this object.
  //
  boost::uuids::uuid uuid;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigSlotHeader), 20);

struct PackedConfigSlot : PackedConfigSlotHeader {
  static constexpr usize kSize = 64;

  little_u8 reserved1_[kSize - sizeof(PackedConfigSlotHeader)];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigSlot), PackedConfigSlot::kSize);

template <typename T>
struct PackedConfigTagFor;

struct alignas(512) PackedConfigBlock {
  static constexpr u64 kMagic = 0x49dcc2d0e14dbe9eull;

  static constexpr u32 kVersion = make_version_u64(0, 1, 0);

  static constexpr usize kSize = 4096;

  static constexpr usize kPayloadCapacity =
      kSize - (64 /*offsetof(payload)*/ + 8 /*sizeof(crc64)*/);

  static constexpr i64 kNullFileOffset = 0x7fffffffffffll;

  struct Flag {
    // TODO [tastolfi 2022-02-16]  define some flags...
  };

  // byte 0 +++++++++++-+-+--+----- --- -- -  -  -   -

  // Must always be PackedPageArenaConfig::kMagic;
  //
  big_u64 magic;

  // The version of this config structure: (from msb to lsb): major(32bit) minor(16bit) rev(16bit).
  //
  big_u64 version;

  // The start of the prev config block relative to this one, or `kNullFileOffset` if there is none.
  //
  little_i64 prev_offset;

  // The start of the next config block relative to this one, or `kNullFileOffset` if there is none.
  //
  little_i64 next_offset;

  // See above.
  //
  little_u64 flags;

  // Reserved for future use.
  //
  little_u8 pad0_[16];

  // Configuration data.
  //
  PackedArray<PackedConfigSlot> slots;

  // byte 64 +++++++++++-+-+--+----- --- -- -  -  -   -

  // This area is used for slot data and any strings or dynamic parts they might need.
  //
  little_u8 payload[kPayloadCapacity];

  // The crc64 of this struct, excluding this field.
  //
  little_u64 crc64;

  // Returns the true CRC64 of this structure.
  //
  u64 true_crc64() const
  {
    auto crc64 = make_crc64();
    crc64.process_bytes(this, sizeof(PackedConfigBlock) - sizeof(little_u64));
    return crc64.checksum();
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigBlock), PackedConfigBlock::kSize);

BATT_SUPPRESS_IF_GCC("-Winvalid-offsetof")

BATT_STATIC_ASSERT_EQ(offsetof(PackedConfigBlock, crc64) + sizeof(little_u64),
                      PackedConfigBlock::kSize);

BATT_UNSUPPRESS_IF_GCC()

}  // namespace llfs

#endif  // LLFS_PACKED_CONFIG_HPP
