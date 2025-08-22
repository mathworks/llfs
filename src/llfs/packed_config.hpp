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

#include <llfs/config.hpp>
//
#include <llfs/crc.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_uuid.hpp>
#include <llfs/version.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/suppress.hpp>

#include <cstddef>

namespace llfs {

struct PackedConfigSlotBase {
  struct Tag {
    static constexpr u16 kNone = 0;           //
    static constexpr u16 kPageArena = 1;      // PackedPageArenaConfig, <llfs/page_arena_config.hpp>
    static constexpr u16 kVolume = 2;         //
    static constexpr u16 kLogDevice = 3;      //
    static constexpr u16 kPageDevice = 4;     //
    static constexpr u16 kPageAllocator = 5;  //
    static constexpr u16 kLogDevice2 = 6;     // Simpler, optimized version of kLogDevice

    // The range [0x1000..0x1fff] is reserved for continuation slots.

    static constexpr u16 kVolumeContinuation = 0x1000;

    static std::string_view to_string(u16 value);
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

  // Set whether or not this is the last object in an llfs file.
  //
  void set_last_in_file(bool last_in_file)
  {
    // We should never call this function unless it's been overriden
    //
    BATT_PANIC() << "Failed call to set_last_in_file on object "
                 << "Type PackedConfigSlotBase because this function has not been implemented. "
                 << BATT_INSPECT(last_in_file);
  }

  // Get whether or not this is the last object in an llfs file.
  //
  bool is_last_in_file() const
  {
    return false;
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigSlotBase), 4);

struct PackedConfigSlotHeader : PackedConfigSlotBase {
  // The globally unique identifier of this object.
  //
  PackedUUID uuid;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigSlotHeader), 20);

struct PackedConfigSlot : PackedConfigSlotHeader {
  static constexpr usize kSize = 64;

  little_u8 reserved1_[kSize - sizeof(PackedConfigSlotHeader)];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigSlot), PackedConfigSlot::kSize);

std::ostream& operator<<(std::ostream& out, const PackedConfigSlot& slot);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
struct PackedConfigTagFor;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct alignas(kDirectIOBlockAlign) PackedConfigBlock {
  static constexpr u64 kMagic = 0x49dcc2d0e14dbe9eull;

  static constexpr u32 kVersion = make_version_u64(0, 1, 0);

  static constexpr usize kSize = 4096;

  static constexpr i32 kSizeLog2 = 12;

  static_assert(1ull << kSizeLog2 == kSize, "Please fix kSizeLog2.");

  static constexpr usize kPayloadCapacity =
      kSize - (64 /*offsetof(payload)*/ + 8 /*sizeof(crc64)*/);

  static constexpr i64 kNullFileOffset = 0x7fffffffffffffffll;

  struct Flag {
    // TODO [tastolfi 2022-02-16]  define some flags...
  };

  static constexpr usize kMaxSlots = kPayloadCapacity / PackedConfigSlot::kSize;

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

  //----- --- -- -  -  -   -
  // IMPORTANT!  `slots` and `payload` fields MUST be adjacent, since `payload` contains the
  // elements of the `slots` array.
  //----- --- -- -  -  -   -

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

template <typename T>
inline void verify_config_slot_type_requirements(batt::StaticType<T> = {})
{
  BATT_STATIC_ASSERT_GE(sizeof(T), sizeof(::llfs::PackedConfigSlot));
  BATT_STATIC_ASSERT_EQ(sizeof(T) % sizeof(::llfs::PackedConfigSlot), 0);

#define LLFS_VERIFY_CONFIG_SLOT_FIELD(field_name)                                                  \
  BATT_STATIC_ASSERT_EQ(offsetof(T, field_name), offsetof(::llfs::PackedConfigSlot, field_name));  \
  BATT_STATIC_ASSERT_TYPE_EQ(decltype(T::field_name),                                              \
                             decltype(::llfs::PackedConfigSlot::field_name))

  LLFS_VERIFY_CONFIG_SLOT_FIELD(tag);
  LLFS_VERIFY_CONFIG_SLOT_FIELD(slot_i);
  LLFS_VERIFY_CONFIG_SLOT_FIELD(n_slots);
  LLFS_VERIFY_CONFIG_SLOT_FIELD(uuid);

#undef LLFS_VERIFY_CONFIG_SLOT_FIELD
}

// Cast the passed PackedConfigSlot to a derived config slot type, running static checks to make
// sure the destination type conforms to the proper requirements.
//
#define LLFS_CONFIG_SLOT_CAST_IMPL(qualifiers, ptr_ref)                                            \
  template <typename T>                                                                            \
  qualifiers T ptr_ref config_slot_cast(qualifiers ::llfs::PackedConfigSlot ptr_ref src)           \
  {                                                                                                \
    ::llfs::verify_config_slot_type_requirements<T>();                                             \
    return reinterpret_cast<qualifiers T ptr_ref>(src);                                            \
  }

LLFS_CONFIG_SLOT_CAST_IMPL(, *)
LLFS_CONFIG_SLOT_CAST_IMPL(, &)
LLFS_CONFIG_SLOT_CAST_IMPL(const, *)
LLFS_CONFIG_SLOT_CAST_IMPL(const, &)

#undef LLFS_CONFIG_SLOT_CAST_IMPL

}  // namespace llfs

#endif  // LLFS_PACKED_CONFIG_HPP
