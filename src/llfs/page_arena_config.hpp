//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ARENA_CONFIG_HPP
#define LLFS_PAGE_ARENA_CONFIG_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/log_device_runtime_options.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/packed_uuid.hpp>
#include <llfs/page_allocator_config.hpp>
#include <llfs/page_arena.hpp>
#include <llfs/page_device_config.hpp>
#include <llfs/status.hpp>

namespace llfs {

struct PackedPageArenaConfig;
struct PageArenaConfigOptions;

//+++++++++++-+-+--+----- --- -- -  -  -   -

Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedPageArenaConfig&> p_config,
                                const PageArenaConfigOptions& options);

StatusOr<PageArena> recover_storage_object(                       //
    const batt::SharedPtr<StorageContext>& storage_context,       //
    const std::string& file_name,                                 //
    const FileOffsetPtr<const PackedPageArenaConfig&>& p_config,  //
    const PageAllocatorRuntimeOptions& allocator_options,         //
    const LogDeviceRuntimeOptions& allocator_log_options,         //
    const IoRingFileRuntimeOptions& page_device_file_options);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageArenaConfigOptions {
  using PackedConfigType = PackedPageArenaConfig;

  // The unique identifier for the arena; if None, a random UUID is generated.
  //
  Optional<PackedUUID> uuid;

  // Config options or uuid of the PageAllocator to link to this arena.
  //
  NestedPageAllocatorConfig page_allocator;

  // Config options or uuid of the PageDevice to link to this arena.
  //
  NestedPageDeviceConfig page_device;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedConfigSlotHeader), 20u);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedPageArenaConfig : PackedConfigSlotHeader {
  static constexpr usize kSize = PackedConfigSlot::kSize;

  // byte 20 +++++++++++-+-+--+----- --- -- -  -  -   -

  // The PageAllocator config.
  //
  PackedUUID page_allocator_uuid;

  // byte 36 +++++++++++-+-+--+----- --- -- -  -  -   -

  // The PageDevice config.
  //
  PackedUUID page_device_uuid;

  // byte 52 +++++++++++-+-+--+----- --- -- -  -  -   -

  // Must be zero for now.
  //
  little_u8 reserved_[12];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageArenaConfig), PackedPageArenaConfig::kSize);
BATT_STATIC_ASSERT_EQ(PackedPageArenaConfig::kSize, PackedConfigSlot::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedPageArenaConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kPageArena;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedPageArenaConfig& t);

}  // namespace llfs

#endif  // LLFS_PAGE_ARENA_CONFIG_HPP
