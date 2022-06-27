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

#include <llfs/int_types.hpp>
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_allocator_config.hpp>
#include <llfs/page_arena.hpp>
#include <llfs/page_device_config.hpp>
#include <llfs/status.hpp>

#include <boost/uuid/uuid.hpp>

namespace llfs {

struct PageArenaConfigOptions {
  PageCount page_count;
  u16 page_size_bits;
  Optional<u16> log_block_size_bits;
  Optional<u64> device_id;
  Optional<boost::uuids::uuid> device_uuid;
  Optional<boost::uuids::uuid> log_uuid;
  Optional<boost::uuids::uuid> allocator_uuid;
  Optional<boost::uuids::uuid> arena_uuid;
  u64 max_attachments;

  u64 page_size() const noexcept
  {
    return u64{1} << this->page_size_bits;
  }

  void page_size(u64 n)
  {
    this->page_size_bits = batt::log2_ceil(n);
    BATT_CHECK_EQ(this->page_size(), n);
  }
};

struct PackedPageArenaConfig : PackedConfigSlotHeader {
  static constexpr usize kSize = 64;

  // byte 20 +++++++++++-+-+--+----- --- -- -  -  -   -

  // The PageDevice config.
  //
  boost::uuids::uuid page_device_uuid;

  // byte 36 +++++++++++-+-+--+----- --- -- -  -  -   -

  // The PageAllocator config.
  //
  boost::uuids::uuid page_allocator_uuid;

  // byte 52 +++++++++++-+-+--+----- --- -- -  -  -   -

  // Must be zero for now.
  //
  little_u8 reserved_[12];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageArenaConfig), PackedPageArenaConfig::kSize);
BATT_STATIC_ASSERT_EQ(PackedPageArenaConfig::kSize, PackedConfigSlot::kSize);

std::ostream& operator<<(std::ostream& out, const PackedPageArenaConfig& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedPageArenaConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kPageArena;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedPageArenaConfig&> p_config,
                                const PageArenaConfigOptions& options);

StatusOr<PageArena> recover_storage_object(                       //
    const batt::SharedPtr<StorageContext>& storage_context,       //
    const std::string& file_name,                                 //
    const FileOffsetPtr<const PackedPageArenaConfig&>& p_config,  //
    const PageAllocatorRuntimeOptions& allocator_options,         //
    const IoRingLogDriverOptions& allocator_log_options,          //
    const IoRingFileRuntimeOptions& page_device_file_options);

}  // namespace llfs

#endif  // LLFS_PAGE_ARENA_CONFIG_HPP
