//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_CONFIG_HPP
#define LLFS_PAGE_ALLOCATOR_CONFIG_HPP

#include <llfs/int_types.hpp>
#include <llfs/log_device_config.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/page_allocator_runtime_options.hpp>
#include <llfs/page_size.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>

namespace llfs {

struct PackedPageAllocatorConfig;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageAllocatorConfigOptions {
  using PackedConfigType = PackedPageAllocatorConfig;

  // The unique identifier for this allocator; if None, a random UUID is generated.
  //
  Optional<boost::uuids::uuid> uuid;

  // The maximum number of clients that can make changes to page ref counts.
  //
  u64 max_attachments;

  // The number of pages tracked by this allocator.
  //
  PageCount page_count;

  // Options for the log device that stores the state of this page allocator.
  //
  LogDeviceConfigOptions log_device;

  // (Sanity check) The page size (log2) of the page device managed by this allocator.
  //
  PageSizeLog2 page_size_log2;

  // The device sequence number.
  //
  Optional<u32> page_device_id;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedPageAllocatorConfig {
  static constexpr usize kSize = PackedConfigSlot::kSize;

  // Must be set to PackedConfigSlot::Tag::kPageAllocator.
  //
  little_u16 tag;

  // Reserved for future use.
  //
  little_u8 pad0_[2];

  // Unique identifier for this page allocator.
  //
  boost::uuids::uuid uuid;

  // The maximum number of attachments allowed.
  //
  little_u64 max_attachments;

  // The page count of the page device managed by this allocator.
  //
  little_i64 page_count;

  // The PageAllocator log config.
  //
  boost::uuids::uuid log_device_uuid;

  // The page size (log2) of the page device managed by this allocator (for sanity checking).
  //
  little_u16 page_size_log2;

  // Reserved for future use.
  //
  little_u8 pad1_[2];

  // The device sequence number for the page device managed by this allocator.
  //
  little_u32 page_device_id;

  // Reserved for future use.
  //
  little_u8 pad2_[4];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageAllocatorConfig), PackedPageAllocatorConfig::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedPageAllocatorConfig> {
  static constexpr u16 value = PackedConfigSlot::Tag::kPageAllocator;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorConfig& t);

Status configure_storage_object(StorageFileBuilder::Transaction&,
                                FileOffsetPtr<PackedPageAllocatorConfig&> p_config,
                                const PageAllocatorConfigOptions& options);

class PageAllocator;

StatusOr<std::unique_ptr<PageAllocator>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context,           //
    const std::string& file_name,                                     //
    const FileOffsetPtr<const PackedPageAllocatorConfig&>& p_config,  //
    const PageAllocatorRuntimeOptions& options,                       //
    const IoRingLogDriverOptions& log_options);

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_CONFIG_HPP
