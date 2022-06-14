#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_CONFIG_HPP
#define LLFS_PAGE_ALLOCATOR_CONFIG_HPP

#include <llfs/int_types.hpp>
#include <llfs/log_device_config.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/page_size.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>

namespace llfs {

struct PackedPageAllocatorConfig;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageAllocatorConfigOptions {
  using PackedConfigType = PackedPageAllocatorConfig;

  // The number of pages tracked by this allocator.
  //
  PageCount page_count;

  // The maximum number of clients that can make changes to page ref counts.
  //
  u64 max_attachments;

  // The unique identifier for this allocator; if None, a random UUID is generated.
  //
  Optional<boost::uuids::uuid> uuid;

  // The unique identifier for the page allocator WAL; if None, a random UUID is generated.
  //
  Optional<boost::uuids::uuid> log_device_uuid;

  // Passed on when created the WAL; a new unique log device is always created when configuring a
  // page allocator; it's not allowed to assign an existing log device to a new allocator.
  //
  Optional<u16> log_device_pages_per_block_log2;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedPageAllocatorConfig {
  static constexpr usize kSize = PackedConfigSlot::kSize;

  // Must be set to PackedConfigSlot::Tag::kPageAllocator.
  //
  little_u32 tag;

  // Reserved for future use.
  //
  little_u8 pad0_[4];

  // The maximum number of attachments allowed.
  //
  little_u64 max_attachments;

  // Unique identifier for this page allocator.
  //
  boost::uuids::uuid uuid;

  // The PageAllocator log config.
  //
  PackedPointer<PackedLogDeviceConfig> log_device;

  // Reserved for future use.
  //
  little_u8 pad1_[28];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageAllocatorConfig), PackedPageAllocatorConfig::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedPageAllocatorConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kPageAllocator;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorConfig& t);

Status configure_storage_object(StorageFileBuilder::Transaction&,
                                FileOffsetPtr<PackedPageAllocatorConfig&> p_config,
                                const PageAllocatorConfigOptions& options);

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_CONFIG_HPP
