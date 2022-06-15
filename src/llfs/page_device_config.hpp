//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_DEVICE_CONFIG_HPP
#define LLFS_PAGE_DEVICE_CONFIG_HPP

#include <llfs/int_types.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/simple_packed_type.hpp>
#include <llfs/storage_file_builder.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>

#include <ostream>

namespace llfs {

struct PackedPageDeviceConfig;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageDeviceConfigOptions {
  using PackedConfigType = PackedPageDeviceConfig;

  // log2(the page size of the device)
  //
  PageSizeLog2 page_size_log2;

  // The number of pages in this device.
  //
  PageCount page_count;

  // The device id for this device; if None, this will be set to the lowest unused id in the storage
  // context where this device is configured.
  //
  Optional<page_device_id_int> device_id;

  // The unique identifier for the device; if None, a random UUID is generated.
  //
  Optional<boost::uuids::uuid> uuid;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedPageDeviceConfig {
  static constexpr usize kSize = PackedConfigSlot::kSize;

  // MUST be PackedObjectConfig::Tag::kPageDevice
  //
  little_u32 tag;

  // Reserved for future use.
  //
  little_u8 pad0_[4];

  // The offset in bytes of the first page, relative to this structure.
  //
  little_i64 page_0_offset;

  // The default short device_id for this device.
  //
  little_u64 device_id;

  // The number of pages addressable by this device.
  //
  little_i64 page_count;

  // The log2 of the page size in bytes.
  //
  little_u16 page_size_log2;

  // A UUID for this device.
  //
  boost::uuids::uuid uuid;

  // Reserved for future use.
  //
  little_u8 reserved_[14];

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize page_size() const
  {
    return usize{1} << this->page_size_log2.value();
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageDeviceConfig), PackedPageDeviceConfig::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedPageDeviceConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kPageDevice;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedPageDeviceConfig& t);

Status configure_storage_object(StorageFileBuilder::Transaction&,
                                FileOffsetPtr<PackedPageDeviceConfig&> p_config,
                                const PageDeviceConfigOptions& options);

}  // namespace llfs

#endif  // LLFS_PACKED_PAGE_DEVICE_CONFIG_HPP
