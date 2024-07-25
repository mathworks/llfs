//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_CONFIG_HPP
#define LLFS_VOLUME_CONFIG_HPP

#include <llfs/constants.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/int_types.hpp>
#include <llfs/log_device_config.hpp>
#include <llfs/log_device_config2.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/page_size.hpp>
#include <llfs/storage_file_builder.hpp>
#include <llfs/volume.hpp>
#include <llfs/volume_options.hpp>
#include <llfs/volume_runtime_options.hpp>

#include <boost/uuid/uuid.hpp>

#include <batteries/static_assert.hpp>

#include <variant>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Forward-declarations
//
struct PackedVolumeConfig;
struct VolumeConfigOptions;

// Add a PackedVolumeConfig to the passed transaction, according to the options specified.
// Applications should not call this function directly; instead, use
// `StorageFileBuilder::add_object(VolumeConfigOptions)`.
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedVolumeConfig&> p_config,
                                const VolumeConfigOptions& options);

// Recover a Volume from the passed storage context.  Applications should not call this function
// directly; instead use `StorageContext::recover_object`.
//
StatusOr<std::unique_ptr<Volume>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context, const std::string file_name,
    const FileOffsetPtr<const PackedVolumeConfig&>& p_volume_config,
    VolumeRuntimeOptions&& volume_runtime_options);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct VolumeConfigOptions {
  using PackedConfigType = PackedVolumeConfig;

  // Base options.
  //
  VolumeOptions base;

  // Options controlling the creation of the root log (WAL).
  //
  std::variant<LogDeviceConfigOptions, LogDeviceConfigOptions2> root_log;

  // Used to calculate the minimum recycler log size.
  //
  Optional<PageCount> recycler_max_buffered_page_count;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeConfig : PackedConfigSlotHeader {
  static constexpr usize kSize = PackedConfigSlot::kSize * 2;

  // The max branching factor for pages referenced by this volume.
  //
  little_u64 max_refs_per_page;

  // The root log configuration.
  //
  boost::uuids::uuid root_log_uuid;

  // The recycler log configuration.
  //
  boost::uuids::uuid recycler_log_uuid;

  u8 pad0_[4];

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  PackedConfigSlotBase slot_1;

  // The average number of bytes between WAL trims.
  //
  little_u64 trim_lock_update_interval_bytes;

  // The number of bytes by which to delay log trimming.
  //
  little_u64 trim_delay_byte_count;

  // Human-readable volume name - UTF-8 encoding
  //
  PackedBytes name;

  // Reserved for future use.
  //
  u8 pad1_[36];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeConfig), PackedVolumeConfig::kSize);
BATT_STATIC_ASSERT_EQ(offsetof(PackedVolumeConfig, slot_1), PackedConfigSlot::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedVolumeConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kVolume;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedVolumeConfig& t);

}  // namespace llfs

#endif  // LLFS_VOLUME_CONFIG_HPP
