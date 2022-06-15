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
#include <llfs/packed_config.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/page_size.hpp>
#include <llfs/storage_file_builder.hpp>
#include <llfs/volume_options.hpp>

#include <boost/uuid/uuid.hpp>

#include <batteries/static_assert.hpp>

namespace llfs {

struct PackedVolumeConfig;
struct VolumeConfigOptions;

Status configure_storage_object(StorageFileBuilder::Transaction&,
                                FileOffsetPtr<PackedVolumeConfig&> p_config,
                                const VolumeConfigOptions& options);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct VolumeConfigOptions {
  using PackedConfigType = PackedVolumeConfig;

  // Base options.
  //
  VolumeOptions base;

  // The size of the root log (WAL).
  //
  usize root_log_size;

  // Where inside the file to place the volume.  Optional, defaults to 0.
  //
  Optional<i64> base_file_offset;

  // How much space to allocate within the recycler log for top-level (discovery depth==0) recycled
  // pages.  Optional; the recycler log size defaults to double the minimum size.
  //
  Optional<PageCount> recycler_max_buffered_page_count;

  // Advanced option.
  //
  Optional<u16> root_log_pages_per_block_log2;

  // Advanced option.
  //
  Optional<u16> recycler_log_pages_per_block_log2;
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

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  PackedConfigSlotBase slot_1;

  // Human-readable volume name - UTF-8 encoding
  //
  PackedBytes name;

  // Reserved for future use.
  //
  u8 pad1_[56];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeConfig), PackedVolumeConfig::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedVolumeConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kVolume;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedVolumeConfig& t);

}  // namespace llfs

#endif  // LLFS_VOLUME_CONFIG_HPP
