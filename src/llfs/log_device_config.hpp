//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_LOG_DEVICE_CONFIG_HPP
#define LLFS_PACKED_LOG_DEVICE_CONFIG_HPP

#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_driver_options.hpp>
#include <llfs/log_device.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/storage_context.hpp>
#include <llfs/storage_file_builder.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>

namespace llfs {

struct PackedLogDeviceConfig;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct LogDeviceConfigOptions {
  using PackedConfigType = PackedLogDeviceConfig;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u64 kDefaultBlockSizeBytes = 16384;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The unique identifier for the log; if None, a random UUID will be generated.
  //
  Optional<boost::uuids::uuid> uuid;

  // log2 of the number of 4kib (memory) pages per flush block.
  // Higher values == higher (better) throughput, higher (worse) latency.
  //
  Optional<u16> pages_per_block_log2;

  // The capacity in bytes of the log.
  //
  usize log_size;

  // +++++++++++-+-+--+----- --- -- -  -  -   -

  // Returns the log flush block size in bytes.
  //
  u64 block_size() const
  {
    static const u64 kDefaultPagesPerBlockLog2 =
        batt::log2_ceil(kDefaultBlockSizeBytes / kLogPageSize);

    return u64{1} << this->pages_per_block_log2.value_or(kDefaultPagesPerBlockLog2);
  }

  // Sets the log flush block size in bytes; `n` must be a power of 2, >= kLogPageSize
  // (default=4096).
  //
  void block_size(u64 n_bytes)
  {
    const u64 n_pages = n_bytes / kLogPageSize;
    this->pages_per_block_log2 = batt::log2_ceil(n_pages);

    BATT_CHECK_EQ((u64{1} << *this->pages_per_block_log2) * kLogPageSize, n_bytes)
        << BATT_INSPECT(n_bytes) << BATT_INSPECT(n_pages) << BATT_INSPECT(kLogPageSize);
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLogDeviceConfig {
  static constexpr usize kSize = PackedConfigSlot::kSize;

  // Must be PackedConfigSlot::Tag::kLogDevice.
  //
  little_u16 tag;

  // Reserved for future use (set to 0 for now).
  //
  u8 pad0_[2];

  // The uuid for this log.
  //
  boost::uuids::uuid uuid;

  // The log2 of the number of 4096-byte pages per flush block.
  //
  little_u16 pages_per_block_log2;

  // The offset of flush block 0 relative to this structure.
  //
  little_i64 block_0_offset;

  // The total size of the log in bytes.
  //
  little_u64 physical_size;

  // The logical size of the log; this excludes all block headers.
  //
  little_u64 logical_size;

  // Reserved for future use (set to 0 for now).
  //
  u8 pad1_[18];

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize pages_per_block() const
  {
    return usize{1} << pages_per_block_log2;
  }

  usize block_size() const
  {
    return 4 * kKiB * this->pages_per_block();
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLogDeviceConfig), PackedLogDeviceConfig::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedLogDeviceConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kLogDevice;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedLogDeviceConfig& t);

Status configure_storage_object(StorageFileBuilder::Transaction&,
                                FileOffsetPtr<PackedLogDeviceConfig&> p_config,
                                const LogDeviceConfigOptions& options);

StatusOr<std::unique_ptr<LogDeviceFactory>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context, const std::string& file_name,
    const FileOffsetPtr<const PackedLogDeviceConfig&>& p_config,
    const IoRingLogDriverOptions& options);

}  // namespace llfs

#endif  // LLFS_PACKED_LOG_DEVICE_CONFIG_HPP
