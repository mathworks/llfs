//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_LOG_DEVICE_CONFIG2_HPP
#define LLFS_PACKED_LOG_DEVICE_CONFIG2_HPP

#include <llfs/config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_device2.hpp>
#include <llfs/log_device.hpp>
#include <llfs/log_device_runtime_options.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/storage_context.hpp>
#include <llfs/storage_file_builder.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>

#include <ostream>
#include <variant>

namespace llfs {

struct LogDeviceConfigOptions2;
struct PackedLogDeviceConfig2;

//+++++++++++-+-+--+----- --- -- -  -  -   -

Status configure_storage_object(StorageFileBuilder::Transaction&,
                                FileOffsetPtr<PackedLogDeviceConfig2&> p_config,
                                const LogDeviceConfigOptions2& options);

StatusOr<std::unique_ptr<IoRingLogDevice2Factory>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context, const std::string& file_name,
    const FileOffsetPtr<const PackedLogDeviceConfig2&>& p_config, LogDeviceRuntimeOptions options);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct LogDeviceConfigOptions2 {
  using PackedConfigType = PackedLogDeviceConfig2;

  static constexpr u16 kDefaultDevicePageSizeLog2 = 9 /*=log2(512)*/;
  static constexpr u16 kDefaultDataAlignmentLog2 = 12 /*=log2(4096)*/;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The unique identifier for the log; if None, a random UUID will be generated.
  //
  Optional<boost::uuids::uuid> uuid;

  // The capacity in bytes of the log.
  //
  usize log_size;

  Optional<u16> device_page_size_log2;
  Optional<u16> data_alignment_log2;
};

inline bool operator==(const LogDeviceConfigOptions2& l, const LogDeviceConfigOptions2& r)
{
  return l.uuid == r.uuid                                       //
         && l.log_size == r.log_size                            //
         && l.device_page_size_log2 == r.device_page_size_log2  //
         && l.data_alignment_log2 == r.data_alignment_log2;
}

inline bool operator!=(const LogDeviceConfigOptions2& l, const LogDeviceConfigOptions2& r)
{
  return !(l == r);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLogDeviceConfig2 : PackedConfigSlotHeader {
  static constexpr usize kSize = PackedConfigSlot::kSize;

  // The offset of flush block 0 relative to this structure.
  //
  little_i64 control_block_offset;

  // The capacity of the log.
  //
  little_u64 logical_size;

  // The log2 of the device page size where this log is stored.
  //
  little_u16 device_page_size_log2;

  // The log2 of the data alignment for the log.
  //
  little_u16 data_alignment_log2;

  // Reserved for future use (set to 0 for now).
  //
  u8 pad1_[24];

  //+++++++++++-+-+--+----- --- -- -  -  -   -
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLogDeviceConfig2), PackedLogDeviceConfig2::kSize);

std::ostream& operator<<(std::ostream& out, const PackedLogDeviceConfig2& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedLogDeviceConfig2> {
  static constexpr u32 value = PackedConfigSlot::Tag::kLogDevice2;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedLogDeviceConfig2& t);

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_PACKED_LOG_DEVICE_CONFIG_HPP
