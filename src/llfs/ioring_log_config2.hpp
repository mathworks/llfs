//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_CONFIG2_HPP
#define LLFS_IORING_LOG_CONFIG2_HPP

#include <llfs/config.hpp>
//

#include <llfs/file_offset_ptr.hpp>
#include <llfs/int_types.hpp>
#include <llfs/interval.hpp>

namespace llfs {

struct PackedLogDeviceConfig2;

struct IoRingLogConfig2 {
  using Self = IoRingLogConfig2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr usize kDefaultDevicePageSize = kDirectIOBlockSize;
  static constexpr usize kDefaultDataAlignment = kDirectIOBlockAlign;

  static constexpr u16 kDefaultDevicePageSizeLog2 = kDirectIOBlockSizeLog2;
  static constexpr u16 kDefaultDataAlignmentLog2 = kDirectIOBlockAlignLog2;

  static_assert((usize{1} << Self::kDefaultDevicePageSizeLog2) == Self::kDefaultDevicePageSize);
  static_assert((usize{1} << Self::kDefaultDataAlignmentLog2) == Self::kDefaultDataAlignment);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static IoRingLogConfig2 from_packed(
      const FileOffsetPtr<const PackedLogDeviceConfig2&>& packed_config);

  static IoRingLogConfig2 from_logical_size(u64 logical_size,
                                            Optional<u64> opt_device_page_size = None,
                                            Optional<u64> opt_data_alignment = None);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i64 control_block_offset;
  u64 log_capacity;
  u16 device_page_size_log2;
  u16 data_alignment_log2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i64 control_block_size() const noexcept
  {
    return i64{1} << this->data_alignment_log2;
  }

  Interval<i64> offset_range() const noexcept
  {
    return Interval<i64>{
        .lower_bound = this->control_block_offset,
        .upper_bound = this->control_block_offset + this->control_block_size() +
                       static_cast<i64>(this->log_capacity),
    };
  }
};

}  //namespace llfs

#endif  // LLFS_IORING_LOG_CONFIG2_HPP
