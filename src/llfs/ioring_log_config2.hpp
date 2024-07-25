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
#include <llfs/ioring_log_config2.hpp>

namespace llfs {

struct PackedLogDeviceConfig2;

struct IoRingLogConfig2 {
  static IoRingLogConfig2 from_packed(
      const FileOffsetPtr<const PackedLogDeviceConfig2&>& packed_config);

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
};

}  //namespace llfs

#endif  // LLFS_IORING_LOG_CONFIG2_HPP
