//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_config2.hpp>
//

#include <llfs/log_device_config2.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/math.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ IoRingLogConfig2 IoRingLogConfig2::from_packed(
    const FileOffsetPtr<const PackedLogDeviceConfig2&>& packed_config)
{
  return IoRingLogConfig2{
      .control_block_offset =
          packed_config.absolute_from_relative_offset(packed_config->control_block_offset),
      .log_capacity = packed_config->logical_size,
      .device_page_size_log2 = packed_config->device_page_size_log2,
      .data_alignment_log2 = packed_config->data_alignment_log2,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ IoRingLogConfig2 IoRingLogConfig2::from_logical_size(u64 logical_size,
                                                                Optional<u64> opt_device_page_size,
                                                                Optional<u64> opt_data_alignment)
{
  const u64 device_page_size = opt_device_page_size.value_or(Self::kDefaultDevicePageSize);
  const u64 data_alignment = opt_data_alignment.value_or(Self::kDefaultDataAlignment);

  const i32 device_page_size_log2 = batt::log2_ceil(device_page_size);
  const i32 data_alignment_log2 = batt::log2_ceil(data_alignment);

  return IoRingLogConfig2{
      .control_block_offset = 0,
      .log_capacity = batt::round_up_bits(data_alignment_log2, logical_size),
      .device_page_size_log2 = BATT_CHECKED_CAST(u16, device_page_size_log2),
      .data_alignment_log2 = BATT_CHECKED_CAST(u16, data_alignment_log2),
  };
}

}  //namespace llfs
