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

}  //namespace llfs
