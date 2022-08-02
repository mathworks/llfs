//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_config.hpp>
//

#include <llfs/log_device_config.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ IoRingLogConfig IoRingLogConfig::from_packed(
    const FileOffsetPtr<const PackedLogDeviceConfig&>& packed_config)
{
  return IoRingLogConfig{
      .logical_size = packed_config->logical_size,
      .physical_offset = packed_config.absolute_block_0_offset(),
      .physical_size = packed_config->physical_size,
      .pages_per_block_log2 = packed_config->pages_per_block_log2,
  };
}

}  // namespace llfs
