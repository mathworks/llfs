//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/log_block_calculator.hpp>
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ IoRingLogConfig IoRingLogConfig::from_logical_size(u64 logical_size,
                                                              Optional<usize> opt_block_size)
{
  const usize block_size = opt_block_size.value_or(IoRingLogConfig::kDefaultBlockSize);
  const i32 block_size_log2 = batt::log2_ceil(block_size);

  BATT_CHECK_GE(block_size_log2, kLogPageSizeLog2);

  return IoRingLogConfig{
      .logical_size = logical_size,
      .physical_offset = 0,
      .physical_size =
          LogBlockCalculator::disk_size_required_for_log_size(logical_size, block_size),
      .pages_per_block_log2 = block_size_log2 - kLogPageSizeLog2,
  };
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
