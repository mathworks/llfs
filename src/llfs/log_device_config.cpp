//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/log_device_config.hpp>
//

#include <llfs/ioring_log_device.hpp>
#include <llfs/raw_block_device.hpp>

#include <batteries/stream_util.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_PRINT_OBJECT_IMPL(PackedLogDeviceConfig,  //
                       (block_0_offset)        //
                       (physical_size)         //
                       (logical_size)          //
                       (pages_per_block_log2)  //
)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedLogDeviceConfig&> p_config,
                                const LogDeviceConfigOptions& options)
{
  const i64 logical_size = round_up_to_page_size_multiple(options.log_size);
  const u16 block_size_log2 = options.pages_per_block_log2.value_or(2);
  const u64 block_size = u64{1} << block_size_log2;
  const i64 physical_size =
      IoRingLogDriver::disk_size_required_for_log_size(logical_size, block_size);

  Interval<i64> blocks_offset = txn.reserve_aligned(/*bits=*/12, physical_size);

  BATT_CHECK_EQ(blocks_offset.size(), physical_size);

  p_config.absolute_block_0_offset(blocks_offset.lower_bound);
  p_config->physical_size = physical_size;
  p_config->logical_size = logical_size;
  p_config->pages_per_block_log2 = block_size_log2;
  p_config->uuid = options.uuid.value_or(boost::uuids::random_generator{}());

  // Initialize the log page headers before flushing config slot.
  //
  txn.require_pre_flush_action(
      [config = IoRingLogConfig::from_packed(p_config)](RawBlockDevice& file) {
        return initialize_ioring_log_device(file, config, ConfirmThisWillEraseAllMyData{true});
      });

  return OkStatus();
}

}  // namespace llfs
