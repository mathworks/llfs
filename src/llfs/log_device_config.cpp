//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/log_device_config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring_log_device.hpp>
#include <llfs/ioring_log_initializer.hpp>
#include <llfs/raw_block_file.hpp>

#include <batteries/checked_cast.hpp>
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
  const u32 pages_per_block_log2 =
      options.pages_per_block_log2.value_or(IoRingLogConfig::kDefaultPagesPerBlockLog2);
  const u64 pages_per_block = u64{1} << pages_per_block_log2;
  const u64 block_size = pages_per_block * kLogPageSize;
  const i64 physical_size =
      IoRingLogDriver::disk_size_required_for_log_size(logical_size, block_size);

  Interval<i64> blocks_offset = txn.reserve_aligned(/*bits=*/kLogPageSizeLog2, physical_size);

  BATT_CHECK_EQ(blocks_offset.size(), physical_size)
      << BATT_INSPECT(logical_size) << BATT_INSPECT(block_size) << BATT_INSPECT(options.log_size);

  p_config.absolute_block_0_offset(blocks_offset.lower_bound);
  p_config->physical_size = physical_size;
  p_config->logical_size = logical_size;
  p_config->pages_per_block_log2 = pages_per_block_log2;
  p_config->uuid = options.uuid.value_or(boost::uuids::random_generator{}());

  // Initialize the log page headers before flushing config slot.
  //
  txn.require_pre_flush_action(
      [config = IoRingLogConfig::from_packed(p_config),
       blocks_offset](RawBlockFile& file) -> Status  //
      {
        BATT_CHECK_EQ(BATT_CHECKED_CAST(i64, config.physical_offset + config.physical_size),
                      blocks_offset.upper_bound);
        Status truncate_status =
            file.truncate_at_least(config.physical_offset + config.physical_size);
        BATT_REQUIRE_OK(truncate_status);

        return initialize_ioring_log_device(file, config, ConfirmThisWillEraseAllMyData{true});
      });

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<IoRingLogDeviceFactory>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context, const std::string& file_name,
    const FileOffsetPtr<const PackedLogDeviceConfig&>& p_config, IoRingLogDriverOptions options)
{
  BATT_ASSIGN_OK_RESULT(usize block_count, p_config->block_count());
  options.limit_queue_depth(block_count);

  const int flags = O_DIRECT | O_SYNC | O_RDWR;

  int fd = batt::syscall_retry([&] {
    return system_open2(file_name.c_str(), flags);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return std::make_unique<IoRingLogDeviceFactory>(storage_context->get_scheduler(), fd, p_config,
                                                  options);
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
