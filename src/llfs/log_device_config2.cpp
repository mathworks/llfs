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

#include <llfs/ioring_log_device2.hpp>
#include <llfs/log_device_config2.hpp>
#include <llfs/raw_block_file.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/stream_util.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_PRINT_OBJECT_IMPL(PackedLogDeviceConfig2,  //
                       (control_block_offset)   //
                       (logical_size)           //
                       (device_page_size_log2)  //
                       (data_alignment_log2)    //
)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedLogDeviceConfig2&> p_config,
                                const LogDeviceConfigOptions2& options)
{
  const i64 logical_size = round_up_to_page_size_multiple(options.log_size);

  const u16 device_page_size_log2 =
      options.device_page_size_log2.value_or(LogDeviceConfigOptions2::kDefaultDevicePageSizeLog2);

  const u16 data_alignment_log2 =
      options.data_alignment_log2.value_or(LogDeviceConfigOptions2::kDefaultDataAlignmentLog2);

  BATT_CHECK_GE(data_alignment_log2, device_page_size_log2);

  const i64 data_page_size = i64{1} << data_alignment_log2;

  const i64 physical_size = /*control block*/ data_page_size + /*data pages*/ logical_size;

  Interval<i64> blocks_offset = txn.reserve_aligned(/*bits=*/data_alignment_log2, physical_size);

  BATT_CHECK_EQ(blocks_offset.size(), physical_size)
      << BATT_INSPECT(logical_size) << BATT_INSPECT(options.log_size);

  p_config->control_block_offset =
      p_config.relative_from_absolute_offset(blocks_offset.lower_bound);
  p_config->logical_size = logical_size;
  p_config->device_page_size_log2 = device_page_size_log2;
  p_config->data_alignment_log2 = data_alignment_log2;
  p_config->uuid = options.uuid.value_or(boost::uuids::random_generator{}());

  BATT_CHECK_EQ(BATT_CHECKED_CAST(i64, blocks_offset.lower_bound + physical_size),
                blocks_offset.upper_bound);

  // Initialize the log page headers before flushing config slot.
  //
  txn.require_pre_flush_action([config = IoRingLogConfig2::from_packed(p_config),
                                blocks_offset](RawBlockFile& file) -> Status  //
                               {
                                 Status truncate_status =
                                     file.truncate_at_least(blocks_offset.upper_bound);

                                 BATT_REQUIRE_OK(truncate_status);

                                 return initialize_log_device2(file, config);
                               });

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<IoRingLogDevice2Factory>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& /*storage_context*/, const std::string& file_name,
    const FileOffsetPtr<const PackedLogDeviceConfig2&>& p_config, LogDeviceRuntimeOptions options)
{
  const int flags = O_DIRECT | O_SYNC | O_RDWR;

  int fd = batt::syscall_retry([&] {
    return ::open(file_name.c_str(), flags);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return std::make_unique<IoRingLogDevice2Factory>(fd, p_config, options);
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
