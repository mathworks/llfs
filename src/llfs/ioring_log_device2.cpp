//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_device2.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status initialize_log_device2(RawBlockFile& file, const IoRingLogConfig2& config,
                              ConfirmThisWillEraseAllMyData confirm)
{
  if (confirm != ConfirmThisWillEraseAllMyData::kYes) {
    return ::llfs::make_status(::llfs::StatusCode::kFileLogEraseNotConfirmed);
  }

  using AlignedUnit = std::aligned_storage_t<kDirectIOBlockSize, kDirectIOBlockAlign>;

  const usize device_page_size = usize{1} << config.device_page_size_log2;
  const usize buffer_size = batt::round_up_to(sizeof(AlignedUnit), device_page_size);

  BATT_CHECK_EQ(buffer_size, device_page_size);

  std::unique_ptr<AlignedUnit[]> buffer{new AlignedUnit[buffer_size / sizeof(AlignedUnit)]};
  std::memset(buffer.get(), 0, buffer_size);

  auto* control_block = reinterpret_cast<PackedLogControlBlock2*>(buffer.get());

  control_block->magic = PackedLogControlBlock2::kMagic;
  control_block->data_size = config.log_capacity;
  control_block->trim_pos = 0;
  control_block->flush_pos = 0;
  control_block->generation = 0;
  control_block->control_block_size = BATT_CHECKED_CAST(u32, device_page_size);
  control_block->control_header_size = BATT_CHECKED_CAST(u32, sizeof(PackedLogControlBlock2));
  control_block->device_page_size_log2 = config.device_page_size_log2;
  control_block->data_alignment_log2 = config.data_alignment_log2;

  BATT_REQUIRE_OK(
      write_all(file, config.control_block_offset, ConstBuffer{buffer.get(), device_page_size}));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingLogDevice2Factory::IoRingLogDevice2Factory(
    int fd, const FileOffsetPtr<const PackedLogDeviceConfig2&>& packed_config,
    const LogDeviceRuntimeOptions& options) noexcept
    : IoRingLogDevice2Factory{fd, IoRingLogConfig2::from_packed(packed_config), options}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingLogDevice2Factory::IoRingLogDevice2Factory(
    int fd, const IoRingLogConfig2& config, const LogDeviceRuntimeOptions& options) noexcept
    : fd_{fd}
    , config_{config}
    , options_{options}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingLogDevice2Factory::~IoRingLogDevice2Factory() noexcept
{
  if (this->fd_ != -1) {
    ::close(this->fd_);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<IoRingLogDevice2>> IoRingLogDevice2Factory::open_ioring_log_device()
{
  BATT_ASSIGN_OK_RESULT(DefaultIoRingLogDeviceStorage storage,
                        DefaultIoRingLogDeviceStorage::make_new(
                            MaxQueueDepth{this->options_.max_concurrent_writes * 2}, this->fd_));

  this->fd_ = -1;

  auto instance =
      std::make_unique<IoRingLogDevice2>(this->config_, this->options_, std::move(storage));

  Status open_status = instance->open();
  BATT_REQUIRE_OK(open_status);

  return instance;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<LogDevice>> IoRingLogDevice2Factory::open_log_device(
    const LogScanFn& scan_fn) /*override*/
{
  auto instance = this->open_ioring_log_device();
  BATT_REQUIRE_OK(instance);

  auto scan_status =
      scan_fn(*(*instance)->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable));
  BATT_REQUIRE_OK(scan_status);

  return instance;
}

}  //namespace llfs
