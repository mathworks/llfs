//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE_HPP
#define LLFS_IORING_LOG_DEVICE_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/basic_ring_buffer_log_device.hpp>
#include <llfs/confirm.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/file_offset_ptr.hpp>
#include <llfs/ioring.hpp>
#include <llfs/ioring_log_config.hpp>
#include <llfs/ioring_log_driver.hpp>
#include <llfs/ioring_log_driver_options.hpp>
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/metrics.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/task.hpp>

namespace llfs {

using IoRingLogDevice = BasicRingBufferLogDevice<IoRingLogDriver>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class IoRingLogDeviceFactory : public LogDeviceFactory
{
 public:
  explicit IoRingLogDeviceFactory(int fd,
                                  const FileOffsetPtr<const PackedLogDeviceConfig&>& packed_config,
                                  const IoRingLogDriverOptions& options) noexcept
      : IoRingLogDeviceFactory{fd, IoRingLogConfig::from_packed(packed_config), options}
  {
  }

  explicit IoRingLogDeviceFactory(int fd, const IoRingLogConfig& config,
                                  const IoRingLogDriverOptions& options) noexcept
      : fd_{fd}
      , config_{config}
      , options_{options}
  {
  }

  ~IoRingLogDeviceFactory() noexcept
  {
    if (this->fd_ != -1) {
      ::close(this->fd_);
    }
  }

  StatusOr<std::unique_ptr<IoRingLogDevice>> open_ioring_log_device()
  {
    auto instance = std::make_unique<IoRingLogDevice>(
        RingBuffer::TempFile{.byte_size = this->config_.logical_size}, this->fd_, this->config_,
        this->options_);

    this->fd_ = -1;

    Status open_status = instance->open();
    BATT_REQUIRE_OK(open_status);

    return instance;
  }

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override
  {
    auto instance = this->open_ioring_log_device();
    BATT_REQUIRE_OK(instance);

    auto scan_status =
        scan_fn(*(*instance)->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable));
    BATT_REQUIRE_OK(scan_status);

    return instance;
  }

 private:
  int fd_;
  IoRingLogConfig config_;
  IoRingLogDriverOptions options_;
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_LOG_DEVICE_HPP
