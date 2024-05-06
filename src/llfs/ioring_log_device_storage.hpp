//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE_STORAGE_HPP
#define LLFS_IORING_LOG_DEVICE_STORAGE_HPP

#include <llfs/config.hpp>
//
#include <llfs/ioring.hpp>
#include <llfs/ioring_file.hpp>
#include <llfs/status.hpp>

namespace llfs {

class DefaultIoRingLogDeviceStorage
{
 public:
  static StatusOr<DefaultIoRingLogDeviceStorage> make_new(MaxQueueDepth entries, int fd)
  {
    BATT_ASSIGN_OK_RESULT(IoRing io_ring, IoRing::make_new(entries));
    BATT_ASSIGN_OK_RESULT(IoRing::File file, IoRing::File::open(io_ring, fd));

    return DefaultIoRingLogDeviceStorage{std::move(io_ring), std::move(file)};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  DefaultIoRingLogDeviceStorage(const DefaultIoRingLogDeviceStorage&) = delete;
  DefaultIoRingLogDeviceStorage& operator=(const DefaultIoRingLogDeviceStorage&) = delete;

  DefaultIoRingLogDeviceStorage(DefaultIoRingLogDeviceStorage&&) = default;
  DefaultIoRingLogDeviceStorage& operator=(DefaultIoRingLogDeviceStorage&&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status register_fd()
  {
    return this->file_.register_fd();
  }

  StatusOr<usize> register_buffers(batt::BoxedSeq<MutableBuffer>&& buffers,
                                   bool update = false) const noexcept
  {
    return this->io_ring_.register_buffers(std::move(buffers), update);
  }

  Status close()
  {
    return this->file_.close();
  }

  void on_work_started() const noexcept
  {
    this->io_ring_.on_work_started();
  }

  void on_work_finished() const noexcept
  {
    this->io_ring_.on_work_finished();
  }

  Status run_event_loop() const noexcept
  {
    return this->io_ring_.run();
  }

  void reset_event_loop() const noexcept
  {
    this->io_ring_.reset();
  }

  template <typename Handler = void(StatusOr<i32>)>
  void post_to_event_loop(Handler&& handler) const noexcept
  {
    this->io_ring_.post(BATT_FORWARD(handler));
  }

  void stop_event_loop() const noexcept
  {
    this->io_ring_.stop();
  }

  Status read_all(i64 offset, MutableBuffer buffer)
  {
    return this->file_.read_all(offset, buffer);
  }

  template <typename Handler>
  void async_write_some_fixed(i64 file_offset, const ConstBuffer& data, i32 buf_index,
                              Handler&& handler)
  {
    this->file_.async_write_some_fixed(file_offset, data, buf_index, BATT_FORWARD(handler));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  explicit DefaultIoRingLogDeviceStorage(IoRing&& io_ring, IoRing::File&& file) noexcept
      : io_ring_{std::move(io_ring)}
      , file_{std::move(file)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  IoRing io_ring_;
  IoRing::File file_;
};

}  //namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE_FILE_HPP
