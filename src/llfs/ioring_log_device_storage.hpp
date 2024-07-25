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
#include <llfs/filesystem.hpp>
#include <llfs/ioring.hpp>
#include <llfs/ioring_file.hpp>
#include <llfs/optional.hpp>
#include <llfs/raw_block_file.hpp>
#include <llfs/status.hpp>

#include <batteries/async/watch.hpp>

#include <thread>

namespace llfs {

class DefaultIoRingLogDeviceStorage
{
 public:
  /** \brief Creates a new IoRing with the specified queue depth, wraps it in a
   * DefaultIoRingLogDeviceStorage, and returns the result.
   *
   * The returned object should be used by an IoRingLogDriver/IoRingLogDevice for the low-level file
   * I/O.
   */
  static StatusOr<DefaultIoRingLogDeviceStorage> make_new(MaxQueueDepth entries, int fd);

  /** \brief A background thread or task that runs the IoRing event loop for a given
   * DefaultIoRingLogDeviceStorage object.
   */
  class EventLoopTask;

  /** \brief Implementation of RawBlockFile based on an instance of DefaultIoRingLogDeviceStorage.
   */
  class RawBlockFileImpl;

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
  void async_write_some(i64 file_offset, const ConstBuffer& data, Handler&& handler);

  template <typename Handler>
  void async_write_some_fixed(i64 file_offset, const ConstBuffer& data, i32 buf_index,
                              Handler&& handler);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  explicit DefaultIoRingLogDeviceStorage(IoRing&& io_ring, IoRing::File&& file) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  IoRing io_ring_;
  IoRing::File file_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
inline void DefaultIoRingLogDeviceStorage::async_write_some(i64 file_offset,
                                                            const ConstBuffer& data,
                                                            Handler&& handler)
{
  this->file_.async_write_some(file_offset, data, BATT_FORWARD(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
inline void DefaultIoRingLogDeviceStorage::async_write_some_fixed(i64 file_offset,
                                                                  const ConstBuffer& data,
                                                                  i32 buf_index, Handler&& handler)
{
  this->file_.async_write_some_fixed(file_offset, data, buf_index, BATT_FORWARD(handler));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class DefaultIoRingLogDeviceStorage::EventLoopTask
{
 public:
  explicit EventLoopTask(DefaultIoRingLogDeviceStorage& storage, std::string_view caller) noexcept;

  EventLoopTask(const EventLoopTask&) = delete;
  EventLoopTask& operator=(const EventLoopTask&) = delete;

  ~EventLoopTask() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void join();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  DefaultIoRingLogDeviceStorage& storage_;
  std::string_view caller_;
  std::thread thread_;
  batt::Watch<bool> done_{false};
  bool join_called_ = false;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class DefaultIoRingLogDeviceStorage::RawBlockFileImpl : public RawBlockFile
{
 public:
  explicit RawBlockFileImpl(DefaultIoRingLogDeviceStorage& storage) noexcept : storage_{storage}
  {
    this->storage_.on_work_started();
    this->event_loop_task_.emplace(this->storage_, "RawBlockFileImpl");
  }

  ~RawBlockFileImpl() noexcept
  {
    BATT_CHECK(this->event_loop_task_);

    this->storage_.on_work_finished();
    this->event_loop_task_->join();
  }

  StatusOr<i64> write_some(i64 offset, const ConstBuffer& data) override
  {
    return batt::Task::await<batt::StatusOr<i64>>([&](auto&& handler) {
      this->storage_.file_.async_write_some(offset, data, BATT_FORWARD(handler));
    });
  }

  StatusOr<i64> read_some(i64 offset, const MutableBuffer& buffer) override
  {
    return batt::Task::await<batt::StatusOr<i64>>([&](auto&& handler) {
      this->storage_.file_.async_read_some(offset, buffer, BATT_FORWARD(handler));
    });
  }

  StatusOr<i64> get_size() override
  {
    return sizeof_fd(this->storage_.file_.get_fd());
  }

  Status truncate(i64 new_offset_upper_bound) override
  {
    return truncate_fd(this->storage_.file_.get_fd(),
                       BATT_CHECKED_CAST(u64, new_offset_upper_bound));
  }

  Status truncate_at_least(i64 minimum_size) override
  {
    StatusOr<i64> current_size = this->get_size();
    BATT_REQUIRE_OK(current_size);

    if (*current_size < minimum_size) {
      return this->truncate(minimum_size);
    }

    return batt::OkStatus();
  }

 private:
  DefaultIoRingLogDeviceStorage& storage_;
  Optional<EventLoopTask> event_loop_task_;
};

}  //namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE_FILE_HPP
