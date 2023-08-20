//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_FAKE_LOG_DEVICE_HPP
#define LLFS_TESTING_FAKE_LOG_DEVICE_HPP

#include <llfs/log_device.hpp>
#include <llfs/memory_log_device.hpp>
#include <llfs/status_code.hpp>

#include <limits>

namespace llfs {
namespace testing {

class FakeLogDevice;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FakeLogDeviceWriter : public LogDevice::Writer
{
 public:
  explicit FakeLogDeviceWriter(FakeLogDevice& fake_device) noexcept;

  // The current available space.
  //
  u64 space() const override;

  // The next slot offset to be written.  Updated by `commit`.
  //
  slot_offset_type slot_offset() override;

  // Allocate memory to write a new log slot of size `byte_count`.  Return error if not enough
  // space.
  //
  // `head_room` (unit=bytes) specifies an additional amount of space to ensure is available in the
  // log before returning success.  The head room is not included in the returned buffer.  Rather,
  // its purpose is to allow differentiated levels of priority amongst slots written to the log.
  // Without this, deadlock might be possible.  For example, a common scheme for log-event-driven
  // state machines is to store periodic checkpoints with deltas in between.  If deltas are allowed
  // to fill the entire capacity of the log, then there will be no room left to write a checkpoint,
  // and trimming the log will be impossible, thus deadlocking the system.
  //
  StatusOr<MutableBuffer> prepare(usize byte_count, usize head_room = 0) override;

  // Commits `byte_count` bytes; does not guarantee that these bytes are durable yet; a Reader may
  // be created to await the flush of a certin slot offset.
  //
  // Returns the new end (slot upper bound) of the log.
  //
  StatusOr<slot_offset_type> commit(usize byte_count) override;

  // Wait for the log to reach the specified state.
  //
  Status await(LogDevice::WriterEvent event) override;

 private:
  FakeLogDevice& fake_device_;
  LogDevice::Writer& impl_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FakeLogDeviceReader : public LogDevice::Reader
{
 public:
  explicit FakeLogDeviceReader(FakeLogDevice& fake_device,
                               std::unique_ptr<LogDevice::Reader> impl) noexcept;

  // Check whether the log device is closed.
  //
  bool is_closed() override;

  // The current log contents.  The memory returned by this method is a valid reflection of this
  // part of the log.  Even if `consume` invalidates some prefix of `data()`, the remaining portion
  // will still be valid. Likewise, once await returns Ok to indicate there is more data ready to
  // read, calling `data()` again will return the same memory with some extra at the end.
  //
  ConstBuffer data() override;

  // The current offset in bytes of this reader, relative to the start of the log.
  //
  slot_offset_type slot_offset() override;

  // Releases ownership of some prefix of `data()` (possibly all of it).  See description of
  // `data()` for more details.
  //
  void consume(usize byte_count) override;

  // Wait for the log to reach the specified state.
  //
  Status await(LogDevice::ReaderEvent event) override;

 private:
  FakeLogDevice& fake_device_;
  std::unique_ptr<LogDevice::Reader> impl_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FakeLogDevice : public LogDevice
{
 public:
  struct State {
    std::atomic<u64> device_time{0};
    std::atomic<u64> failure_time{std::numeric_limits<u64>::max()};
    std::atomic<bool> closed{false};

    bool is_failed() const
    {
      return this->device_time >= this->failure_time;
    }
  };

  explicit FakeLogDevice(LogDevice& impl, std::shared_ptr<State>&& state) noexcept
      : impl_{impl}
      , state_{std::move(state)}
  {
  }

  explicit FakeLogDevice(LogDevice& impl) noexcept : impl_{impl}, state_{std::make_shared<State>()}
  {
  }

  LogDevice& impl() const
  {
    return this->impl_;
  }

  std::shared_ptr<State> get_state() const
  {
    return this->state_;
  }

  Status age()
  {
    this->state_->device_time++;
    if (this->state_->device_time >= this->state_->failure_time) {
      return make_status(StatusCode::kFakeLogDeviceExpectedFailure);
    }
    if (this->state_->closed.load()) {
      return batt::StatusCode::kClosed;
    }
    return OkStatus();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // The maximum capacity in bytes of this log device.
  //
  u64 capacity() const override
  {
    return this->impl_.capacity();
  }

  // The current size of all committed data in the log.
  //
  u64 size() const override
  {
    return this->impl_.size();
  }

  // Convenience; the current available space.
  //
  u64 space() const override
  {
    return this->impl_.space();
  }

  Status trim(slot_offset_type slot_lower_bound) override
  {
    Status health_check = this->age();
    BATT_REQUIRE_OK(health_check);

    return this->impl_.trim(slot_lower_bound);
  }

  // Create a new reader.
  //
  std::unique_ptr<Reader> new_reader(Optional<slot_offset_type> slot_lower_bound,
                                     LogReadMode mode) override
  {
    return std::make_unique<FakeLogDeviceReader>(*this,
                                                 this->impl_.new_reader(slot_lower_bound, mode));
  }

  // Returns the current active slot range for the log.  `mode` determines whether the upper bound
  // will be the flushed or committed upper bound.
  //
  SlotRange slot_range(LogReadMode mode) override
  {
    return this->impl_.slot_range(mode);
  }

  // There can be only one Writer at a time.
  //
  Writer& writer() override
  {
    return this->writer_;
  }

  Status close() override
  {
    Status health_check = this->age();
    BATT_REQUIRE_OK(health_check);

    this->state_->closed.store(true);

    return OkStatus();
  }

  Status sync(LogReadMode mode, SlotUpperBoundAt event) override
  {
    Status health_check = this->age();
    BATT_REQUIRE_OK(health_check);

    return this->impl_.sync(mode, event);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  LogDevice& impl_;
  FakeLogDeviceWriter writer_{*this};
  std::shared_ptr<State> state_;
};

inline std::ostream& operator<<(std::ostream& out, const FakeLogDevice::State& t)
{
  return out << "FakeLogDevice::State{.device_time=" << t.device_time
             << ", .failure_time=" << t.failure_time << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename LogDriverImpl>
class FakeLogDeviceFactory : public LogDeviceFactory
{
 public:
  explicit FakeLogDeviceFactory(LogDevice& device_impl, LogDriverImpl& log_driver_impl,
                                std::shared_ptr<FakeLogDevice::State>&& state) noexcept
      : device_impl_{device_impl}
      , log_driver_impl_{log_driver_impl}
      , state_{std::move(state)}
  {
  }

  FakeLogDeviceFactory(FakeLogDeviceFactory&& that) noexcept
      : device_impl_{that.device_impl_}
      , log_driver_impl_{that.log_driver_impl_}
      , state_{std::move(that.state_)}
  {
  }

  // Creates a log device in 'recovery mode,' passing it to the supplied scan function.
  //
  // Recovery mode is different from a LogDevice's normal mode of operation in that some trailing
  // portion of the log may be truncated as a result of the recovery process.  This is equivalent to
  // rolling back or aborting partially completed operations.  The scan_fn's job is twofold:
  //
  //  - Cache any state required to resume normal operation by the user of the log
  //  - Determine the truncate point, which will be the new commit/flush position going forward
  //
  // The truncate point, given as a byte offset from the log origin, is returned by `scan_fn` upon
  // successful completion.  If a fatal (non-recoverable) error is encountered, the scan_fn
  // indicates this by returning a non-Ok status.
  //
  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override
  {
    auto instance =
        std::make_unique<FakeLogDevice>(this->device_impl_, batt::make_copy(this->state_));

    StatusOr<slot_offset_type> scan_status =
        scan_fn(*instance->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable));

    BATT_REQUIRE_OK(scan_status);

    // Truncate the log at the indicated point.
    //
    this->truncate(this->log_driver_impl_, *scan_status);

    return instance;
  }

  const std::shared_ptr<FakeLogDevice::State>& state() const
  {
    return this->state_;
  }

 private:
  LogDevice& device_impl_;
  LogDriverImpl& log_driver_impl_;
  std::shared_ptr<FakeLogDevice::State> state_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline FakeLogDeviceFactory<MemoryLogStorageDriver> make_fake_log_device_factory(
    MemoryLogDevice& mem_log)
{
  return FakeLogDeviceFactory<MemoryLogStorageDriver>{mem_log, mem_log.driver().impl(),
                                                      std::make_shared<FakeLogDevice::State>()};
}

inline FakeLogDeviceFactory<MemoryLogStorageDriver> make_fake_log_device_factory(
    MemoryLogDevice& mem_log, std::shared_ptr<FakeLogDevice::State>&& state)
{
  return FakeLogDeviceFactory<MemoryLogStorageDriver>{mem_log, mem_log.driver().impl(),
                                                      std::move(state)};
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class FakeLogDeviceWriter
//
inline FakeLogDeviceWriter::FakeLogDeviceWriter(FakeLogDevice& fake_device) noexcept
    : fake_device_{fake_device}
    , impl_{fake_device.impl().writer()}
{
}

inline u64 FakeLogDeviceWriter::space() const
{
  return this->impl_.space();
}

inline slot_offset_type FakeLogDeviceWriter::slot_offset()
{
  return this->impl_.slot_offset();
}

inline StatusOr<MutableBuffer> FakeLogDeviceWriter::prepare(usize byte_count, usize head_room)
{
  Status health_check = this->fake_device_.age();
  BATT_REQUIRE_OK(health_check);

  return this->impl_.prepare(byte_count, head_room);
}

inline StatusOr<slot_offset_type> FakeLogDeviceWriter::commit(usize byte_count)
{
  Status health_check = this->fake_device_.age();
  if (!health_check.ok()) {
    this->impl_.commit(0).IgnoreError();
  }
  BATT_REQUIRE_OK(health_check);

  return this->impl_.commit(byte_count);
}

inline Status FakeLogDeviceWriter::await(LogDevice::WriterEvent event)
{
  Status health_check = this->fake_device_.age();
  BATT_REQUIRE_OK(health_check);

  return this->impl_.await(event);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class FakeLogDeviceReader
//
inline FakeLogDeviceReader::FakeLogDeviceReader(FakeLogDevice& fake_device,
                                                std::unique_ptr<LogDevice::Reader> impl) noexcept
    : fake_device_{fake_device}
    , impl_{std::move(impl)}
{
  BATT_CHECK_NOT_NULLPTR(this->impl_);
}

inline bool FakeLogDeviceReader::is_closed()
{
  BATT_CHECK_NOT_NULLPTR(this->impl_);
  return this->fake_device_.get_state()->closed.load();
}

inline ConstBuffer FakeLogDeviceReader::data()
{
  BATT_CHECK_NOT_NULLPTR(this->impl_);
  return this->impl_->data();
}

inline slot_offset_type FakeLogDeviceReader::slot_offset()
{
  BATT_CHECK_NOT_NULLPTR(this->impl_);
  return this->impl_->slot_offset();
}

inline void FakeLogDeviceReader::consume(usize byte_count)
{
  BATT_CHECK_NOT_NULLPTR(this->impl_);
  return this->impl_->consume(byte_count);
}

inline Status FakeLogDeviceReader::await(LogDevice::ReaderEvent event)
{
  Status health_check = this->fake_device_.age();
  BATT_REQUIRE_OK(health_check);

  return this->impl_->await(event);
}

}  // namespace testing
}  // namespace llfs

#endif  // LLFS_TESTING_FAKE_LOG_DEVICE_HPP
