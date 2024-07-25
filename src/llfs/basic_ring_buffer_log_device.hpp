//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BASIC_RING_BUFFER_LOG_DEVICE_HPP
#define LLFS_BASIC_RING_BUFFER_LOG_DEVICE_HPP

#include <llfs/basic_log_storage_driver.hpp>
#include <llfs/basic_log_storage_reader.hpp>
#include <llfs/log_device.hpp>
#include <llfs/log_storage_driver_context.hpp>
#include <llfs/ring_buffer.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//

class LogDeviceSnapshot;

template <class Impl>
class BasicRingBufferLogDevice
    : public LogDevice
    , protected LogStorageDriverContext
{
 public:
  friend class LogDeviceSnapshot;

  using driver_type = BasicLogStorageDriver<Impl>;

  class WriterImpl;

  explicit BasicRingBufferLogDevice(const RingBuffer::Params& params) noexcept;

  template <typename... Args, typename = batt::EnableIfNoShadow<
                                  BasicRingBufferLogDevice, const RingBuffer::Params&, Args&&...>>
  explicit BasicRingBufferLogDevice(const RingBuffer::Params& params, Args&&... args) noexcept;

  ~BasicRingBufferLogDevice() noexcept;

  u64 capacity() const override;

  u64 size() const override;

  Status trim(slot_offset_type slot_lower_bound) override;

  std::unique_ptr<Reader> new_reader(Optional<slot_offset_type> slot_lower_bound,
                                     LogReadMode mode) override;

  SlotRange slot_range(LogReadMode mode) override;

  Writer& writer() override;

  Status open()
  {
    return this->driver_.open();
  }

  Status close() override;

  void halt() override
  {
    this->driver_.halt();
  }

  void join() override
  {
    this->driver_.join();
  }

  Status sync(LogReadMode mode, SlotUpperBoundAt event) override;

  driver_type& driver() noexcept
  {
    return this->driver_;
  }

  const driver_type& driver() const noexcept
  {
    return this->driver_;
  }

 private:
  std::unique_ptr<WriterImpl> writer_ = std::make_unique<WriterImpl>(this);
  driver_type driver_{*this};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class BasicRingBufferLogDevice
//

template <class Impl>
inline BasicRingBufferLogDevice<Impl>::BasicRingBufferLogDevice(
    const RingBuffer::Params& params) noexcept
    : LogStorageDriverContext{params}
{
}

template <class Impl>
template <typename... Args, typename>
inline BasicRingBufferLogDevice<Impl>::BasicRingBufferLogDevice(const RingBuffer::Params& params,
                                                                Args&&... args) noexcept
    : LogStorageDriverContext{params}
    , driver_{*this, BATT_FORWARD(args)...}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline BasicRingBufferLogDevice<Impl>::~BasicRingBufferLogDevice() noexcept
{
  this->close().IgnoreError();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline u64 BasicRingBufferLogDevice<Impl>::capacity() const
{
  return this->buffer_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline u64 BasicRingBufferLogDevice<Impl>::size() const
{
  return slot_clamp_distance(this->driver_.get_trim_pos(), this->driver_.get_commit_pos());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline Status BasicRingBufferLogDevice<Impl>::trim(slot_offset_type slot_lower_bound)
{
  LLFS_CHECK_SLOT_GE(slot_lower_bound, this->driver_.get_trim_pos());

  return this->driver_.set_trim_pos(slot_lower_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline std::unique_ptr<LogDevice::Reader> BasicRingBufferLogDevice<Impl>::new_reader(
    Optional<slot_offset_type> slot_lower_bound, LogReadMode mode)
{
  auto& context = static_cast<LogStorageDriverContext&>(*this);
  return std::make_unique<BasicLogStorageReader<Impl>>(
      context, /*driver=*/this->driver_, mode,
      slot_lower_bound.value_or(this->driver_.get_trim_pos()));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline SlotRange BasicRingBufferLogDevice<Impl>::slot_range(LogReadMode mode)
{
  if (mode == LogReadMode ::kDurable) {
    return SlotRange{this->driver_.get_trim_pos(), this->driver_.get_flush_pos()};
  }
  return SlotRange{this->driver_.get_trim_pos(), this->driver_.get_commit_pos()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline LogDevice::Writer& BasicRingBufferLogDevice<Impl>::writer()
{
  return *this->writer_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline Status BasicRingBufferLogDevice<Impl>::close()
{
  const bool closed_prior = this->closed_.exchange(true);
  if (!closed_prior) {
    return this->driver_.close();
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline Status BasicRingBufferLogDevice<Impl>::sync(LogReadMode mode, SlotUpperBoundAt event)
{
  switch (mode) {
    case LogReadMode::kInconsistent:
      return OkStatus();

    case LogReadMode::kSpeculative:
      return this->driver_.await_commit_pos(event.offset).status();

    case LogReadMode::kDurable:
      return this->driver_.await_flush_pos(event.offset).status();
  }

  BATT_PANIC() << "bad LogReadMode value: " << (unsigned)mode;
  BATT_UNREACHABLE();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class BasicRingBufferLogDevice::WriterImpl
//

template <class Impl>
class BasicRingBufferLogDevice<Impl>::WriterImpl : public LogDevice::Writer
{
 public:
  explicit WriterImpl(BasicRingBufferLogDevice<Impl>* device) noexcept : device_{device}
  {
    initialize_status_codes();
  }

  slot_offset_type slot_offset() override
  {
    return this->device_->driver_.get_commit_pos();
  }

  u64 space() const override
  {
    const slot_offset_type readable_begin = this->device_->driver_.get_trim_pos();
    const slot_offset_type readable_end = this->device_->driver_.get_commit_pos();

    const usize space_available =
        this->device_->buffer_.size() - LLFS_CHECKED_SLOT_DISTANCE(readable_begin, readable_end);

    return space_available;
  }

  StatusOr<MutableBuffer> prepare(usize byte_count, usize head_room) override
  {
    if (this->device_->closed_.load()) {
      return ::llfs::make_status(StatusCode::kPrepareFailedLogClosed);
    }

    const usize space_required = byte_count + head_room;

    if (this->space() < space_required) {
      return ::llfs::make_status(StatusCode::kPrepareFailedTrimRequired);
    }

    const slot_offset_type commit_pos = this->device_->driver_.get_commit_pos();
    MutableBuffer writable_region = this->device_->buffer_.get_mut(commit_pos);

    this->prepared_offset_ = commit_pos;

    return MutableBuffer{writable_region.data(), byte_count};
  }

  StatusOr<slot_offset_type> commit(usize byte_count) override
  {
    auto guard = batt::finally([&] {
      this->prepared_offset_ = None;
    });

    if (this->device_->closed_.load()) {
      return ::llfs::make_status(StatusCode::kCommitFailedLogClosed);
    }

    BATT_CHECK(this->prepared_offset_);

    const slot_offset_type new_offset = *this->prepared_offset_ + byte_count;

    Status status = this->device_->driver_.set_commit_pos(new_offset);

    BATT_REQUIRE_OK(status);

    return new_offset;
  }

  Status await(WriterEvent event) override
  {
    return batt::case_of(
        event,
        [&](const SlotLowerBoundAt& trim_lower_bound_at) -> Status {
          BATT_DEBUG_INFO("Writer::await(SlotLowerBoundAt{"
                          << trim_lower_bound_at.offset << "})"
                          << " buffer_size=" << this->device_->buffer_.size() << " space="
                          << this->space() << " trim_pos=" << this->device_->driver_.get_trim_pos()
                          << " flush_pos=" << this->device_->driver_.get_flush_pos()
                          << " commit_pos=" << this->device_->driver_.get_commit_pos());

          return this->device_->driver_.await_trim_pos(trim_lower_bound_at.offset).status();
        },
        [&](const BytesAvailable& prepare_available) -> Status {
          BATT_DEBUG_INFO("Writer::await(BytesAvailable{" << prepare_available.size << "})");

          const slot_offset_type data_upper_bound = this->device_->driver_.get_commit_pos();

          return this->await(SlotLowerBoundAt{
              .offset = data_upper_bound - slot_offset_type{this->device_->buffer_.size()} +
                        prepare_available.size,
          });
        });
  }

 private:
  BasicRingBufferLogDevice<Impl>* device_;
  Optional<slot_offset_type> prepared_offset_;
};

}  // namespace llfs

#endif  // LLFS_BASIC_RING_BUFFER_LOG_DEVICE_HPP
