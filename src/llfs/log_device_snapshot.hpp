//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_LOG_DEVICE_SNAPSHOT_HPP
#define LLFS_LOG_DEVICE_SNAPSHOT_HPP

#include <llfs/basic_ring_buffer_log_device.hpp>
#include <llfs/slot.hpp>

#include <batteries/buffer.hpp>

#include <boost/operators.hpp>

#include <memory>

namespace llfs {

class LogDeviceSnapshot;

usize hash_value(const LogDeviceSnapshot& s);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class LogDeviceSnapshot : public boost::equality_comparable<LogDeviceSnapshot>
{
 public:
  friend usize hash_value(const LogDeviceSnapshot& s);

  template <typename Impl>
  static LogDeviceSnapshot from_device(BasicRingBufferLogDevice<Impl>& device, LogReadMode mode)
  {
    LogDeviceSnapshot snapshot;

    snapshot.trim_pos_ = device.driver().get_trim_pos();
    snapshot.flush_pos_ = device.driver().get_flush_pos();
    snapshot.commit_pos_ = [&] {
      if (mode == LogReadMode::kDurable) {
        return device.driver().get_flush_pos();
      }
      return device.driver().get_commit_pos();
    }();

    ConstBuffer src =
        batt::resize_buffer(device.buffer_.get(snapshot.trim_pos_),
                            LLFS_CHECKED_SLOT_DISTANCE(snapshot.trim_pos_, snapshot.commit_pos_));

    snapshot.byte_storage_.reset(new u8[src.size()]);

    std::memcpy(snapshot.byte_storage_.get(), src.data(), src.size());

    snapshot.hash_value_ = snapshot.compute_hash_value();

    return snapshot;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  LogDeviceSnapshot() = default;

  LogDeviceSnapshot(const LogDeviceSnapshot&) = default;
  LogDeviceSnapshot& operator=(const LogDeviceSnapshot&) = default;

  LogDeviceSnapshot(LogDeviceSnapshot&&) = default;
  LogDeviceSnapshot& operator=(LogDeviceSnapshot&&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  slot_offset_type trim_pos() const
  {
    return this->trim_pos_;
  }

  slot_offset_type flush_pos() const
  {
    return this->flush_pos_;
  }

  slot_offset_type commit_pos() const
  {
    return this->commit_pos_;
  }

  usize size() const
  {
    return LLFS_CHECKED_SLOT_DISTANCE(this->trim_pos(), this->commit_pos());
  }

  const u8* bytes() const
  {
    return this->byte_storage_.get();
  }

  const u8* begin() const
  {
    return this->bytes();
  }

  const u8* end() const
  {
    return this->bytes() + this->size();
  }

  ConstBuffer as_buffer() const
  {
    return ConstBuffer{this->bytes(), this->size()};
  }

  explicit operator bool() const noexcept
  {
    return this->byte_storage_ != nullptr;
  }

 private:
  usize compute_hash_value() const;

  slot_offset_type trim_pos_ = 0;
  slot_offset_type flush_pos_ = 0;
  slot_offset_type commit_pos_ = 0;
  std::shared_ptr<u8[]> byte_storage_{nullptr};
  usize hash_value_ = this->compute_hash_value();
};

// Two snapshots are equivalent if all the slot offset position variables are the same and the
// _contents_ of the data buffer is equal.
//
bool operator==(const LogDeviceSnapshot& l, const LogDeviceSnapshot& r);

usize hash_value(const LogDeviceSnapshot& s);

}  // namespace llfs

#endif  // LLFS_LOG_DEVICE_SNAPSHOT_HPP
