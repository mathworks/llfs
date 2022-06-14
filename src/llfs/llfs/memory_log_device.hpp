#pragma once
#ifndef LLFS_MEMORY_LOG_DEVICE_HPP
#define LLFS_MEMORY_LOG_DEVICE_HPP

#include <llfs/basic_ring_buffer_device.hpp>
#include <llfs/log_device.hpp>
#include <llfs/ring_buffer.hpp>

#include <batteries/async/watch.hpp>

namespace llfs {

class MemoryLogDeviceFactory;

class MemoryLogStorageDriver /*Impl*/
{
 public:
  explicit MemoryLogStorageDriver(LogStorageDriverContext&) noexcept
  {
  }

  //----

  Status set_trim_pos(slot_offset_type trim_pos)
  {
    this->trim_pos_.set_value(trim_pos);

    return OkStatus();
  }

  slot_offset_type get_trim_pos() const
  {
    return this->trim_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type min_offset)
  {
    return this->trim_pos_.await_true([&](slot_offset_type latest_pos) {
      return !slot_less_than(latest_pos, min_offset);
    });
  }

  //----

  slot_offset_type get_flush_pos() const
  {
    return this->get_commit_pos();
  }

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type min_offset)
  {
    return this->await_commit_pos(min_offset);
  }

  //----

  Status set_commit_pos(slot_offset_type commit_pos)
  {
    this->commit_flush_pos_.set_value(commit_pos);
    return OkStatus();
  }

  slot_offset_type get_commit_pos() const
  {
    return this->commit_flush_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type min_offset)
  {
    return this->commit_flush_pos_.await_true([&](slot_offset_type latest_pos) {
      return !slot_less_than(latest_pos, min_offset);
    });
  }

  //----

  Status close()
  {
    this->trim_pos_.close();
    this->commit_flush_pos_.close();

    return OkStatus();
  }

 private:
  friend class LogTruncateAccess;

  void truncate(slot_offset_type truncate_pos)
  {
    this->commit_flush_pos_.set_value(truncate_pos);
  }

  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> commit_flush_pos_{0};
};

class MemoryLogDevice : public BasicRingBufferDevice</*Impl=*/MemoryLogStorageDriver>
{
 public:
  explicit MemoryLogDevice(usize size) noexcept
      : BasicRingBufferDevice<MemoryLogStorageDriver>{RingBuffer::TempFile{size}}
  {
  }

  void restore_snapshot(const LogDeviceSnapshot& snapshot, LogReadMode mode);
};

class MemoryLogDeviceFactory : public LogDeviceFactory
{
 public:
  explicit MemoryLogDeviceFactory(slot_offset_type size) noexcept : size_{size}
  {
  }

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override
  {
    auto instance = std::make_unique<MemoryLogDevice>(this->size_);

    auto scan_status =
        scan_fn(*instance->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable));

    BATT_REQUIRE_OK(scan_status);

    // Truncate the log at the indicated point.
    //
    this->truncate(instance->driver().impl(), *scan_status);

    return instance;
  }

 private:
  slot_offset_type size_;
};

}  // namespace llfs

#endif  // LLFS_MEMORY_LOG_DEVICE_HPP
