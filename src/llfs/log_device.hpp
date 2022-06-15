//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// LogDevice - an interface for WAL storage.
//
#pragma once
#ifndef LLFS_LOG_DEVICE_HPP
#define LLFS_LOG_DEVICE_HPP

#include <llfs/buffer.hpp>
#include <llfs/slot.hpp>

namespace llfs {

struct SlotUpperBoundAt {
  slot_offset_type offset;
};
struct SlotLowerBoundAt {
  slot_offset_type offset;
};
struct BytesAvailable {
  slot_offset_type size;
};

enum struct LogReadMode : unsigned {
  kInconsistent = 0,
  kSpeculative = 1,
  kDurable = 2,
};
constexpr unsigned kNumLogReadModes = 3;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class LogDevice
{
 protected:
  LogDevice() = default;

 public:
  using ReaderEvent = std::variant<SlotUpperBoundAt, BytesAvailable>;
  using WriterEvent = std::variant<SlotLowerBoundAt, BytesAvailable>;

  class Reader;
  class Writer;

  LogDevice(const LogDevice&) = delete;
  LogDevice& operator=(const LogDevice&) = delete;

  virtual ~LogDevice() = default;

  // The maximum capacity in bytes of this log device.
  //
  virtual u64 capacity() const = 0;

  // The current size of all committed data in the log.
  //
  virtual u64 size() const = 0;

  // Convenience; the current available space.
  //
  virtual u64 space() const
  {
    return this->capacity() - this->size();
  }

  // Trim the log at `slot_lower_bound`.  May not take effect immediately if there are active
  // Readers whose slot_offset is below `slot_lower_bound`.
  //
  virtual Status trim(slot_offset_type slot_lower_bound) = 0;

  // Create a new reader.
  //
  virtual std::unique_ptr<Reader> new_reader(Optional<slot_offset_type> slot_lower_bound,
                                             LogReadMode mode) = 0;

  // Returns the current active slot range for the log.  `mode` determines whether the upper bound
  // will be the flushed or committed upper bound.
  //
  virtual SlotRange slot_range(LogReadMode mode) = 0;

  // There can be only one Writer at a time.
  //
  virtual Writer& writer() = 0;

  virtual Status close() = 0;

  virtual Status sync(LogReadMode mode, SlotUpperBoundAt event) = 0;

  // Convenience: wait for kSpeculative to catch up to kDurable.
  //
  Status flush()
  {
    return this->sync(LogReadMode::kDurable,
                      SlotUpperBoundAt{
                          .offset = this->slot_range(LogReadMode::kSpeculative).upper_bound,
                      });
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
using LogScanFn = std::function<StatusOr<slot_offset_type>(LogDevice::Reader& reader)>;

class LogTruncateAccess
{
 private:
  friend class LogDeviceFactory;

  template <typename Obj, typename... Args>
  static decltype(auto) truncate(Obj* obj, Args&&... args)
  {
    return obj->truncate(BATT_FORWARD(args)...);
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class LogDeviceFactory
{
 public:
  LogDeviceFactory(const LogDeviceFactory&) = delete;
  LogDeviceFactory& operator=(const LogDeviceFactory&) = delete;

  virtual ~LogDeviceFactory() = default;

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
  virtual StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) = 0;

 protected:
  LogDeviceFactory() = default;

  template <typename LogDriverImpl>
  void truncate(LogDriverImpl& driver_impl, slot_offset_type pos)
  {
    LogTruncateAccess::truncate(&driver_impl, pos);
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class LogDevice::Reader
{
 protected:
  Reader() = default;

 public:
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  virtual ~Reader() = default;

  // Check whether the log device is closed.
  //
  virtual bool is_closed() = 0;

  // The current log contents.  The memory returned by this method is a valid reflection of this
  // part of the log.  Even if `consume` invalidates some prefix of `data()`, the remaining portion
  // will still be valid. Likewise, once await returns Ok to indicate there is more data ready to
  // read, calling `data()` again will return the same memory with some extra at the end.
  //
  virtual ConstBuffer data() = 0;

  // The current offset in bytes of this reader, relative to the start of the log.
  //
  virtual slot_offset_type slot_offset() = 0;

  // Releases ownership of some prefix of `data()` (possibly all of it).  See description of
  // `data()` for more details.
  //
  virtual void consume(usize byte_count) = 0;

  // Wait for the log to reach the specified state.
  //
  virtual Status await(ReaderEvent event) = 0;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class LogDevice::Writer
{
 protected:
  Writer() = default;

 public:
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  virtual ~Writer() = default;

  // The current available space.
  //
  virtual u64 space() const = 0;

  // The next slot offset to be written.  Updated by `commit`.
  //
  virtual slot_offset_type slot_offset() = 0;

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
  virtual StatusOr<MutableBuffer> prepare(usize byte_count, usize head_room = 0) = 0;

  // Commits `byte_count` bytes; does not guarantee that these bytes are durable yet; a Reader may
  // be created to await the flush of a certin slot offset.
  //
  // Returns the new end (slot upper bound) of the log.
  //
  virtual StatusOr<slot_offset_type> commit(usize byte_count) = 0;

  // Wait for the log to reach the specified state.
  //
  virtual Status await(WriterEvent event) = 0;
};

}  // namespace llfs

#endif  // LLFS_LOG_DEVICE_HPP
