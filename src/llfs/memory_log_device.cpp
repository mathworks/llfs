//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/memory_log_device.hpp>
//

#include <llfs/log_device_snapshot.hpp>

namespace llfs {

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
// class MemoryLogStorageDriver
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemoryLogStorageDriver::is_auto_flush() const noexcept
{
  return this->flush_pos_ == &this->commit_pos_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryLogStorageDriver::set_auto_flush(bool on) noexcept
{
  if (on == this->is_auto_flush()) {
    // No change.
    //
    return;
  }

  this->sync_flush_pos();
  if (on) {
    this->flush_pos_ = &this->commit_pos_;
  } else {
    this->flush_pos_ = &this->manual_flush_pos_;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryLogStorageDriver::sync_flush_pos() noexcept
{
  this->manual_flush_pos_.set_value(this->commit_pos_.get_value());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemoryLogStorageDriver::unflushed_size() noexcept
{
  if (this->is_auto_flush()) {
    return 0;
  }
  return slot_distance(this->get_flush_pos(), this->get_commit_pos());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
isize MemoryLogStorageDriver::advance_flush_pos(isize offset_delta) noexcept
{
  if (this->is_auto_flush()) {
    return 0;
  }
  offset_delta = slot_max(0, offset_delta);
  offset_delta = slot_min(offset_delta, this->unflushed_size());
  this->manual_flush_pos_.fetch_add(offset_delta);
  return offset_delta;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type MemoryLogStorageDriver::flush_up_to_offset(slot_offset_type offset) noexcept
{
  if (this->is_auto_flush()) {
    return this->get_flush_pos();
  }

  // Don't ever go past the commit_pos!
  //
  offset = slot_min(offset, this->get_commit_pos());

  return clamp_min_slot(this->manual_flush_pos_, offset);
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
// class MemoryLogDevice
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemoryLogDevice::MemoryLogDevice(usize size) noexcept
    : BasicRingBufferLogDevice<MemoryLogStorageDriver>{RingBuffer::TempFile{size}}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryLogDevice::restore_snapshot(const LogDeviceSnapshot& snapshot, LogReadMode mode)
{
  RingBuffer& ring_buffer = this->LogStorageDriverContext::buffer_;

  BATT_CHECK_LE(snapshot.size(), ring_buffer.size());

  this->driver().set_trim_pos(snapshot.trim_pos()).IgnoreError();
  if (mode == LogReadMode::kDurable) {
    this->driver().set_commit_pos(snapshot.flush_pos()).IgnoreError();
  } else {
    this->driver().set_commit_pos(snapshot.commit_pos()).IgnoreError();
  }

  MutableBuffer dst = ring_buffer.get_mut(snapshot.trim_pos());

  std::memcpy(dst.data(), snapshot.bytes(), snapshot.size());
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
// class MemoryLogDeviceFactory
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemoryLogDeviceFactory::MemoryLogDeviceFactory(slot_offset_type size) noexcept
    : size_{size}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<LogDevice>> MemoryLogDeviceFactory::open_log_device(
    const LogScanFn& scan_fn) /*override*/
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

}  // namespace llfs
