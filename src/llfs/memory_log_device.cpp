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

}  // namespace llfs
