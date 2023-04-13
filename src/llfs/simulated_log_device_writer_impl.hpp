//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMULATED_LOG_DEVICE_WRITER_IMPL_HPP
#define LLFS_SIMULATED_LOG_DEVICE_WRITER_IMPL_HPP

#include <llfs/config.hpp>
//
#include <llfs/log_device.hpp>
#include <llfs/simulated_log_device_impl.hpp>

#include <memory>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SimulatedLogDevice::Impl::WriterImpl : public LogDevice::Writer
{
 public:
  explicit WriterImpl(Impl& impl) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
  StatusOr<MutableBuffer> prepare(usize byte_count, usize head_room) override;

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
  Impl& impl_;

  std::shared_ptr<Impl::CommitChunk> prepared_chunk_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_WRITER_IMPL_HPP
