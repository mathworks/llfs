//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMULATED_LOG_DEVICE_READER_IMPL_HPP
#define LLFS_SIMULATED_LOG_DEVICE_READER_IMPL_HPP

#include <llfs/config.hpp>
//
#include <llfs/simulated_log_device_impl.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SimulatedLogDevice::Impl::ReaderImpl : public LogDevice::Reader
{
 public:
  explicit ReaderImpl(Impl& impl, slot_offset_type slot_offset, LogReadMode mode) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
  slot_offset_type slot_offset() override
  {
    return this->slot_offset_;
  }

  // Releases ownership of some prefix of `data()` (possibly all of it).  See description of
  // `data()` for more details.
  //
  void consume(usize byte_count) override;

  // Wait for the log to reach the specified state.
  //
  Status await(ReaderEvent event) override;

 private:
  // The simulated log impl that created this reader.
  //
  Impl& impl_;

  // The current position of the reader.
  //
  slot_offset_type slot_offset_;

  // Controls whether the reader can see committed data, or only flushed.
  //
  const LogReadMode mode_;

  // The size of the last chunk returned by `this->data()`; this may be less than the chunk size, if
  // the slot has been partially trimmed.
  //
  usize data_size_ = 0;

  // Only valid between calls to data() and consume(); the chunk currently being read by the user of
  // this reader.
  //
  std::shared_ptr<Impl::CommitChunk> chunk_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_READER_IMPL_HPP
