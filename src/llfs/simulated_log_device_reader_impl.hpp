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
  Impl& impl_;

  slot_offset_type slot_offset_;

  const LogReadMode mode_;

  usize data_size_ = 0;

  std::shared_ptr<Impl::CommitChunk> chunk_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_READER_IMPL_HPP
