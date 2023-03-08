//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BUFFERED_LOG_DATA_READER_HPP
#define LLFS_BUFFERED_LOG_DATA_READER_HPP

#include <llfs/log_device.hpp>

namespace llfs {

class BufferedLogDataReader : public LogDevice::Reader
{
 public:
  using ReaderEvent = LogDevice::ReaderEvent;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BufferedLogDataReader(slot_offset_type slot_offset, const ConstBuffer& buffer) noexcept;

  BufferedLogDataReader(const BufferedLogDataReader&) = delete;
  BufferedLogDataReader& operator=(const BufferedLogDataReader&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
  Status await(ReaderEvent event) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  slot_offset_type slot_offset_;
  ConstBuffer buffer_;
};

}  // namespace llfs

#endif  // LLFS_BUFFERED_LOG_DATA_READER_HPP
