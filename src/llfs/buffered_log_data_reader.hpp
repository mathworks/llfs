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

/** \brief Implementation of LogDevice::Reader that reads a static segment of data from a
 * `ConstBuffer`.
 */
class BufferedLogDataReader : public LogDevice::Reader
{
 public:
  using ReaderEvent = LogDevice::ReaderEvent;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new Reader for the given data, which begins at the given log slot offset.
   */
  explicit BufferedLogDataReader(slot_offset_type slot_offset, const ConstBuffer& buffer) noexcept;

  /** \brief Disable copy/move construction.
   */
  BufferedLogDataReader(const BufferedLogDataReader&) = delete;

  /** \brief Disable copy/move assignment.
   */
  BufferedLogDataReader& operator=(const BufferedLogDataReader&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Always returns false (for BufferedLogDataReader).
   */
  bool is_closed() override;

  /** \brief The current log contents.  The memory returned by this method is a valid reflection of
   * this part of the log.  Even if `consume` invalidates some prefix of `data()`, the remaining
   * portion will still be valid. Likewise, once await returns Ok to indicate there is more data
   * ready to read, calling `data()` again will return the same memory with some extra at the end.
   */
  ConstBuffer data() override;

  /** \brief The current offset in bytes of this reader, relative to the start of the log.
   */
  slot_offset_type slot_offset() override;

  /** \brief Releases ownership of some prefix of `data()` (possibly all of it).  See description of
   * `data()` for more details.
   *
   * The passed `byte_count` is automatically truncated to the current remaining size of data.
   */
  void consume(usize byte_count) override;

  /** \brief Wait for the log to reach the specified state.
   *
   * For BufferedLogDataReader, this function never blocks; if the event has already happened, it
   * immediately returns OkStatus(), otherwise it returns `batt::StatusCode::kOutOfRange`.
   */
  Status await(ReaderEvent event) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  /** \brief The slot offset of the _current_ first byte of `this->buffer_`.
   */
  slot_offset_type slot_offset_;

  /** \brief The remaining (unconsumed) data for this reader.
   */
  ConstBuffer buffer_;
};

}  // namespace llfs

#endif  // LLFS_BUFFERED_LOG_DATA_READER_HPP
