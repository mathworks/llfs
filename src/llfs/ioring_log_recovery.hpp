//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_RECOVERY_HPP
#define LLFS_IORING_LOG_RECOVERY_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_config.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_log_page_buffer.hpp>
#include <llfs/ring_buffer.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_interval_map.hpp>
#include <llfs/status.hpp>

#include <functional>

namespace llfs {

/*! \brief Manages log data and state recovery from on-disk information.
 */
class IoRingLogRecovery
{
 public:
  using ReadDataFn = std::function<Status(i64 file_offset, MutableBuffer dst_buffer)>;

  explicit IoRingLogRecovery(const IoRingLogConfig& config, RingBuffer& ring_buffer,
                             ReadDataFn&& read_data);

  Status run();

  slot_offset_type get_trim_pos() const
  {
    return this->trim_pos_.value_or(0);
  }

  slot_offset_type get_flush_pos() const
  {
    return this->flush_pos_.value_or(this->get_trim_pos());
  }

 private:
  Status validate_block() const;

  MutableBuffer block_buffer();

  const PackedLogPageHeader& block_header() const;

  ConstBuffer block_payload() const;

  void recover_block_data();

  void recover_flush_pos();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The physical log configuration plus cached derived values.
  //
  const IoRingLogConfig config_;

  // The destination log ring buffer we are trying to reconstruct.
  //
  RingBuffer& ring_buffer_;

  // Callback used to read data into the block storage buffer; passed in at creation time.
  //
  ReadDataFn read_data_;

  // The maximum trim_pos field value read from all valid block headers.
  //
  Optional<slot_offset_type> trim_pos_;

  // The true slot upper bound for valid flushed data.
  //
  Optional<slot_offset_type> flush_pos_;

  // The memory used to load individual log blocks.
  //
  std::unique_ptr<PackedLogPageBuffer[]> block_storage_;

  SlotIntervalMap latest_slot_range_;

  SlotIntervalMap committed_data_;
};

}  // namespace llfs

#endif  // LLFS_IORING_LOG_RECOVERY_HPP
