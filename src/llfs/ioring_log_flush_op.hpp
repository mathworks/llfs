//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_FLUSH_OP_HPP
#define LLFS_IORING_LOG_FLUSH_OP_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/data_layout.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring.hpp>
#include <llfs/log_block_calculator.hpp>
#include <llfs/packed_log_page_buffer.hpp>
#include <llfs/packed_log_page_header.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <batteries/metrics/metric_collectors.hpp>

#include <batteries/async/handler.hpp>
#include <batteries/async/watch.hpp>

namespace llfs {

template <typename DriverImpl>
class BasicIoRingLogFlushOp;

template <template <typename> class FlushOpImpl>
class BasicIoRingLogDriver;

using IoRingLogFlushOp = BasicIoRingLogFlushOp<BasicIoRingLogDriver<BasicIoRingLogFlushOp>>;

template <typename DriverImpl>
class BasicIoRingLogFlushOp
{
 public:
  enum struct WritingPart {
    kHead = 0,
    kTail = 1,
  };

  struct Metrics {
    LatencyMetric write_latency;
    CountMetric<u64> bytes_written{0};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  BasicIoRingLogFlushOp() = default;

  BasicIoRingLogFlushOp(const BasicIoRingLogFlushOp&) = delete;
  BasicIoRingLogFlushOp& operator=(const BasicIoRingLogFlushOp&) = delete;

  ~BasicIoRingLogFlushOp() noexcept;

  //-----

  void initialize(DriverImpl* driver);

  void activate();

  //-----

  void handle_commit(slot_offset_type known_commit_pos);

  //-----

  usize self_index() const;

  PackedLogPageHeader* get_header() const;

  // Copy data from the device ring buffer to this->page_block; return true if some data was copied.
  //
  bool fill_buffer(slot_offset_type known_commit_pos);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  slot_offset_type flush_pos() const
  {
    return this->flush_pos_;
  }

  MutableBuffer get_buffer() const;

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

 private:
  // Return the committed data portion of the block buffer, aligned to 512-byte boundaries.
  //
  ConstBuffer get_writable_data() const;

  // Update the log offset pointers from the driver.
  //
  void update_log_positions();

  // Start writing the first atomic write block.
  //
  void flush_head();

  void handle_flush_head(const StatusOr<i32>& result);

  auto get_flush_head_handler()
  {
    return make_custom_alloc_handler(this->handler_memory_, [this](const StatusOr<i32>& result) {
      this->handle_flush_head(result);
    });
  }

  ConstBuffer unflushed_tail_data() const;

  // Start writing everything after the first atomic write block.
  //
  void flush_tail();

  void handle_flush_tail(const StatusOr<i32>& result);

  auto get_flush_tail_handler()
  {
    return make_custom_alloc_handler(this->handler_memory_, [this](const StatusOr<i32>& result) {
      this->handle_flush_tail(result);
    });
  }

  // Should be called first after each async write... returns true if there was a failure that
  // forces this object to stop.
  //
  bool check_for_fatal_failure(const StatusOr<i32>& result, WritingPart writing_part);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Metrics metrics_;

  DriverImpl* driver_ = nullptr;

  std::unique_ptr<PackedLogPageBuffer[]> page_block_;

  slot_offset_type flush_pos_ = 0;

  // Dedicated static memory buffer to lower the overhead of asynchronous calls.
  //
  batt::HandlerMemory<128> handler_memory_;

  // The offset within the log file to which this op's current page should be flushed.
  //
  i64 file_offset_ = 0;

  // `flushed_tail_range_` and `tail_write_range_` are byte offsets relative to the start of the
  // current block.
  //
  batt::Interval<usize> flushed_tail_range_{0, 0};
  Optional<batt::Interval<usize>> tail_write_range_ = None;

  u64 most_recent_tail_flush_size_ = 0;

  // Cached value from the driver.
  //
  u64 block_capacity_ = 0;

  // Active only during an asynchronous write (flush) operation.
  //
  Optional<LatencyTimer> write_timer_;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLogPageHeader), 64);

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_LOG_FLUSH_OP_HPP
