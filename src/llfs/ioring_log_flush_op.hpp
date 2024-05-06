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
#include <llfs/ioring_log_device_storage.hpp>
#include <llfs/ioring_log_driver_fwd.hpp>
#include <llfs/log_block_calculator.hpp>
#include <llfs/metrics.hpp>
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

using IoRingLogFlushOp = BasicIoRingLogFlushOp<
    BasicIoRingLogDriver<BasicIoRingLogFlushOp, DefaultIoRingLogDeviceStorage>>;

template <typename DriverImpl>
class BasicIoRingLogFlushOp
{
 public:
  enum struct WritingPart {
    kHead = 0,
    kTail = 1,
    kTrimPos = 2,
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

  slot_offset_type durable_flush_pos() const
  {
    return this->durable_flush_pos_;
  }

  MutableBuffer get_buffer() const;

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  /** \brief Returns the physical log block index to which this op is currently flushing.
   */
  LogBlockCalculator::PhysicalBlockIndex get_current_log_block_index() const
  {
    PackedLogPageHeader* const header = this->get_header();
    return LogBlockCalculator::PhysicalBlockIndex{
        this->driver_->calculate().physical_block_index_from(
            SlotLowerBoundAt{header->slot_offset})};
  }

  /** \brief Returns one past the physical log block index to which this op is currently flushing.
   */
  LogBlockCalculator::PhysicalBlockIndex get_current_block_upper_bound() const
  {
    return LogBlockCalculator::PhysicalBlockIndex{this->get_current_log_block_index() + 1u};
  }

  /** \brief Returns the physical log block index of the block immediately after this op's current
   * flush destination; NOTE: this is *not* the next block index that will be used by this op (which
   * depends on the queue depth, i.e. the number of concurrent flush ops), but rather the next block
   * in the log's physical layout (with wrap).
   */
  LogBlockCalculator::PhysicalBlockIndex get_next_log_block_index() const
  {
    PackedLogPageHeader* const header = this->get_header();
    return LogBlockCalculator::PhysicalBlockIndex{
        this->driver_->calculate().physical_block_index_from(
            SlotLowerBoundAt{header->slot_offset + this->block_capacity_})};
  }

  /** \brief Returns true iff the header for this op's current log block is known to have been
   * written at least once.
   */
  bool is_current_log_block_initialized() const
  {
    return this->driver_->get_init_upper_bound() > this->get_current_log_block_index();
  }

  /** \brief Returns true iff the header for the block immediately after this op's current block is
   * known to have been written at least once.
   */
  bool is_next_log_block_initialized() const
  {
    return this->driver_->get_init_upper_bound() > this->get_next_log_block_index();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const char* debug_info_message() const
  {
    return this->debug_info_message_;
  }

  std::atomic<bool> quiet_failure_logging{false};

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

  // Should be called first after each async write... returns true if the caller should NOT continue
  // processing the result.
  //
  bool handle_errors(const StatusOr<i32>& result, WritingPart writing_part);

  // If the trim_pos needs to be updated for this block before writing tail pages in order to avoid
  // possible data loss, this function will return the trim_pos value to be written.  If there is no
  // risk of such data loss, it returns `None`.
  //
  Optional<slot_offset_type> need_to_update_trim_pos();

  // Start writing the first (head) block for the sake of updating the durable trim pos.  This
  // function will temporarily set the commit_size of this block to zero and, if successful, call
  // flush_tail() afterwards.
  //
  void flush_trim_pos(slot_offset_type known_trim_pos);

  void handle_flush_trim_pos(const StatusOr<i32>& result);

  auto get_flush_trim_pos_handler()
  {
    return make_custom_alloc_handler(this->handler_memory_, [this](const StatusOr<i32>& result) {
      this->handle_flush_trim_pos(result);
    });
  }
  //----- --- -- -  -  -   -

  // Suspends this flush op until the driver's init_upper_bound is different from last_known_value.
  // This op will resume inside handle_init_upper_bound_changed.
  //
  void await_init_upper_bound_changed(usize last_known_value);

  // Invoked to resume this flush op when the driver's init_upper_bound has changed.  This is used
  // to make sure that we never commit a full block (by flushing the header) until we are sure that
  // the next physical block has been initialized, so that recovery doesn't get confused.
  //
  void handle_init_upper_bound_changed(const StatusOr<usize>& result);

  // Returns a bound version of handle_init_upper_bound_changed with custom memory allocation that
  // uses this object's inline scratch buffer (this->handler_memory_).
  //
  auto get_init_upper_bound_changed_handler()
  {
    return make_custom_alloc_handler(this->handler_memory_, [this](const StatusOr<usize>& result) {
      this->handle_init_upper_bound_changed(result);
    });
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Metrics metrics_;

  DriverImpl* driver_ = nullptr;

  std::unique_ptr<PackedLogPageBuffer[]> page_block_;

  // The highest slot number confirmed to be written to device by this op.
  //
  slot_offset_type durable_flush_pos_ = 0;

  // The known commit_size written to the device by this op.
  //
  u64 durable_commit_size_ = 0;

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

  // Temporary storage to save the `commit_size` field of the header while writing on behalf of
  // flush_trim_pos().
  //
  Optional<u64> saved_commit_size_;

  // A human-readable representation of the state of this object for diagnostic purposes.
  //
  const char* debug_info_message_ = "created";
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLogPageHeader), 64);

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_LOG_FLUSH_OP_HPP
