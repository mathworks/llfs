//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DRIVER_HPP
#define LLFS_IORING_LOG_DRIVER_HPP

#include <llfs/config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/basic_log_storage_driver.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_config.hpp>
#include <llfs/ioring_log_device_storage.hpp>
#include <llfs/ioring_log_driver_fwd.hpp>
#include <llfs/ioring_log_driver_options.hpp>
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/log_block_calculator.hpp>
#include <llfs/metrics.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_log_page_header.hpp>

#include <batteries/async/watch.hpp>

BATT_SUPPRESS("-Wunused-parameter")

#include <boost/heap/d_ary_heap.hpp>
#include <boost/heap/policies.hpp>

BATT_UNSUPPRESS()

#include <atomic>
#include <string>
#include <vector>

namespace llfs {

using IoRingLogDriver = BasicIoRingLogDriver<BasicIoRingLogFlushOp, DefaultIoRingLogDeviceStorage>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <template <typename> class FlushOpImpl, typename StorageT>
class BasicIoRingLogDriver
{
 public:
  using Self = BasicIoRingLogDriver;
  using FlushOp = FlushOpImpl<Self>;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static usize disk_size_required_for_log_size(u64 logical_size, u64 block_size)
  {
    return LogBlockCalculator::disk_size_required_for_log_size(logical_size, block_size);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Metrics {
    LatencyMetric flush_write_latency;
    CountMetric<u64> logical_bytes_flushed{0};
    CountMetric<u64> physical_bytes_flushed{0};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BasicIoRingLogDriver(LogStorageDriverContext& context,     //
                                batt::TaskScheduler& task_scheduler,  //
                                StorageT&& storage,                   //
                                const IoRingLogConfig& config,        //
                                const IoRingLogDriverOptions& options) noexcept;

  ~BasicIoRingLogDriver() noexcept;

  std::string_view name() const
  {
    return this->name_;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // A storage driver impl must provide these 9 methods.
  //
  Status set_trim_pos(slot_offset_type trim_pos);

  slot_offset_type get_trim_pos() const
  {
    return this->trim_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type trim_pos)
  {
    return await_slot_offset(trim_pos, this->trim_pos_);
  }

  //----

  // There is no set_flush_pos because it's up to the storage driver to flush log data and update
  // the flush pos in the background.

  slot_offset_type get_flush_pos() const
  {
    return this->flush_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type flush_pos)
  {
    StatusOr<slot_offset_type> status_or = await_slot_offset(flush_pos, this->flush_pos_);

    // If the flush_pos Watch has been closed, it may indicate there was an I/O error; check the
    // LogStorageDriverContext and report any error status we find.
    //
    if (status_or.status() == batt::StatusCode::kClosed) {
      BATT_REQUIRE_OK(this->context_.get_error_status());
    }

    return status_or;
  }

  //----

  Status set_commit_pos(slot_offset_type commit_pos);

  slot_offset_type get_commit_pos() const
  {
    return this->commit_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type commit_pos)
  {
    return await_slot_offset(commit_pos, this->commit_pos_);
  }

  //----

  Status open();

  Status close()
  {
    this->halt();
    this->join();
    return this->storage_close_status_;
  }

  void halt();

  void join();

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  const LogBlockCalculator& calculate() const
  {
    return this->calculate_;
  }

  usize index_of_flush_op(const FlushOp* flush_op) const
  {
    return std::distance(this->flush_ops_.data(), flush_op);
  }

  // Returns the known upper bound flush position for the given flush operation.
  //
  slot_offset_type get_flush_op_durable_upper_bound(usize flush_op_index) const
  {
    BATT_CHECK_LT(flush_op_index, this->flush_ops_.size());
    return this->flush_ops_[flush_op_index].durable_flush_pos();
  }

  void wait_for_commit(slot_offset_type least_upper_bound);

  void poll_flush_state();

  /** \brief Called by flush ops to report I/O failures.
   */
  void report_flush_error(Status error_status);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ConstBuffer get_data(slot_offset_type slot_offset) const
  {
    return this->context_.buffer_.get(slot_offset);
  }

  template <typename Handler>
  void async_write_some(i64 file_offset, const ConstBuffer& data, i32 buf_index, Handler&& handler)
  {
    this->storage_.async_write_some_fixed(file_offset, data, buf_index, BATT_FORWARD(handler));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void update_durable_trim_pos(slot_offset_type pos)
  {
    clamp_min_slot(this->durable_trim_pos_, pos);
  }

  slot_offset_type get_durable_trim_pos() const
  {
    return this->durable_trim_pos_.get_value();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Clamps the initialized physical block index upper bound to at least index.
   */
  void update_init_upper_bound(LogBlockCalculator::PhysicalBlockIndex index)
  {
    this->init_upper_bound_.clamp_min_value(index);
  }

  /** \brief Returns the current initialized physical block index upper bound.  This value is one
   * plus the highest known initialized physical block index.
   */
  usize get_init_upper_bound() const noexcept
  {
    return this->init_upper_bound_.get_value();
  }

  /** \brief Invokes the passed handler asynchronously as soon as the initialized upper bound
   *  (physical block index) is not equal to last_known_value.
   *
   * \param last_known_value The value to compare against the init_upper_bound_ Watch
   * \param handler A callable with signature void(StatusOr<usize>)
   */
  template <typename Handler = void(StatusOr<usize>)>
  void async_wait_init_upper_bound(usize last_known_value, Handler&& handler)
  {
    this->init_upper_bound_.async_wait(last_known_value, BATT_FORWARD(handler));
  }

 private:
  using SlotOffsetHeap = boost::heap::d_ary_heap<slot_offset_type,                   //
                                                 boost::heap::arity<2>,              //
                                                 boost::heap::compare<SlotGreater>,  //
                                                 boost::heap::mutable_<true>         //
                                                 >;

  class FlushState
  {
   public:
    explicit FlushState(BasicIoRingLogDriver* driver) noexcept;

    void poll(BasicIoRingLogDriver* driver);

    slot_offset_type get_flushed_upper_bound() const;

   private:
    slot_offset_type flushed_upper_bound_;
    LogBlockCalculator::FlushOpIndex next_flush_op_index_;
    slot_offset_type flush_op_slot_upper_bound_;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  Status read_log_data();

  void start_flush_task();

  void flush_task_main();

  void poll_commit_state();

  void handle_commit_pos_update(const StatusOr<slot_offset_type>& updated_commit_pos);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Used to access the RingBuffer.
  //
  LogStorageDriverContext& context_;

  batt::TaskScheduler& task_scheduler_;

  // The physical log configuration plus cached derived values.
  //
  const IoRingLogConfig config_;

  // Runtime options for the log driver plus cached derived values.
  //
  const IoRingLogDriverOptions options_;
  const std::string& name_ = this->options_.name;

  // Calculator for all derived values that depend on the config and options.
  //
  const LogBlockCalculator calculate_;

  // Interface to the low-level storage media used to save log data.
  //
  StorageT storage_;

  // The return value of this->storage_.close().
  //
  Status storage_close_status_ = batt::StatusCode::kUnknown;

  // Set to true once when halt is first called; used to detect unexpected/premature exit of
  // background tasks.
  //
  std::atomic<bool> halt_requested_{false};

  // The standard log state variables.
  //
  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> flush_pos_{0};
  batt::Watch<slot_offset_type> commit_pos_{0};

  // One past the highest physical log block index that has been initialized.
  //
  batt::Watch<usize> init_upper_bound_{0};

  // The highest trim_pos confirmed to be durably written to the log file by one of the flush ops.
  //
  batt::Watch<slot_offset_type> durable_trim_pos_{0};

  // The log is segmented into blocks of 512-byte pages; these blocks are flushed in parallel, by
  // the FlushOp objects below.
  //
  std::vector<FlushOp> flush_ops_;

  // A binary min-heap (priority queue) of flush ops that are blocked waiting for the commit pos
  // variable to advance; the heap is ordered by the value of commit_pos each op requires, so that
  // we can quickly activate the right ops when commit_pos advances, thus avoiding a thundering
  // herd bottle neck (the naive solution to this problem would be for each op to just wait
  // directly on `this->commit_pos_`, which doesn't scale).
  //
  SlotOffsetHeap waiting_for_commit_;

  batt::HandlerMemory<128> commit_handler_memory_;
  bool commit_pos_listener_active_ = false;
  bool inside_poll_commit_state_ = false;

  Optional<FlushState> flush_state_;
  Metrics metrics_;
  Optional<batt::Task> flush_task_;
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_IORING_LOG_DRIVER_HPP
