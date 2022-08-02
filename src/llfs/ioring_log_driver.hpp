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

#include <llfs/basic_log_storage_driver.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_config.hpp>
#include <llfs/ioring_log_driver_options.hpp>
#include <llfs/ioring_log_flush_op.hpp>
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

template <template <typename> class FlushOpImpl>
class BasicIoRingLogDriver;

using IoRingLogDriver = BasicIoRingLogDriver<BasicIoRingLogFlushOp>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <template <typename> class FlushOpImpl>
class BasicIoRingLogDriver
{
 public:
  using Self = BasicIoRingLogDriver;
  using FlushOp = FlushOpImpl<Self>;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static usize disk_size_required_for_log_size(u64 logical_size, u64 block_size)
  {
    const u64 block_capacity = block_size - sizeof(PackedLogPageHeader);
    const u64 block_count =
        (logical_size + block_capacity - 1) / block_capacity + (1 /* for wrap-around */);

    return block_size * block_count;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Metrics {
    LatencyMetric flush_write_latency;
    CountMetric<u64> logical_bytes_flushed{0};
    CountMetric<u64> physical_bytes_flushed{0};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BasicIoRingLogDriver(LogStorageDriverContext& context, int fd,
                                const IoRingLogConfig& config,
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
    return await_slot_offset(flush_pos, this->flush_pos_);
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
    return this->file_.close();
  }

  void halt();

  void join();

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  usize pages_per_block() const
  {
    return this->config_.pages_per_block();
  }

  usize block_size() const
  {
    return this->block_size_;
  }

  usize block_capacity() const
  {
    return this->block_capacity_;
  }

  usize block_count() const
  {
    return this->block_count_;
  }

  u64 physical_size() const
  {
    return this->log_end_ - this->log_start_;
  }

  usize queue_depth() const
  {
    return this->queue_depth_;
  }

  u64 block_0_file_offset() const
  {
    return this->log_start_;
  }

  usize index_of_flush_op(const FlushOp& flush_op) const
  {
    return std::distance(this->flush_ops_.data(), &flush_op);
  }

  u64 file_offset_from_slot(slot_offset_type offset) const
  {
    const u64 logical_block_index = offset / this->block_capacity();
    const u64 physical_block_index = logical_block_index % this->block_count();

    return this->block_0_file_offset() + physical_block_index * this->block_size();
  }

  // Return the index of the highest-offset logical block that is bounded by `slot_upper_bound`.
  //
  usize logical_block_for_slot_upper_bound(slot_offset_type slot_upper_bound) const;

  // Return the index of the highest-offset physical block that is bounded by `slot_upper_bound`.
  //
  usize physical_block_for_slot_upper_bound(slot_offset_type slot_upper_bound) const;

  // Return the SlotRange of the highest-offset block that is bounded by `slot_upper_bound`.
  //
  SlotRange block_slot_range_for_upper_bound(slot_offset_type slot_upper_bound) const;

  // Return the index of the flush op object assigned to handle flushing from the given log offset.
  //
  usize flush_op_index_for_slot_upper_bound(slot_offset_type slot_upper_bound) const;

  // Return the known upper bound flush position for the given flush operation.
  //
  slot_offset_type get_flush_op_durable_upper_bound(usize flush_op_index) const;

  // Return the next flush_op index.
  //
  usize get_next_flush_op_index(usize index) const;

  void wait_for_commit(slot_offset_type least_upper_bound);

  void poll_flush_state();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ConstBuffer get_data(slot_offset_type slot_offset) const
  {
    return this->context_.buffer_.get(slot_offset);
  }

  template <typename Handler>
  void async_write_some(i64 log_offset, const ConstBuffer& data, i32 buf_index, Handler&& handler)
  {
    this->file_.async_write_some_fixed(log_offset, data, buf_index, BATT_FORWARD(handler));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  using SlotOffsetHeap = boost::heap::d_ary_heap<slot_offset_type,                   //
                                                 boost::heap::arity<2>,              //
                                                 boost::heap::compare<SlotGreater>,  //
                                                 boost::heap::mutable_<true>         //
                                                 >;

  class FlushState
  {
   public:
    explicit FlushState(IoRingLogDriver* driver) noexcept;

    void poll(IoRingLogDriver* driver);

    slot_offset_type get_flushed_upper_bound() const;

   private:
    slot_offset_type flushed_upper_bound_;
    usize next_flush_op_index_;
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

  // The physical log configuration plus cached derived values.
  //
  const IoRingLogConfig config_;
  const u64 log_start_ = this->config_.physical_offset;
  const u64 log_end_ = this->config_.physical_offset + this->config_.physical_size;
  const usize block_size_ = this->config_.block_size();
  const usize block_capacity_ = this->config_.block_capacity();
  const usize block_count_ = this->config_.block_count();

  // Runtime options for the log driver plus cached derived values.
  //
  const IoRingLogDriverOptions options_;
  const std::string& name_ = this->options_.name;
  const usize queue_depth_ = this->options_.queue_depth();
  const usize queue_depth_mask_ = this->options_.queue_depth_mask();

  // The IoRing and file used to do log flushing.
  //
  IoRing ioring_;
  IoRing::File file_;

  // Set to true once when halt is first called; used to detect unexpected/premature exit of
  // background tasks.
  //
  std::atomic<bool> halt_requested_{false};

  // The standard log state variables.
  //
  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> flush_pos_{0};
  batt::Watch<slot_offset_type> commit_pos_{0};

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

#endif  // LLFS_IORING_LOG_DRIVER_HPP
