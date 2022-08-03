//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DRIVER_IPP
#define LLFS_IORING_LOG_DRIVER_IPP

#include <llfs/data_reader.hpp>
#include <llfs/ioring_log_driver.hpp>
#include <llfs/ioring_log_initializer.hpp>
#include <llfs/metrics.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/seq/boxed.hpp>
#include <batteries/stream_util.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline BasicIoRingLogDriver<FlushOpImpl>::BasicIoRingLogDriver(
    LogStorageDriverContext& context, int fd, const IoRingLogConfig& config,
    const IoRingLogDriverOptions& options) noexcept
    : context_{context}
    , config_{config}
    , options_{options}
    , calculate_{config, options}
    , ioring_{*IoRing::make_new(MaxQueueDepth{this->calculate().queue_depth() * 2})}
    , file_{this->ioring_, fd}
    , flush_ops_(this->calculate().queue_depth())

{
  const auto metric_name = [this](const std::string_view& property) {
    return batt::to_string("IoRingLogDevice_", this->name_, "_", property);
  };

  global_metric_registry()
      .add(metric_name("trim_pos"), this->trim_pos_)
      .add(metric_name("flush_pos"), this->flush_pos_)
      .add(metric_name("commit_pos"), this->commit_pos_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline BasicIoRingLogDriver<FlushOpImpl>::~BasicIoRingLogDriver() noexcept
{
  global_metric_registry()  //
      .remove(this->trim_pos_)
      .remove(this->flush_pos_)
      .remove(this->commit_pos_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline Status BasicIoRingLogDriver<FlushOpImpl>::open()
{
  // Read all blocks into the ring buffer.
  //
  Status data_read = this->read_log_data();
  BATT_REQUIRE_OK(data_read);

  Status fd_registered = this->file_.register_fd();
  BATT_REQUIRE_OK(fd_registered);

  // First initialize all ops in the queue pipeline to point back at this driver.  IMPORTANT: this
  // must be done before we allow the commit_pos to change!
  //
  for (auto& op : this->flush_ops_) {
    op.initialize(this);
  }

  Status buffers_registered = this->ioring_.register_buffers(
      as_seq(this->flush_ops_) | seq::map([](const IoRingLogFlushOp& op) {
        return op.get_buffer();
      }) |
      seq::boxed());

  BATT_REQUIRE_OK(buffers_registered);

  this->start_flush_task();

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline Status BasicIoRingLogDriver<FlushOpImpl>::read_log_data()
{
  BATT_CHECK(!this->flush_task_);

  // Each IoRing log device gets its own IoRing.  We must start it on a background thread to process
  // the read operations in the loop below.  After we are done, we must stop this background thread
  // and return the IoRing to its original state so that device driver can start up.
  //
  this->ioring_.on_work_started();

  // The background thread to process IO completions.
  //
  std::thread ioring_thread{[this] {
    LLFS_VLOG(1) << "ioring_thread started";
    this->ioring_.run().IgnoreError();
    LLFS_VLOG(1) << "ioring_thread returning";
  }};

  // Shut down the ioring and reset when we leave this scope.
  //
  const auto stop_ioring_thread = batt::finally([&] {
    LLFS_VLOG(1) << "Stopping ioring (IoRingLogDrvier::read_log_data)";
    this->ioring_.on_work_finished();
    ioring_thread.join();
    this->ioring_.reset();
  });

  // TODO [tastolfi 2022-02-09] this can be made more efficient; the block headers contain clues
  // that allow us to skip some work.  Also, we don't have to read entire blocks always, since
  // commit_size may be less than block_capacity.

  BATT_CHECK_EQ(this->config_.block_size() % sizeof(PackedLogPageBuffer), 0u);

  std::unique_ptr<PackedLogPageBuffer[]> block_storage{
      new PackedLogPageBuffer[this->config_.block_size() / sizeof(PackedLogPageBuffer)]};

  auto& block_header = block_storage[0].header;
  MutableBuffer block_buffer{(void*)block_storage.get(), this->config_.block_size()};
  ConstBuffer block_payload = block_buffer + sizeof(PackedLogPageHeader);

  BATT_CHECK_EQ(block_buffer.size(), this->config_.block_size());
  BATT_CHECK_EQ(block_payload.size(), this->config_.block_capacity());

  LLFS_VLOG(1) << "initial log recovery started";

  u64 bytes_copied = 0;
  bool end_of_data = false;

  u64 file_offset = this->config_.physical_offset;
  std::vector<llfs::slot_offset_type> recovered_commit_points;
  for (usize block_i = 0; block_i < this->config_.block_count(); ++block_i) {
    {
      const auto& hdr = block_header;
      LLFS_VLOG(1) << "reading log; " << BATT_INSPECT(block_i) << "/" << this->config_.block_count()
                   << BATT_INSPECT(file_offset) << BATT_INSPECT(this->config_.block_size())
                   << BATT_INSPECT(block_buffer.size()) << BATT_INSPECT(hdr.trim_pos)
                   << BATT_INSPECT(hdr.flush_pos) << BATT_INSPECT(hdr.commit_size)
                   << BATT_INSPECT(hdr.slot_offset);
    }

    Status read_status = this->file_.read_all(file_offset, block_buffer);
    BATT_REQUIRE_OK(read_status);

    if (block_header.magic != PackedLogPageHeader::kMagic) {
      // TODO [tastolfi 2022-02-09] specific error message/code
      return {batt::StatusCode::kDataLoss};
    }
    if (block_header.commit_size > this->config_.block_capacity()) {
      // TODO [tastolfi 2022-02-09] specific error message/code
      return {batt::StatusCode::kDataLoss};
    }
    // TODO [tastolfi 2022-02-09] validate CRC

    recovered_commit_points.emplace_back(block_header.commit_pos);
    if (!end_of_data) {
      clamp_min_slot(this->trim_pos_, block_header.trim_pos);
      clamp_min_slot(this->flush_pos_, block_header.flush_pos);
      clamp_min_slot(this->flush_pos_, block_header.slot_offset + block_header.commit_size);

      if (block_header.commit_size < this->calculate().block_capacity()) {
        end_of_data = true;
      }
    }

    if (block_header.commit_size > 0) {
      MutableBuffer dst = this->context_.buffer_.get_mut(block_header.slot_offset);
      std::memcpy(dst.data(), block_payload.data(), block_header.commit_size);
      bytes_copied += block_header.commit_size;
    }

    file_offset += this->config_.block_size();
  }

  // After opening, there is no unflushed data so commit_pos == flush_pos.
  //
  this->commit_pos_.set_value(this->flush_pos_.get_value());

  // Step forward through the log reading slot size headers until we find the true flushed upper
  // bound.
  //
  {
    slot_offset_type slots_upper_bound = this->trim_pos_.get_value();

    // If we can use a recovered commit point to skip ahead, do so.  (Sorting the recovered commit
    // points will take at least O(n), so just do a single pass scan through the offsets).
    //
    for (const slot_offset_type known_commit : recovered_commit_points) {
      // We are looking for the greatest commit offset that is not greater than the flushed upper
      // bound.
      //
      if (llfs::slot_less_than(slots_upper_bound, known_commit) &&
          !llfs::slot_less_than(this->flush_pos_.get_value(), known_commit)) {
        slots_upper_bound = known_commit;
      }
    }

    usize bytes_remaining = this->flush_pos_.get_value() - slots_upper_bound;
    for (;;) {
      DataReader reader{resize_buffer(this->get_data(slots_upper_bound), bytes_remaining)};
      const usize bytes_available_before = reader.bytes_available();
      Optional<u64> slot_body_size = reader.read_varint();
      if (!slot_body_size) {
        break;
      }
      const usize bytes_available_after = reader.bytes_available();
      const usize slot_header_size = bytes_available_before - bytes_available_after;
      const usize slot_size = slot_header_size + *slot_body_size;

      if (slot_size > bytes_remaining) {
        break;
      }
      slots_upper_bound += slot_size;
      bytes_remaining -= slot_size;
    }
    BATT_CHECK_LE(slots_upper_bound, this->flush_pos_.get_value())
        << "flush_pos should never increase as a result of this scan!";
    this->flush_pos_.set_value(slots_upper_bound);
  }

  // Initialize state according to recovered values.
  //
  this->flush_state_.emplace(this);

  LLFS_VLOG(1) << "log recovery complete; total: "
               << (this->config_.block_size() * this->config_.block_count()) << ";"
               << BATT_INSPECT(this->trim_pos_.get_value())
               << BATT_INSPECT(this->flush_pos_.get_value())
               << BATT_INSPECT(this->commit_pos_.get_value()) << BATT_INSPECT(bytes_copied);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline Status BasicIoRingLogDriver<FlushOpImpl>::set_trim_pos(slot_offset_type trim_pos)
{
  LLFS_VLOG(1) << "BasicIoRingLogDriver<FlushOpImpl>::set_trim_pos(" << trim_pos << ")"
               << BATT_INSPECT(this->trim_pos_.get_value());

  clamp_min_slot(this->trim_pos_, trim_pos);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline Status BasicIoRingLogDriver<FlushOpImpl>::set_commit_pos(slot_offset_type commit_pos)
{
  clamp_min_slot(this->commit_pos_, commit_pos);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::halt()
{
  const bool previously_halted = this->halt_requested_.exchange(true);
  if (!previously_halted) {
    LLFS_VLOG(1) << "BasicIoRingLogDriver<FlushOpImpl>::halt() - (trim="
                 << this->trim_pos_.get_value() << " flush=" << this->flush_pos_.get_value()
                 << " commit=" << this->commit_pos_.get_value() << ")";

    this->trim_pos_.close();
    this->flush_pos_.close();
    this->commit_pos_.close();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::join()
{
  if (this->flush_task_) {
    this->flush_task_->join();
    this->flush_task_ = None;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::start_flush_task()
{
  this->flush_task_.emplace(
      batt::Runtime::instance().schedule_task(),
      [this] {
        this->flush_task_main();
      },
      batt::to_string("IoRingLogDriver::flush_task(", this->name_, ")"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::flush_task_main()
{
  Status status = [&]() -> Status {
    const u64 total_size = this->calculate().physical_size();
    LLFS_VLOG(1) << "(driver=" << this->name_ << ") log physical size=0x" << std::hex << total_size
                 << " block_size=0x" << this->calculate().block_size() << " block_capacity=0x"
                 << this->calculate().block_capacity() << " queue_depth=" << std::dec
                 << this->calculate().queue_depth();
    LLFS_VLOG(1) << "(driver=" << this->name_
                 << ") buffer delay=" << this->options_.page_write_buffer_delay_usec << "usec";

    // Now tell the ops to start flushing data.  They will write in parallel but only one at a
    // time will perform an async_wait on `commit_pos_`, to prevent thundering herd bottlenecks.
    //
    for (auto& op : this->flush_ops_) {
      op.activate();
    }

    this->ioring_.on_work_started();
    this->poll_flush_state();
    this->poll_commit_state();

    // Run the IoRing on a background thread so as not to tie up the executor on which this task
    // is running.
    //
    batt::Watch<bool> done{false};
    std::thread io_thread{[this, &done] {
      LLFS_VLOG(1) << "(driver=" << this->name_ << ") invoking IoRing::run()";
      Status io_status = this->ioring_.run();
      if (!io_status.ok()) {
        LLFS_LOG_WARNING() << "(driver=" << this->name_
                           << ") IoRing::run() returned: " << io_status;
      }
      done.set_value(true);
    }};
    auto done_status = done.await_equal(true);
    BATT_CHECK(done_status.ok());
    io_thread.join();

    return OkStatus();
  }();

  if (!this->halt_requested_.load()) {
    LLFS_LOG_WARNING() << "[IoRingLogDriver::flush_task] exited unexpectedly with status="
                       << status;
  } else {
    LLFS_VLOG(1) << "[IoRingLogDriver::flush_task] exited with status=" << status;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::poll_flush_state()
{
  this->flush_state_->poll(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::poll_commit_state()
{
  // Don't recursively re-enter `poll_commit_state`.
  //
  if (this->inside_poll_commit_state_) {
    return;
  }
  this->inside_poll_commit_state_ = true;
  auto on_scope_exit = batt::finally([&] {
    this->inside_poll_commit_state_ = false;
  });

  // Return immediately if there are no flush ops waiting for `commit_pos_` to advance.
  //
  if (this->waiting_for_commit_.empty()) {
    return;
  }

  // Load the current value of commit_pos_; we are going to poll all flush ops that were waiting
  // for commit_pos to reach or exceed this value.
  //
  const slot_offset_type known_commit_pos = this->commit_pos_.get_value();
  LLFS_VLOG(2) << "(driver=" << this->name_ << ") observed commit_pos=" << known_commit_pos;

  // Keep notifying flush ops until we catch up or run out of waiters.
  //
  while (!this->waiting_for_commit_.empty()) {
    // Pop the next commit_pos minimum from the priority queue.
    //
    const slot_offset_type next_wait_pos = this->waiting_for_commit_.top();
    if (slot_less_than(known_commit_pos, next_wait_pos)) {
      // If we're already waiting for Watch notification on `this->commit_pos_`, then nothing to
      // do!
      //
      if (!this->commit_pos_listener_active_) {
        this->commit_pos_listener_active_ = true;

        LLFS_VLOG(2) << "(driver=" << this->name_ << ")" << BATT_INSPECT(known_commit_pos)
                     << BATT_INSPECT(next_wait_pos);

        // If we break out of this loop before we completely drain `waiting_for_commit_`, we must
        // start another wait operation.
        //
        this->commit_pos_.async_wait(          //
            known_commit_pos,                  //
            make_custom_alloc_handler(         //
                this->commit_handler_memory_,  //
                [this](const StatusOr<slot_offset_type>& updated_commit_pos) {
                  this->handle_commit_pos_update(updated_commit_pos);
                }));
      }
      break;
    }
    this->waiting_for_commit_.pop();

    // Figure out which op must have been waiting on the given pos.
    //
    const usize op_index = this->calculate().flush_op_index_from(SlotUpperBoundAt{next_wait_pos});

    LLFS_VLOG(2) << "(driver=" << this->name_ << ") commit_pos=" << known_commit_pos
                 << " waking op[" << op_index
                 << "], which was waiting on commit_pos >= " << next_wait_pos;

    this->flush_ops_[op_index].handle_commit(known_commit_pos);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::wait_for_commit(slot_offset_type least_upper_bound)
{
  this->waiting_for_commit_.push(least_upper_bound);
  this->poll_commit_state();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::handle_commit_pos_update(
    const StatusOr<slot_offset_type>& updated_commit_pos)
{
  // VERY IMPORTANT: we must execute the actual handler logic on the ioring thread to avoid data
  // races!
  //
  this->ioring_.post(make_custom_alloc_handler(  //
      this->commit_handler_memory_,              //
      [this, updated_commit_pos](const StatusOr<i32>& post_result) {
        this->commit_pos_listener_active_ = false;

        if (!updated_commit_pos.ok() || !post_result.ok()) {
          this->ioring_.on_work_finished();
          this->ioring_.stop();
          return;
        }

        LLFS_VLOG(2) << "(driver=" << this->name_
                     << ") commit_pos listener invoked: " << updated_commit_pos;

        this->poll_commit_state();
      }));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class FlushState
//
template <template <typename> class FlushOpImpl>
inline BasicIoRingLogDriver<FlushOpImpl>::FlushState::FlushState(IoRingLogDriver* driver) noexcept
    : flushed_upper_bound_{driver->get_flush_pos()}

    , next_flush_op_index_{driver->calculate().flush_op_index_from(SlotUpperBoundAt{
          .offset = this->flushed_upper_bound_ + 1,
      })}

    , flush_op_slot_upper_bound_{driver->calculate()
                                     .block_slot_range_from(SlotUpperBoundAt{
                                         .offset = this->flushed_upper_bound_ + 1,
                                     })
                                     .upper_bound}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline void BasicIoRingLogDriver<FlushOpImpl>::FlushState::poll(IoRingLogDriver* driver)
{
  bool update = false;

  for (;;) {
    const slot_offset_type op_durable_upper_bound =
        slot_min(this->flush_op_slot_upper_bound_,
                 driver->get_flush_op_durable_upper_bound(this->next_flush_op_index_));

    if (slot_less_than(this->flushed_upper_bound_, op_durable_upper_bound)) {
      this->flushed_upper_bound_ = op_durable_upper_bound;
      update = true;
    }

    if (op_durable_upper_bound != this->flush_op_slot_upper_bound_) {
      break;
    }

    this->next_flush_op_index_ =
        driver->calculate().next_flush_op_index(this->next_flush_op_index_);

    this->flush_op_slot_upper_bound_ += driver->calculate().block_capacity();
  }
  if (update) {
    LLFS_LOG_INFO() << "(driver=" << driver->name_ << ") FlushState update=true;"
                    << BATT_INSPECT(flushed_upper_bound_)
                    << BATT_INSPECT(driver->flush_pos_.get_value());

    clamp_min_slot(driver->flush_pos_, this->flushed_upper_bound_);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl>
inline slot_offset_type BasicIoRingLogDriver<FlushOpImpl>::FlushState::get_flushed_upper_bound()
    const
{
  return this->flushed_upper_bound_;
}

}  // namespace llfs

#endif  // LLFS_IORING_LOG_DRIVER_IPP
