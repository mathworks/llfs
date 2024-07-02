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

#include <llfs/config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/data_reader.hpp>
#include <llfs/ioring_log_driver.hpp>
#include <llfs/ioring_log_initializer.hpp>
#include <llfs/ioring_log_recovery.hpp>
#include <llfs/metrics.hpp>
#include <llfs/slot_interval_map.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/seq/boxed.hpp>
#include <batteries/stream_util.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline BasicIoRingLogDriver<FlushOpImpl, StorageT>::BasicIoRingLogDriver(
    LogStorageDriverContext& context, batt::TaskScheduler& task_scheduler, StorageT&& storage,
    const IoRingLogConfig& config, const IoRingLogDriverOptions& options) noexcept
    : context_{context}
    , task_scheduler_{task_scheduler}
    , config_{config}
    , options_{options}
    , calculate_{config, options}
    , storage_{std::move(storage)}
    , flush_ops_(std::max(usize{2}, this->calculate().queue_depth()))
//                        ^ we require at least two flush ops for lazy init:
//                           one to flush a full block at the end of the log and one to
//                           initialize the next (empty) block header.

{
  BATT_CHECK_GE(this->calculate().block_count(), this->calculate().queue_depth())
      << "Queue Depth (== number of concurrent flush ops) may not exceed the number of blocks in "
         "the log; please use different options!";

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
template <template <typename> class FlushOpImpl, typename StorageT>
inline BasicIoRingLogDriver<FlushOpImpl, StorageT>::~BasicIoRingLogDriver() noexcept
{
  global_metric_registry()  //
      .remove(this->trim_pos_)
      .remove(this->flush_pos_)
      .remove(this->commit_pos_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline Status BasicIoRingLogDriver<FlushOpImpl, StorageT>::open()
{
  // Read all blocks into the ring buffer.
  //
  Status data_read = this->read_log_data();
  BATT_REQUIRE_OK(data_read) << BATT_INSPECT(this->name_);

  Status fd_registered = this->storage_.register_fd();
  BATT_REQUIRE_OK(fd_registered);

  // First initialize all ops in the queue pipeline to point back at this driver.  IMPORTANT: this
  // must be done before we allow the commit_pos to change!
  //
  for (auto& op : this->flush_ops_) {
    op.initialize(this);
  }

  StatusOr<usize> buffers_registered = this->storage_.register_buffers(
      as_seq(this->flush_ops_) | seq::map([](const Self::FlushOp& op) {
        return op.get_buffer();
      }) |
      seq::boxed());

  BATT_REQUIRE_OK(buffers_registered);

  this->start_flush_task();

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline Status BasicIoRingLogDriver<FlushOpImpl, StorageT>::read_log_data()
{
  BATT_CHECK(!this->flush_task_);

  // Each IoRing log device gets its own IoRing.  We must start it on a background thread to process
  // the read operations in the loop below.  After we are done, we must stop this background thread
  // and return the IoRing to its original state so that device driver can start up.
  //
  this->storage_.on_work_started();

  // The background thread to process IO completions.
  //
  typename StorageT::EventLoopTask event_loop{this->storage_, this->name_};

  // Shut down the ioring and reset when we leave this scope.
  //
  const auto stop_ioring_thread = batt::finally([&] {
    LLFS_VLOG(1) << "Stopping ioring (IoRingLogDriver::read_log_data)";
    this->storage_.on_work_finished();
    event_loop.join();
    this->storage_.reset_event_loop();
  });

  IoRingLogRecovery recovery{
      this->config_, this->context_.buffer_,
      /*read_data_fn=*/[this](i64 file_offset, MutableBuffer buffer) -> Status {
        return this->storage_.read_all(file_offset + this->config_.physical_offset, buffer);
      }};

  LLFS_VLOG(1) << "Starting log recovery..." << BATT_INSPECT(this->name_);
  Status recovery_status = recovery.run();
  LLFS_VLOG(1) << "Log recovery finished: " << BATT_INSPECT(recovery_status);
  BATT_REQUIRE_OK(recovery_status);

  this->trim_pos_.set_value(recovery.get_trim_pos());
  this->flush_pos_.set_value(recovery.get_flush_pos());
  this->commit_pos_.set_value(recovery.get_flush_pos());
  this->durable_trim_pos_.set_value(recovery.get_trim_pos());

  // Calculate the highest physical block index that has been initialized.
  {
    // If the flush pos is at the end of a block, then the logical block index calculated below will
    // be equal to the first empty block, which must have been initialized in order for the last
    // full block's header to be written.  Otherwise the index will be a non-full block (the last
    // and only such block in the log); in either case, we add one to get the upper bound. be
    //
    const auto logical_init_upper_bound = LogBlockCalculator::LogicalBlockIndex{
        this->calculate_.logical_block_index_from(SlotLowerBoundAt{recovery.get_flush_pos()}) + 1};

    // If we are at or beyond the block count, set init_upper_bound_ to its maximum value.
    //
    if (logical_init_upper_bound >= this->calculate_.block_count()) {
      this->init_upper_bound_.set_value(this->calculate_.block_count());
    } else {
      this->init_upper_bound_.set_value(
          this->calculate_.physical_block_index_from(logical_init_upper_bound));
    }
  }

  // Initialize state according to recovered values.
  //
  this->flush_state_.emplace(this);

  LLFS_VLOG(1) << "log recovery complete; total: "
               << (this->config_.block_size() * this->config_.block_count()) << ";"
               << BATT_INSPECT(this->trim_pos_.get_value())
               << BATT_INSPECT(this->flush_pos_.get_value())
               << BATT_INSPECT(this->commit_pos_.get_value());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline Status BasicIoRingLogDriver<FlushOpImpl, StorageT>::set_trim_pos(slot_offset_type trim_pos)
{
  LLFS_VLOG(1) << "BasicIoRingLogDriver::set_trim_pos(" << trim_pos << ")"
               << BATT_INSPECT(this->trim_pos_.get_value());

  clamp_min_slot(this->trim_pos_, trim_pos);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline StatusOr<slot_offset_type> BasicIoRingLogDriver<FlushOpImpl, StorageT>::await_flush_pos(
    slot_offset_type flush_pos)
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline Status BasicIoRingLogDriver<FlushOpImpl, StorageT>::set_commit_pos(
    slot_offset_type commit_pos)
{
  clamp_min_slot(this->commit_pos_, commit_pos);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::halt()
{
  const bool previously_halted = this->halt_requested_.exchange(true);
  if (!previously_halted) {
    LLFS_VLOG(1) << "BasicIoRingLogDriver::halt() - (driver=" << this->name_
                 << " trim=" << this->trim_pos_.get_value()
                 << " flush=" << this->flush_pos_.get_value()
                 << " commit=" << this->commit_pos_.get_value() << ")";

    this->trim_pos_.close();
    this->flush_pos_.close();
    this->commit_pos_.close();

    this->storage_work_finished();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::join()
{
  if (this->flush_task_) {
    BATT_DEBUG_INFO(
        BATT_INSPECT(this->halt_requested_)
        << BATT_INSPECT(this->trim_pos_.is_closed()) << BATT_INSPECT(this->flush_pos_.is_closed())
        << BATT_INSPECT(this->commit_pos_.is_closed()) << BATT_INSPECT(this->storage_working_));

    this->flush_task_->join();
    this->flush_task_ = None;

    LLFS_VLOG(1) << "IoRingLogDriver stopped;" << BATT_INSPECT(this->get_trim_pos())
                 << BATT_INSPECT(this->get_flush_pos()) << BATT_INSPECT(this->get_commit_pos());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::start_flush_task()
{
  this->flush_task_.emplace(
      this->task_scheduler_.schedule_task(),
      [this] {
        this->flush_task_main();
      },
      batt::to_string("IoRingLogDriver::flush_task(", this->name_, ")"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::flush_task_main()
{
  Status status = [&]() -> Status {
    const u64 total_size = this->calculate().physical_size();
    LLFS_VLOG(1) << "(driver=" << this->name_ << ") log physical size=0x" << std::hex << total_size
                 << " block_size=0x" << this->calculate().block_size() << " block_capacity=0x"
                 << this->calculate().block_capacity() << " queue_depth=" << std::dec
                 << this->calculate().queue_depth();
    LLFS_VLOG(1) << "(driver=" << this->name_ << ")";

    // Now tell the ops to start flushing data.  They will write in parallel but only one at a
    // time will perform an async_wait on `commit_pos_`, to prevent thundering herd bottlenecks.
    //
    for (auto& op : this->flush_ops_) {
      op.activate();
    }

    BATT_DEBUG_INFO([&](std::ostream& out) {
      usize op_i = 0;
      const usize block_count = this->calculate().block_count();
      for (auto& op : this->flush_ops_) {
        out << "FlushOp[" << op_i << "]: " << op.debug_info_message()
            << " (block=" << op.get_current_log_block_index() << "/" << block_count << "), ";
        op_i += 1;
      }
      out << BATT_INSPECT(this->trim_pos_) << BATT_INSPECT(this->flush_pos_)
          << BATT_INSPECT(commit_pos_);
    });

    this->storage_work_started();
    this->poll_flush_state();
    this->poll_commit_state();

    // Process I/O events concurrently in the background so as not to tie up the executor on which
    // this task is running.
    //
    typename StorageT::EventLoopTask event_loop{this->storage_, this->name_};

    // Wait for the event loop to exit.
    //
    event_loop.join();

    // Now it is safe to close the storage layer (i.e. direct I/O block file).
    //
    this->storage_close_status_ = this->storage_.close();

    if (!this->storage_close_status_.ok()) {
      if (default_ioring_quiet_failure_logging()) {
        LLFS_VLOG(1) << "Failed to close storage for IoRingLogDevice;"
                     << BATT_INSPECT_STR(this->name_) << BATT_INSPECT(this->storage_close_status_);
      } else {
        LLFS_LOG_WARNING() << "Failed to close storage for IoRingLogDevice;"
                           << BATT_INSPECT_STR(this->name_)
                           << BATT_INSPECT(this->storage_close_status_);
      }
    }

    return OkStatus();
  }();

  if (!this->halt_requested_.load() && this->storage_working_.load()) {
    LLFS_LOG_WARNING() << "[IoRingLogDriver::flush_task] exited unexpectedly with status="
                       << status;
  } else {
    LLFS_VLOG(1) << "[IoRingLogDriver::flush_task] exited with status=" << status;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::poll_flush_state()
{
  this->flush_state_->poll(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::report_flush_error(Status error_status)
{
  this->context_.update_error_status(error_status);
  this->flush_pos_.close();
  this->storage_work_finished();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::poll_commit_state()
{
  LLFS_VLOG(2) << "(driver=" << this->name_ << ") poll_commit_state() entered";

  // Don't recursively re-enter `poll_commit_state`.
  //
  if (this->inside_poll_commit_state_) {
    LLFS_VLOG(2) << "(driver=" << this->name_ << ") poll_commit_state() recursive call - returning";
    return;
  }
  this->inside_poll_commit_state_ = true;
  auto on_scope_exit = batt::finally([&] {
    this->inside_poll_commit_state_ = false;
  });

  // Return immediately if there are no flush ops waiting for `commit_pos_` to advance.
  //
  if (this->waiting_for_commit_.empty()) {
    LLFS_VLOG(2) << "(driver=" << this->name_
                 << ") poll_commit_state() waiting_for_commit_ is empty - returning";
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

        LLFS_VLOG(2) << "(driver=" << this->name_ << ") waiting on commit_pos;"
                     << BATT_INSPECT(known_commit_pos) << BATT_INSPECT(next_wait_pos)
                     << BATT_INSPECT(this->commit_pos_.is_closed());

        // If we break out of this loop before we completely drain `waiting_for_commit_`, we must
        // start another wait operation.
        //
        this->commit_pos_.async_wait(          //
            known_commit_pos,                  //
            make_custom_alloc_handler(         //
                this->commit_handler_memory_,  //
                [this](const StatusOr<slot_offset_type>& updated_commit_pos) {
                  LLFS_VLOG(2) << "(driver=" << this->name_
                               << ") commit_pos_ updated: " << updated_commit_pos;
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
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::wait_for_commit(
    slot_offset_type least_upper_bound)
{
  this->waiting_for_commit_.push(least_upper_bound);
  this->poll_commit_state();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::handle_commit_pos_update(
    const StatusOr<slot_offset_type>& updated_commit_pos)
{
  // VERY IMPORTANT: we must execute the actual handler logic on the ioring thread to avoid data
  // races!
  //
  this->storage_.post_to_event_loop(make_custom_alloc_handler(  //
      this->commit_handler_memory_,                             //
      [this, updated_commit_pos](const StatusOr<i32>& post_result) {
        LLFS_VLOG(2) << "(driver=" << this->name_ << ") handle_commit_pos_update("
                     << updated_commit_pos << ")" << BATT_INSPECT(post_result);

        this->commit_pos_listener_active_ = false;

        if (!updated_commit_pos.ok() || !post_result.ok()) {
          this->storage_work_finished();
          return;
        }

        this->poll_commit_state();
      }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::storage_work_started()
{
  const bool was_working = this->storage_working_.exchange(true);
  BATT_CHECK(!was_working);

  this->storage_.on_work_started();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::storage_work_finished()
{
  const bool was_working = this->storage_working_.exchange(false);
  if (was_working) {
    this->storage_.on_work_finished();
  }
  // else:
  //   Don't assert/check here, since there are multiple legitimate places where we could call
  //   `on_work_finished`.
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class FlushState
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline BasicIoRingLogDriver<FlushOpImpl, StorageT>::FlushState::FlushState(
    BasicIoRingLogDriver* driver) noexcept
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
template <template <typename> class FlushOpImpl, typename StorageT>
inline void BasicIoRingLogDriver<FlushOpImpl, StorageT>::FlushState::poll(
    BasicIoRingLogDriver* driver)
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
    LLFS_VLOG(1) << "(driver=" << driver->name_ << ") FlushState update=true;"
                 << BATT_INSPECT(flushed_upper_bound_)
                 << BATT_INSPECT(driver->flush_pos_.get_value());

    clamp_min_slot(driver->flush_pos_, this->flushed_upper_bound_);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <template <typename> class FlushOpImpl, typename StorageT>
inline slot_offset_type
BasicIoRingLogDriver<FlushOpImpl, StorageT>::FlushState::get_flushed_upper_bound() const
{
  return this->flushed_upper_bound_;
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_IORING_LOG_DRIVER_IPP
