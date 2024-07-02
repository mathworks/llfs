//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_log_device_storage.hpp>
//
#include <llfs/ioring_log_driver.hpp>
#include <llfs/ioring_log_driver.ipp>
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/ioring_log_flush_op.ipp>
#include <llfs/storage_simulation.hpp>

namespace llfs {

// Explicitly instantiate BasicIoRingLogDriver for the simulated LogDevice storage.
//
template class BasicIoRingLogDriver<BasicIoRingLogFlushOp, SimulatedLogDeviceStorage>;

// BasicIoRingLogFlushOp is required by BasicIoRingLogDriver, so explicitly instantiate that too.
//
template class BasicIoRingLogFlushOp<
    BasicIoRingLogDriver<BasicIoRingLogFlushOp, SimulatedLogDeviceStorage>>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class SimulatedLogDeviceStorage::DurableState

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::DurableState::crash_and_recover(u64 simulation_step) /*override*/
{
  batt::ScopedLock<Impl> locked{this->impl_};

  locked->last_recovery_step_ = simulation_step;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDeviceStorage::DurableState::validate_args(i64 offset, usize size,
                                                              const char* op_type) noexcept
{
  if (offset % sizeof(AlignedBlock) != 0 || size != sizeof(AlignedBlock)) {
    return batt::StatusCode::kInvalidArgument;
  }
  if (offset < this->file_offset_.lower_bound) {
    return batt::StatusCode::kOutOfRange;
  }
  if (offset + static_cast<i64>(size) > this->file_offset_.upper_bound) {
    return batt::StatusCode::kOutOfRange;
  }
  if (this->simulation_.inject_failure()) {
    LLFS_VLOG(1) << "(id=" << this->id_ << ") injecting failure;" << BATT_INSPECT(offset)
                 << BATT_INSPECT(size) << BATT_INSPECT(op_type);
    return batt::StatusCode::kInternal;
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename BufferT, typename OpFn>
Status SimulatedLogDeviceStorage::DurableState::block_op_impl(u64 creation_step, i64 offset,
                                                              const BufferT& buffer, OpFn&& op_fn)
{
  const char* op_type = std::is_same_v<std::decay_t<BufferT>, ConstBuffer> ? "write" : "read";

  BATT_REQUIRE_OK(this->validate_args(offset, buffer.size(), op_type));

  batt::ScopedLock<Impl> locked{this->impl_};

  if (creation_step < locked->last_recovery_step_) {
    return batt::StatusCode::kClosed;
  }

  auto& p_block = locked->blocks_[offset];
  BATT_FORWARD(op_fn)(p_block);

  LLFS_VLOG(1) << "(id=" << this->id_ << ") DurableState::" << op_type << "_block(" << offset
               << ") Success";

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDeviceStorage::DurableState::write_block(u64 creation_step, i64 offset,
                                                            const ConstBuffer& buffer)
{
  return this->block_op_impl(creation_step, offset, buffer,
                             [&buffer](std::unique_ptr<AlignedBlock>& p_block) {
                               if (!p_block) {
                                 p_block = std::make_unique<AlignedBlock>();
                               }
                               std::memcpy(p_block.get(), buffer.data(), buffer.size());
                             });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDeviceStorage::DurableState::read_block(u64 creation_step, i64 offset,
                                                           const MutableBuffer& buffer)
{
  return this->block_op_impl(creation_step, offset, buffer,
                             [&buffer](std::unique_ptr<AlignedBlock>& p_block) {
                               if (!p_block) {
                                 std::memset(buffer.data(), 0, buffer.size());
                               } else {
                                 std::memcpy(buffer.data(), p_block.get(), buffer.size());
                               }
                             });
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class SimulatedLogDeviceStorage::EphemeralState

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDeviceStorage::EphemeralState::EphemeralState(
    std::shared_ptr<DurableState>&& durable_state) noexcept
    : durable_state_{std::move(durable_state)}
    , simulation_{this->durable_state_->simulation()}
    , creation_step_{this->simulation_.current_step()}
{
  this->work_count_per_caller_.fill(0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::TaskScheduler& SimulatedLogDeviceStorage::EphemeralState::get_task_scheduler() noexcept
{
  return this->simulation_.is_running() ? this->simulation_.task_scheduler()
                                        : batt::Runtime::instance().default_scheduler();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDeviceStorage::EphemeralState::close()
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::close()";

  if (this->closed_.exchange(true)) {
    return batt::StatusCode::kClosed;
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::on_work_started(usize caller)
{
  ++this->work_count_per_caller_[caller];

  const i32 old_value = this->work_count_.fetch_add(1);

  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::on_work_started() " << old_value
               << " -> " << (old_value + 1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::on_work_finished(usize caller)
{
  --this->work_count_per_caller_[caller];

  const i32 old_value = this->work_count_.fetch_sub(1);

  for (i32 n : this->work_count_per_caller_) {
    BATT_CHECK_GE(n, 0) << BATT_INSPECT(this->id_)
                        << BATT_INSPECT_RANGE(this->work_count_per_caller_) << BATT_INSPECT(caller);
  }

  BATT_CHECK_GT(old_value, 0) << BATT_INSPECT(this->id_)
                              << BATT_INSPECT_RANGE(this->work_count_per_caller_)
                              << BATT_INSPECT(caller);

  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::on_work_finished() " << old_value
               << " -> " << (old_value - 1);

  if (old_value == 1) {
    this->queue_.poke();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDeviceStorage::EphemeralState::run_event_loop()
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::run_event_loop() entered" << std::endl
               << boost::stacktrace::stacktrace{};
  for (;;) {
    LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::run_event_loop() top of loop:"
                 << BATT_INSPECT(this->stopped_) << BATT_INSPECT(this->work_count_);

    // If there is no more work to do (work_count_ == 0), or stop has been explicitly requested,
    // then exit the loop now before awaiting the next completion handler.
    //
    if (this->stopped_.load() || this->work_count_.load() == 0) {
      break;
    }

    BATT_DEBUG_INFO(BATT_INSPECT(this->stopped_)
                    << BATT_INSPECT(this->work_count_) << BATT_INSPECT(this->closed_));

    LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::run_event_loop() waiting for queue";
    StatusOr<std::function<void()>> handler = this->queue_.await_next();
    if (!handler.ok()) {
      if (handler.status() == batt::StatusCode::kPoke) {
        continue;
      }

      if (handler.status() == batt::StatusCode::kClosed) {
        break;
      }

      return handler.status();
    }

    try {
      (*handler)();
    } catch (...) {
      LLFS_LOG_WARNING() << "Simulation handler threw exception!";
    }
  }
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::run_event_loop() exiting";

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::reset_event_loop()
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::reset_event_loop()"
               << BATT_INSPECT(this->stopped_);
  this->stopped_.store(false);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::stop_event_loop()
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::reset_event_loop()"
               << BATT_INSPECT(this->stopped_);
  this->stopped_.store(true);
  this->queue_.poke();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::post_to_event_loop(
    std::function<void(StatusOr<i32>)>&& handler)
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::post_to_event_loop(handler) entered"
               << BATT_INSPECT(this->simulation_.is_running());

  this->on_work_started(POST_TO_EVENT_LOOP);

  auto runnable = [shared_this = batt::shared_ptr_from(this),
                   handler = BATT_FORWARD(handler)]() mutable {
    auto* const this_ = shared_this.get();

    LLFS_VLOG(1) << "(id=" << this_->id_
                 << ") EphemeralState::post_to_event_loop(handler) invoked runnable";

    // We don't need to capture `shared_this` in the completion handler we push to the queue, since
    // either the work count will keep the event loop task running (and if the log driver doesn't
    // join the even loop task, this is a serious error we don't want to mask) *or* the completion
    // handler will never run, in which case a dangling pointer doesn't matter (if a tree falls in a
    // forest...)
    //
    this_->queue_
        .push([this_, handler]() mutable {
          auto on_scope_exit = batt::finally([&] {
            this_->on_work_finished(POST_TO_EVENT_LOOP);
          });
          BATT_FORWARD(handler)(StatusOr<i32>{0});
        })
        .IgnoreError();
  };

  if (this->simulation_.is_running()) {
    this->simulation_.post(std::move(runnable));
  } else {
    runnable();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDeviceStorage::EphemeralState::read_all(i64 offset, const MutableBuffer& buffer)
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::read_all(offset=" << offset
               << ", size=" << buffer.size() << ")";

  return this->multi_block_iop<MutableBuffer, &DurableState::read_block>(offset, buffer);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::async_write_some(
    i64 file_offset, const ConstBuffer& data, std::function<void(StatusOr<i32>)>&& handler)
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphemeralState::async_write_some(offset=" << file_offset
               << ", size=" << data.size() << ")";

  if (this->closed_.load()) {
    BATT_FORWARD(handler)(Status{batt::StatusCode::kClosed});
    return;
  }

  this->on_work_started(ASYNC_WRITE_SOME);

  auto write_task = std::make_shared<batt::Task>(
      this->get_task_scheduler().schedule_task(),
      [shared_this = batt::shared_ptr_from(this), file_offset, data,
       handler = BATT_FORWARD(handler)]() mutable {
        auto* const this_ = shared_this.get();

        // Write all blocks synchronously.
        //
        Status status =
            this_->multi_block_iop<ConstBuffer, &DurableState::write_block>(file_offset, data);

        auto result = [&] {
          if (status.ok()) {
            return StatusOr<i32>{static_cast<i32>(data.size())};
          }
          return StatusOr<i32>{status};
        }();

        this_->queue_
            .push([handler, result, this_] {
              auto on_scope_exit = batt::finally([this_] {
                this_->on_work_finished(ASYNC_WRITE_SOME);
              });
              handler(result);
            })
            .IgnoreError();
      },
      batt::Task::DeferStart{true});

  write_task->call_when_done([write_task]() mutable {
    // Release the task.
    //
    write_task = nullptr;
  });

  write_task->start();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename BufferT,
          Status (SimulatedLogDeviceStorage::DurableState::*Op)(u64, i64, const BufferT&)>
Status SimulatedLogDeviceStorage::EphemeralState::multi_block_iop(i64 offset, BufferT buffer)
{
  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphmeralState::multi_block_iop(" << BATT_INSPECT(offset)
               << "," << BATT_INSPECT(buffer.size())
               << ") type=" << (std::is_same_v<BufferT, ConstBuffer> ? "write" : "read");

  if (this->closed_.load()) {
    return batt::StatusCode::kClosed;
  }

  std::vector<Status> block_status(buffer.size() / sizeof(AlignedBlock));
  batt::Watch<usize> blocks_remaining(block_status.size());
  std::atomic<usize> pin_count{block_status.size()};

  if (buffer.size() != block_status.size() * sizeof(AlignedBlock)) {
    return batt::StatusCode::kInvalidArgument;
  }

  usize i = 0;
  while (buffer.size() > 0) {
    LLFS_VLOG(1) << "(id=" << this->id_ << ") EphmeralState::multi_block_iop()"
                 << BATT_INSPECT(offset) << BATT_INSPECT(i) << "/" << block_status.size();

    this->simulation_post([this, i, offset, &block_status, &blocks_remaining, &pin_count,
                           block_buffer = BufferT{buffer.data(), sizeof(AlignedBlock)}] {
      LLFS_VLOG(1) << "(id=" << this->id_ << ") EphmeralState::multi_block_iop() - activated"
                   << BATT_INSPECT(offset) << BATT_INSPECT(i) << "/" << block_status.size();

      auto on_scope_exit = batt::finally([&] {
        blocks_remaining.fetch_sub(1);
        pin_count.fetch_sub(1);
      });

      block_status[i] = this->closed_.load() ? Status{batt::StatusCode::kClosed}
                                             : (this->durable_state_.get()->*Op)(
                                                   this->creation_step_, offset, block_buffer);
    });

    offset += sizeof(AlignedBlock);
    buffer += sizeof(AlignedBlock);
    ++i;
  }

  LLFS_VLOG(1) << "(id=" << this->id_
               << ") EphmeralState::multi_block_iop() waiting for all ops to complete";

  blocks_remaining.await_equal(0).IgnoreError();
  BATT_CHECK_EQ(blocks_remaining.get_value(), 0u);
  while (pin_count.load() != 0) {
    batt::Task::yield();
  }

  LLFS_VLOG(1) << "(id=" << this->id_
               << ") EphmeralState::multi_block_iop() checking per-block statuses";

  for (const Status& status : block_status) {
    BATT_REQUIRE_OK(status);
  }

  LLFS_VLOG(1) << "(id=" << this->id_ << ") EphmeralState::multi_block_iop() success!";

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::simulation_post(std::function<void()>&& fn)
{
  if (this->simulation_.is_running()) {
    this->simulation_.post(std::move(fn));
  } else {
    try {
      std::move(fn)();
    } catch (...) {
      LLFS_LOG_WARNING() << "Simulation post fn threw exception!";
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class SimulatedLogDeviceStorage::RawBlockFileImpl

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDeviceStorage::RawBlockFileImpl::RawBlockFileImpl(
    std::shared_ptr<DurableState>&& durable_state) noexcept
    : durable_state_{std::move(durable_state)}
    , creation_step_{this->durable_state_->simulation().current_step()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> SimulatedLogDeviceStorage::RawBlockFileImpl::write_some(
    i64 offset, const ConstBuffer& buffer_arg) /*override*/
{
  ConstBuffer buffer = buffer_arg;
  i64 n_written = 0;

  while (buffer.size() > 0) {
    BATT_REQUIRE_OK(this->durable_state_->write_block(
        this->creation_step_, offset,
        ConstBuffer{buffer.data(), std::min(buffer.size(), sizeof(AlignedBlock))}));
    offset += sizeof(AlignedBlock);
    buffer += sizeof(AlignedBlock);
    n_written += sizeof(AlignedBlock);
  }

  return n_written;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> SimulatedLogDeviceStorage::RawBlockFileImpl::read_some(
    i64 offset, const MutableBuffer& buffer_arg) /*override*/
{
  MutableBuffer buffer = buffer_arg;
  i64 n_read = 0;

  while (buffer.size() > 0) {
    BATT_REQUIRE_OK(this->durable_state_->read_block(
        this->creation_step_, offset,
        MutableBuffer{buffer.data(), std::min(buffer.size(), sizeof(AlignedBlock))}));
    offset += sizeof(AlignedBlock);
    buffer += sizeof(AlignedBlock);
    n_read += sizeof(AlignedBlock);
  }

  return n_read;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> SimulatedLogDeviceStorage::RawBlockFileImpl::get_size() /*override*/
{
  return Status{batt::StatusCode::kUnimplemented};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class SimulatedLogDeviceStorage::EventLoopTask

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDeviceStorage::EventLoopTask::EventLoopTask(
    SimulatedLogDeviceStorage& storage, std::string_view caller) noexcept
    : storage_{storage}
    , caller_{caller}
    , task_{this->storage_.impl_->get_task_scheduler().schedule_task(),
            [this] {
              LLFS_VLOG(1) << "Entered simulated log storage event loop;"
                           << BATT_INSPECT(this->caller_)
                           << BATT_INSPECT(this->storage_.impl_->id());

              this->storage_.impl_->run_event_loop().IgnoreError();

              LLFS_VLOG(1) << "Leaving simulated log storage event loop;"
                           << BATT_INSPECT(this->caller_)
                           << BATT_INSPECT(this->storage_.impl_->id());
            },
            batt::to_string("SimulatedLogDeviceStorage::EventLoopTask(", caller, ")")}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SimulatedLogDeviceStorage::EventLoopTask::~EventLoopTask() noexcept
{
  BATT_CHECK(this->join_called_);
  BATT_CHECK(this->task_.is_done());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EventLoopTask::join()
{
  this->join_called_ = true;
  this->task_.join();

  LLFS_VLOG(1) << "EventLoopTask::join() exiting;"
               << BATT_INSPECT(this->storage_.impl_->work_count())
               << BATT_INSPECT(this->storage_.impl_->is_stopped())
               << BATT_INSPECT(this->storage_.impl_->id());
}

}  //namespace llfs
