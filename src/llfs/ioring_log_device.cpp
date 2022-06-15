//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_device.hpp>
//

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <batteries/async/runtime.hpp>
#include <batteries/seq/boxed.hpp>
#include <batteries/stream_util.hpp>

#include <turtle/util/metric_registry.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingLogDriver::IoRingLogDriver(LogStorageDriverContext& context, int fd, const Config& config,
                                 const Options& options) noexcept
    : context_{context}
    , config_{config}
    , options_{options}
    , ioring_{*IoRing::make_new(this->queue_depth() * 2)}
    , file_{this->ioring_, fd}
    , flush_ops_(this->queue_depth())
{
  const auto metric_name = [this](const std::string_view& property) {
    return batt::to_string("IoRingLogDevice_", this->name_, "_", property);
  };

  global_metric_registry()
      .add(metric_name("trim_pos"), this->trim_pos_)
      .add(metric_name("flush_pos"), this->flush_pos_)
      .add(metric_name("commit_pos"), this->commit_pos_);
  //.add(metric_name("flush_write_latency"), this->metrics_.flush_write_latency)
  //.add(metric_name("logical_bytes_flushed"), this->metrics_.logical_bytes_flushed)
  //.add(metric_name("physical_bytes_flushed"), this->metrics_.physical_bytes_flushed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingLogDriver::~IoRingLogDriver() noexcept
{
  global_metric_registry()  //
      .remove(this->trim_pos_)
      .remove(this->flush_pos_)
      .remove(this->commit_pos_);
  //.remove(this->metrics_.flush_write_latency)
  //.remove(this->metrics_.logical_bytes_flushed)
  //.remove(this->metrics_.physical_bytes_flushed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingLogDriver::open()
{
  // Read all blocks into the ring buffer.
  //
  Status data_read = this->read_log_data();
  BATT_REQUIRE_OK(data_read);

  Status fd_registered = this->file_.register_fd();
  BATT_REQUIRE_OK(fd_registered);

  // First initialize all ops in the queue pipeline to point back at this driver.
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
Status IoRingLogDriver::read_log_data()
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
    VLOG(1) << "ioring_thread started";
    this->ioring_.run().IgnoreError();
    VLOG(1) << "ioring_thread returning";
  }};

  // Shut down the ioring and reset when we leave this scope.
  //
  const auto stop_ioring_thread = batt::finally([&] {
    VLOG(1) << "Stopping ioring";
    this->ioring_.on_work_finished();
    ioring_thread.join();
    this->ioring_.reset();
  });

  // TODO [tastolfi 2022-02-09] this can be made more efficient; the block headers contain clues
  // that allow us to skip some work.  Also, we don't have to read entire blocks always, since
  // commit_size may be less than block_capacity.

  BATT_CHECK_EQ(this->config_.block_size() % sizeof(PackedPageHeaderBuffer), 0u);

  std::unique_ptr<PackedPageHeaderBuffer[]> block_storage{
      new PackedPageHeaderBuffer[this->config_.block_size() / sizeof(PackedPageHeaderBuffer)]};

  auto& block_header = block_storage[0].header;
  MutableBuffer block_buffer{(void*)block_storage.get(), this->config_.block_size()};
  ConstBuffer block_payload = block_buffer + sizeof(PackedPageHeader);

  BATT_CHECK_EQ(block_buffer.size(), this->config_.block_size());
  BATT_CHECK_EQ(block_payload.size(), this->config_.block_capacity());

  u64 file_offset = this->config_.physical_offset;
  for (usize block_i = 0; block_i < this->config_.block_count(); ++block_i) {
    VLOG(1) << "reading log; " << BATT_INSPECT(block_i) << BATT_INSPECT(file_offset)
            << BATT_INSPECT(this->config_.block_size()) << BATT_INSPECT(block_buffer.size())
            << BATT_INSPECT(this->file_.get_fd());

    Status read_status = this->file_.read_all(file_offset, block_buffer);
    BATT_REQUIRE_OK(read_status);

    if (block_header.magic != IoRingLogDriver::PackedPageHeader::kMagic) {
      // TODO [tastolfi 2022-02-09] specific error message/code
      return {batt::StatusCode::kDataLoss};
    }
    if (block_header.commit_size > this->config_.block_capacity()) {
      // TODO [tastolfi 2022-02-09] specific error message/code
      return {batt::StatusCode::kDataLoss};
    }
    // TODO [tastolfi 2022-02-09] validate CRC

    clamp_min_slot(this->trim_pos_, block_header.trim_pos);
    clamp_min_slot(this->flush_pos_, block_header.flush_pos);

    if (block_header.commit_size > 0) {
      MutableBuffer dst = this->context_.buffer_.get_mut(block_header.slot_offset);
      std::memcpy(dst.data(), block_payload.data(), block_header.commit_size);
    }

    file_offset += this->config_.block_size();
  }

  // After opening, there is no unflushed data so commit_pos == flush_pos.
  //
  this->commit_pos_.set_value(this->flush_pos_.get_value());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingLogDriver::set_trim_pos(slot_offset_type trim_pos)
{
  clamp_min_slot(this->trim_pos_, trim_pos);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDriver::halt()
{
  this->trim_pos_.close();
  this->flush_pos_.close();
  this->commit_pos_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDriver::join()
{
  if (this->flush_task_) {
    this->flush_task_->join();
    this->flush_task_ = None;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDriver::start_flush_task()
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
void IoRingLogDriver::flush_task_main()
{
  Status status = [&]() -> Status {
    const u64 total_size = (this->log_end_ - this->log_start_);
    LOG(INFO) << "(driver=" << this->name_ << ") log physical size=0x" << std::hex << total_size
              << " block_size=0x" << this->block_size() << " block_capacity=0x"
              << this->block_capacity() << " queue_depth=" << std::dec << this->queue_depth();
    LOG(INFO) << "(driver=" << this->name_
              << ") buffer delay=" << this->options_.page_write_buffer_delay_usec << "usec";

    // Now tell the ops to start flushing data.  They will write in parallel but only one at a time
    // will perform an async_wait on `commit_pos_`, to prevent thundering herd bottlenecks.
    //
    for (auto& op : this->flush_ops_) {
      op.activate();
    }

    this->ioring_.on_work_started();
    this->poll_flush_state();
    this->poll_commit_state();

    // Run the IoRing on a background thread so as not to tie up the executor on which this task is
    // running.
    //
    batt::Watch<bool> done{false};
    std::thread io_thread{[this, &done] {
      LOG(INFO) << "(driver=" << this->name_ << ") invoking IoRing::run()";
      Status io_status = this->ioring_.run();
      if (!io_status.ok()) {
        LOG(WARNING) << "(driver=" << this->name_ << ") IoRing::run() returned: " << io_status;
      }
      done.set_value(true);
    }};
    auto done_status = done.await_equal(true);
    BATT_CHECK(done_status.ok());
    io_thread.join();

    return OkStatus();
  }();

  LOG(INFO) << "[IoRingLogDriver::flush_task] exited with status=" << status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDriver::poll_flush_state()
{
  this->poll_flush_pos_(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDriver::poll_commit_state()
{
  if (!this->waiting_for_commit_.empty()) {
    slot_offset_type known_commit_pos = this->commit_pos_.get_value();
    VLOG(1) << "(driver=" << this->name_ << ") read commit_pos=" << known_commit_pos;
    do {
      slot_offset_type next_wait_pos = this->waiting_for_commit_.top();
      if (slot_less_than(known_commit_pos, next_wait_pos)) {
        this->wait_for_commit_pos(known_commit_pos);
        break;
      }
      this->waiting_for_commit_.pop();

      // Figure out which op must have been waiting on the given pos.
      //
      const usize op_index =
          ((next_wait_pos - 1) / this->block_capacity()) & this->queue_depth_mask_;
      VLOG(1) << "(driver=" << this->name_ << ") commit_pos=" << known_commit_pos << " waking op["
              << op_index << "], which was waiting on commit_pos >= " << next_wait_pos;

      this->flush_ops_[op_index].poll_commit_pos(known_commit_pos);
    } while (!this->waiting_for_commit_.empty());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDriver::wait_for_commit_pos(slot_offset_type last_seen)
{
  if (!this->commit_pos_listener_active_) {
    VLOG(1) << "(driver=" << this->name_ << ") wait_for_commit_pos(last_seen=" << last_seen << ")";
    this->commit_pos_listener_active_ = true;
    this->commit_pos_.async_wait(
        last_seen, make_custom_alloc_handler(
                       this->commit_handler_memory_,
                       [this](const StatusOr<slot_offset_type>& updated_commit_pos) {
                         this->ioring_.post(make_custom_alloc_handler(
                             this->commit_handler_memory_,
                             [this, updated_commit_pos](const StatusOr<i32>& post_result) {
                               this->commit_pos_listener_active_ = false;
                               if (!updated_commit_pos.ok() || !post_result.ok()) {
                                 this->ioring_.on_work_finished();
                                 this->ioring_.stop();
                                 return;
                               }
                               VLOG(1) << "(driver=" << this->name_
                                       << ") commit_pos listener invoked: " << updated_commit_pos;
                               this->poll_commit_state();
                             }));
                       }));
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PollFlushPos
//
IoRingLogDriver::PollFlushPos::PollFlushPos(IoRingLogDriver* this_)
    : next_op_index_{0}
    , local_flush_pos_{0}
    , op_upper_bound_{local_flush_pos_ + this_->block_capacity()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDriver::PollFlushPos::operator()(IoRingLogDriver* this_)
{
  bool update = false;
  for (;;) {
    auto& op = this_->flush_ops_[next_op_index_];
    const slot_offset_type target_flush_pos = slot_min(op.flush_pos(), op_upper_bound_);

    if (slot_less_than(local_flush_pos_, target_flush_pos)) {
      local_flush_pos_ = target_flush_pos;
      update = true;
    }

    if (local_flush_pos_ != op_upper_bound_) {
      break;
    }

    next_op_index_ = (next_op_index_ + 1) & this_->queue_depth_mask_;
    op_upper_bound_ += this_->block_capacity();
  }
  if (update) {
    VLOG(1) << "(driver=" << this_->name_ << ") writing flush_pos=" << local_flush_pos_;
    this_->flush_pos_.set_value(local_flush_pos_);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type IoRingLogDriver::PollFlushPos::get() const
{
  return local_flush_pos_;
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status initialize_ioring_log_device(IoRing::File& file, const IoRingLogDriver::Config& config,
                                    ConfirmThisWillEraseAllMyData confirm)
{
  IoRingLogDriver::PackedPageHeaderBuffer buffer;
  buffer.clear();

  buffer.header.magic = IoRingLogDriver::PackedPageHeader::kMagic;
  buffer.header.slot_offset = 0;
  buffer.header.commit_size = 0;
  buffer.header.crc64 = 0;  // TODO [tastolfi 2022-02-09] implement me
  buffer.header.trim_pos = 0;
  buffer.header.flush_pos = 0;

  u64 file_offset = config.physical_offset;
  for (u64 block_i = 0; block_i < config.block_count(); ++block_i) {
    VLOG(1) << "writing initial block header; " << BATT_INSPECT(buffer.header.slot_offset)
            << BATT_INSPECT(file_offset);
    Status write_status = file.write_all(file_offset, buffer.as_const_buffer());
    BATT_REQUIRE_OK(write_status);
    buffer.header.slot_offset += config.block_capacity();
    file_offset += config.block_size();
  }

  return OkStatus();
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
