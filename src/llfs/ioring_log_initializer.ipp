//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_INITIALIZER_IPP
#define LLFS_IORING_LOG_INITIALIZER_IPP

#include <llfs/config.hpp>
//
#ifndef LLFS_DISABLE_IO_URING

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename IoRingImpl>
/*explicit*/ inline BasicIoRingLogInitializer<IoRingImpl>::BasicIoRingLogInitializer(
    usize n_tasks, typename IoRingImpl::File& file, const IoRingLogConfig& config,
    u64 n_blocks_to_init) noexcept
    : file_{file}
    , config_{config}
    , subtasks_(n_tasks)
    , n_blocks_to_init_{n_blocks_to_init}
{
  for (auto& task : this->subtasks_) {
    task.that = this;
    task.buffer.clear();
    task.buffer.header.reset();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename IoRingImpl>
inline batt::Status BasicIoRingLogInitializer<IoRingImpl>::run()
{
  LLFS_VLOG(1) << "IoRingLogInitializer::run() Entered; "
               << BATT_INSPECT(this->config_.block_count())
               << BATT_INSPECT(this->config_.block_size())
               << BATT_INSPECT(this->config_.block_capacity())
               << BATT_INSPECT(this->n_blocks_to_init_);
  {
    batt::MutableBuffer memory{this->subtasks_.data(), this->subtasks_.size() * sizeof(Subtask)};
    LLFS_VLOG(1) << "memory = " << (const void*)memory.data() << ".."
                 << (const void*)(this->subtasks_.data() + this->subtasks_.size());

    // Cache the file descriptor information in the kernel for faster access.
    //
    Status fd_status = this->file_.register_fd();
    BATT_REQUIRE_OK(fd_status);

    // Map our memory buffer to the kernel for faster I/O.
    //
    StatusOr<usize> buffers_status = this->file_.get_io_ring_impl()->register_buffers(
        seq::single_item(std::move(memory)) | seq::boxed(), /*update=*/false);

    LLFS_VLOG(2) << "register_buffers status=" << buffers_status;
    BATT_REQUIRE_OK(buffers_status);
  }
  auto on_scope_exit = batt::finally([&] {
    this->file_.get_io_ring_impl()->unregister_buffers().IgnoreError();
    LLFS_VLOG(1) << "IoRingLogInitializer::run() Finished";
  });

  for (auto& task : this->subtasks_) {
    task.start_write();
  }
  Status all_finished = this->finished_count_.await_equal(this->subtasks_.size());
  BATT_REQUIRE_OK(all_finished);
  for (auto& task : this->subtasks_) {
    BATT_REQUIRE_OK(task.final_status);
  }

  return OkStatus();
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename IoRingImpl>
inline void BasicIoRingLogInitializer<IoRingImpl>::Subtask::start_write()
{
  LLFS_VLOG(2) << "[Subtask:" << this->self_index()
               << "] IoRingLogInitializer::Subtask::start_write()"
               << BATT_INSPECT(this->block_progress) << BATT_INSPECT(this->file_offset);

  BasicIoRingLogInitializer* const that = this->that;
  const IoRingLogConfig& config = that->config_;

  batt::ConstBuffer bytes = this->buffer.as_const_buffer() + this->block_progress;
  if (bytes.size() == 0) {
    LLFS_VLOG(2) << " -- At end of buffer; fetching next block index...";

    const usize block_i = that->next_block_i_.fetch_add(1);

    LLFS_VLOG(2) << " -- " << BATT_INSPECT(block_i) << "/" << that->n_blocks_to_init_;

    // If `block_i` is at or past the end of the log, we are done!  Increment the finished count
    // and return.
    //
    if (block_i >= that->n_blocks_to_init_) {
      LLFS_VLOG(2) << " -- FINISHED (all blocks written)";
      this->finish(batt::OkStatus());
      return;
    }

    // We have a new block to write.  Reset our state and continue.
    //
    this->file_offset = config.physical_offset + config.block_size() * block_i;
    this->block_progress = 0;
    this->buffer.header.slot_offset = config.block_capacity() * block_i;
    bytes = this->buffer.as_const_buffer();
  }

  LLFS_VLOG(2) << " -- async_write_some(offset=" << this->file_offset << ", bytes=[" << bytes.size()
               << "])";

  that->file_.async_write_some_fixed(
      this->file_offset, bytes, /*buf_index=*/0,
      batt::make_custom_alloc_handler(this->handler_memory,
                                      [this](const batt::StatusOr<i32>& n_written) {
                                        this->handle_write(n_written);
                                      }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename IoRingImpl>
inline void BasicIoRingLogInitializer<IoRingImpl>::Subtask::handle_write(
    const batt::StatusOr<i32>& n_written)
{
  LLFS_VLOG(2) << "[Subtask:" << this->self_index()
               << "] IoRingLogInitializer::Subtask::handle_write(status=" << n_written.status()
               << ", n_written=" << (n_written.ok() ? *n_written : -1) << ")";

  if (!n_written.ok()) {
    this->finish(n_written.status());
    return;
  }

  BATT_CHECK_GE(*n_written, 0);

  this->block_progress += *n_written;
  this->file_offset += *n_written;
  this->start_write();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename IoRingImpl>
inline void BasicIoRingLogInitializer<IoRingImpl>::Subtask::finish(const batt::Status& status)
{
  BATT_CHECK(!this->done);

  this->done = true;
  this->final_status.Update(status);
  const auto prior_finished_count = this->that->finished_count_.fetch_add(1);

  LLFS_VLOG(2) << BATT_INSPECT(prior_finished_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename IoRingImpl>
inline usize BasicIoRingLogInitializer<IoRingImpl>::Subtask::self_index() const
{
  return this - this->that->subtasks_.data();
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_IORING_LOG_INITIALIZER_IPP
