//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_FLUSH_OP_IPP
#define LLFS_IORING_LOG_FLUSH_OP_IPP

#include <llfs/ioring_log_flush_op.hpp>

#include <llfs/metrics.hpp>

namespace llfs {

#define THIS_VLOG(lvl)                                                                             \
  LLFS_VLOG(lvl) << "(driver=" << this->driver_->name() << ") LogFlushOp[" << this->self_index()   \
                 << "] "

#define THIS_LOG(lvl)                                                                              \
  LOG(lvl) << "(driver=" << this->driver_->name() << ") LogFlushOp[" << this->self_index() << "] "

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::initialize(DriverImpl* driver)
{
  BATT_CHECK_NOT_NULLPTR(driver);

  this->driver_ = driver;
  this->page_block_.reset(new PackedLogPageBuffer[this->driver_->calculate().pages_per_block()]);

  BATT_CHECK_EQ(this->driver_->calculate().pages_per_block() * sizeof(PackedLogPageBuffer),
                this->driver_->calculate().block_size());

  const usize my_index = this->self_index();

  // Figure out which flush op will be responsible for writing the next committed byte.  If that's
  // us, we need to seed our page buffer with data from the log.
  //
  const auto next_commit_slot_offset = SlotUpperBoundAt{driver->get_commit_pos() + 1};

  const LogBlockCalculator::FlushOpIndex next_commit_op_index =
      driver->calculate().flush_op_index_from(next_commit_slot_offset);

  BATT_CHECK_LT(next_commit_op_index, driver->calculate().queue_depth());

  const SlotRange next_commit_block_slot_range =
      driver->calculate().block_slot_range_from(next_commit_slot_offset);

  // Calculate how many blocks ahead of the current commit point this op is... (0 == we are the op
  // responsible for the next flushed data).
  //
  const usize ahead_of_next = [&] {
    if (my_index < next_commit_op_index) {
      return my_index + driver->calculate().queue_depth() - next_commit_op_index;
    } else {
      return my_index - next_commit_op_index;
    }
  }();

  // Initialize the log page header.
  {
    PackedLogPageHeader* const header = this->get_header();

    header->slot_offset = next_commit_block_slot_range.lower_bound +
                          driver->calculate().block_capacity() * ahead_of_next;

    if (ahead_of_next == 0) {
      header->commit_size = driver->get_commit_pos() - header->slot_offset;
    } else {
      header->commit_size = 0;
    }
    BATT_CHECK_LT(header->commit_size, driver->calculate().block_capacity());

    if (header->commit_size > 0) {
      const ConstBuffer src = driver->get_data(header->slot_offset);
      const MutableBuffer dst{header + 1, driver->calculate().block_capacity()};
      std::memcpy(dst.data(), src.data(), header->commit_size);
    }

    header->trim_pos = driver->get_trim_pos();
    header->flush_pos = driver->get_flush_pos();
    header->commit_pos = driver->get_commit_pos();

    this->file_offset_ =
        driver->calculate().block_start_file_offset_from(SlotLowerBoundAt{header->slot_offset});

    this->flush_pos_ = driver->get_flush_pos();
  }

  THIS_VLOG(1) << "initialized";

  const auto metric_name = [&](std::string_view property) {
    return batt::to_string("IoRingLogFlushOp_", my_index, "_", this->driver_->name(), "_",
                           property);
  };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), this->metrics_.n)

  ADD_METRIC_(write_latency);
  ADD_METRIC_(bytes_written);

#undef ADD_METRIC_
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline BasicIoRingLogFlushOp<DriverImpl>::~BasicIoRingLogFlushOp() noexcept
{
  global_metric_registry()  //
      .remove(this->metrics_.write_latency)
      .remove(this->metrics_.bytes_written);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::activate()
{
  const usize my_index = this->self_index();

  PackedLogPageHeader* header = this->get_header();
  header->reset(/*slot_offset=*/my_index * this->driver_->calculate().block_capacity());

  THIS_VLOG(1) << "activated; slot_offset=" << this->get_header()->slot_offset;

  this->handle_commit(this->driver_->get_commit_pos());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::handle_commit(slot_offset_type known_commit_pos)
{
  PackedLogPageHeader* header = this->get_header();

  // The end of the committed range in the current page is the known flush position because
  // `ready_to_write` is empty, and every time we fill it we immediately flush.
  //
  const slot_offset_type known_flush_pos = header->slot_offset + header->commit_size;

  THIS_VLOG(1) << "poll_commit_pos(known_commit_pos=" << known_commit_pos
               << "), known_flush_pos=" << known_flush_pos;

  BATT_CHECK_EQ(this->ready_to_write_.size(), 0u);

  if (!slot_less_than(known_flush_pos, known_commit_pos)) {
    THIS_VLOG(1) << "caught up; waiting for commit_pos to advance..."
                 << BATT_INSPECT(known_commit_pos);

    this->driver_->wait_for_commit(/*min_required=*/known_flush_pos + 1);
    return;
  }

  const bool data_copied = this->fill_buffer(known_commit_pos);
  BATT_CHECK(data_copied);

  this->start_flush();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::start_flush()
{
  BATT_CHECK_NE(this->ready_to_write_.size(), 0u);
  BATT_CHECK_EQ(this->ready_to_write_.size(),
                batt::round_down_bits(kLogAtomicWriteSizeLog2, this->ready_to_write_.size()));

  const usize progress = this->next_write_offset_ - this->file_offset_;

  THIS_VLOG(1) << "start_flush(); progress=" << progress
               << " commit_size=" << this->get_header()->commit_size << "/"
               << this->driver_->calculate().block_capacity()
               << " ready_to_write.size()=" << this->ready_to_write_.size() << "; async_write("
               << std::hex << "0x" << this->file_offset_ + progress << ", [0x"
               << this->ready_to_write_.size() << "])";
  {
    auto* header = this->get_header();
    header->trim_pos = this->driver_->get_trim_pos();
    header->flush_pos = this->driver_->get_flush_pos();
    header->commit_pos = this->driver_->get_commit_pos();
  }

  this->write_timer_.emplace(this->metrics_.write_latency);

  this->driver_->async_write_some(this->next_write_offset_, this->ready_to_write_,
                                  (i32)this->self_index(), this->get_flush_handler());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::finish_flush()
{
  THIS_VLOG(1) << "finish_flush()";

  this->ready_to_write_ = ConstBuffer{this->page_block_.get(), kLogAtomicWriteSize};
  this->next_write_offset_ = this->file_offset_;
  {
    auto* header = this->get_header();
    header->trim_pos = this->driver_->get_trim_pos();
    header->flush_pos =
        slot_max(header->slot_offset + header->commit_size, this->driver_->get_flush_pos());
  }

  this->write_timer_.emplace(this->metrics_.write_latency);

  this->driver_->async_write_some(this->file_offset_, this->ready_to_write_,
                                  (int)this->self_index(), this->get_flush_handler());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::handle_flush(const StatusOr<i32>& result)
{
  this->write_timer_ = None;

  THIS_VLOG(1) << "async_write() -> " << result;

  if (!result.ok()) {
    if (batt::status_is_retryable(result.status())) {
      THIS_VLOG(1) << "EAGAIN; retrying...";
      this->start_flush();
      return;
    }
    LLFS_LOG_INFO() << "flush failed: " << result.status();
    return;
  }
  BATT_CHECK_EQ(*result & 511, 0u)
      << "We expect that I/O will complete in disk block aligned chunks";
  BATT_CHECK_GE(*result, 0);

  this->ready_to_write_ += *result;
  this->next_write_offset_ += *result;
  this->metrics_.bytes_written.fetch_add(*result);

  // Handle short write.
  //
  if (this->ready_to_write_.size() != 0) {
    // Top off the buffer, if possible.
    //
    (void)this->fill_buffer(this->driver_->get_commit_pos());

    // Retry.
    //
    this->start_flush();
    return;
  }

  PackedLogPageHeader* header = this->get_header();

  // Make sure the on-device flush_pos reflects the flushed data.
  //
  if (slot_less_than(header->flush_pos, header->slot_offset + header->commit_size)) {
    this->finish_flush();
    return;
  }

  // We've completed flushing the page.  Update the local flush_pos.
  // THIS SHOULD BE THE ONLY PLACE WE UPDATE `this->flush_pos_`!
  //
  THIS_VLOG(1) << "flushed entire buffer; header->commit_size=" << header->commit_size << "/"
               << this->driver_->calculate().block_capacity();

  this->flush_pos_ = header->slot_offset + header->commit_size;
  this->driver_->poll_flush_state();

  // We are done with this generation.  Advance slot offset by one complete window
  // (kPageCapacity * kQueueDepth).
  //
  if (header->commit_size == this->driver_->calculate().block_capacity()) {
    header->commit_size = 0;
    header->slot_offset +=
        this->driver_->calculate().block_capacity() * this->driver_->calculate().queue_depth();

    this->file_offset_ = this->driver_->calculate().block_start_file_offset_from(
        SlotLowerBoundAt{header->slot_offset});
  }

  this->handle_commit(this->driver_->get_commit_pos());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline usize BasicIoRingLogFlushOp<DriverImpl>::self_index() const
{
  return this->driver_->index_of_flush_op(*this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline auto BasicIoRingLogFlushOp<DriverImpl>::get_header() const -> PackedLogPageHeader*
{
  return reinterpret_cast<PackedLogPageHeader*>(this->page_block_.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline MutableBuffer BasicIoRingLogFlushOp<DriverImpl>::get_buffer() const
{
  return MutableBuffer{
      this->page_block_.get(),
      this->driver_->calculate().block_size(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline bool BasicIoRingLogFlushOp<DriverImpl>::fill_buffer(slot_offset_type known_commit_pos)
{
  PackedLogPageHeader* header = this->get_header();
  slot_offset_type prior_commit_pos = header->slot_offset + header->commit_size;

  BATT_CHECK(!slot_less_than(known_commit_pos, prior_commit_pos))
      << "commit_pos should never go backwards!";

  auto buffer =
      MutableBuffer{
          header + 1,
          this->driver_->calculate().block_capacity(),
      } +
      header->commit_size;

  usize n_to_copy = std::min(known_commit_pos - prior_commit_pos, buffer.size());
  THIS_VLOG(1) << "fill_buffer() -> [" << n_to_copy
               << " bytes], commit_size=" << header->commit_size << "->"
               << (header->commit_size + n_to_copy) << "/"
               << this->driver_->calculate().block_capacity();
  if (n_to_copy == 0) {
    return false;
  }

  // Copy from the ring buffer to this block.
  //
  std::memcpy(buffer.data(), this->driver_->get_data(prior_commit_pos).data(), n_to_copy);

  // This should be the ONLY place where commit_size is increased!
  //
  header->commit_size += n_to_copy;

  // If we appended new data to the buffer, which forces an update to the header, then we must
  // re-write the whole page.
  //
  this->ready_to_write_ = ConstBuffer{
      this->page_block_.get(),
      batt::round_up_bits(kLogAtomicWriteSizeLog2, sizeof(PackedPageHeader) + header->commit_size)};

  this->next_write_offset_ = this->file_offset_;

  // If we're only writing one atomic page worth of data, don't change the buffer.
  //
  if (this->ready_to_write_.size() <= kLogAtomicWriteSize) {
    header->flush_pos =
        slot_max(header->slot_offset + header->commit_size, this->driver_->get_flush_pos());
    return true;
  }

  // Since we are going to rewrite the header afterwards anyhow, skip any previously written data.
  //
  u64 offset = 0;
  if (slot_less_than(header->slot_offset, this->flush_pos_)) {
    BATT_CHECK(slot_less_than(this->flush_pos_, header->slot_offset + header->commit_size))
        << BATT_INSPECT(this->flush_pos_) << BATT_INSPECT(header->slot_offset)
        << BATT_INSPECT(this->driver_->calculate().block_capacity());

    offset = batt::round_down_bits(
        kLogAtomicWriteSizeLog2,
        sizeof(PackedLogPageHeader) + slot_distance(header->slot_offset, this->flush_pos_));
  }
  offset = std::max(kLogAtomicWriteSize, offset);

  BATT_CHECK_LE(offset, this->ready_to_write_.size());

  this->ready_to_write_ += offset;
  this->next_write_offset_ += offset;

  return this->ready_to_write_.size() > 0;
}

#undef THIS_LOG
#undef THIS_VLOG

}  // namespace llfs

#endif  // LLFS_IORING_LOG_FLUSH_OP_IPP
