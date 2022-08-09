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

  this->flushed_tail_range_.lower_bound = kLogAtomicWriteSize;

  // Initialize the log page header.
  {
    PackedLogPageHeader* const header = this->get_header();

    header->magic = PackedLogPageHeader::kMagic;
    header->crc64 = -1;

    header->slot_offset = next_commit_block_slot_range.lower_bound +
                          driver->calculate().block_capacity() * ahead_of_next;

    if (ahead_of_next == 0) {
      header->commit_size = driver->get_commit_pos() - header->slot_offset;
    } else {
      header->commit_size = 0;
    }
    BATT_CHECK_LT(header->commit_size, driver->calculate().block_capacity());

    if (header->commit_size > kLogAtomicWriteSize) {
      this->flushed_tail_range_.upper_bound = header->commit_size - kLogAtomicWriteSize;
    } else {
      this->flushed_tail_range_.upper_bound = kLogAtomicWriteSize;
    }

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

  this->block_capacity_ = driver->calculate().block_capacity();

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
  THIS_VLOG(1) << "activated; slot_offset=" << this->get_header()->slot_offset;

  this->handle_commit(this->driver_->get_commit_pos());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::handle_commit(slot_offset_type known_commit_pos)
{
  PackedLogPageHeader* const header = this->get_header();

  const slot_offset_type known_flush_pos = header->slot_offset + header->commit_size;

  THIS_VLOG(1) << "handle_commit(known_commit_pos=" << known_commit_pos << ")"
               << BATT_INSPECT(header->slot_offset) << BATT_INSPECT(header->commit_size)
               << BATT_INSPECT(known_flush_pos);

  if (!slot_less_than(known_flush_pos, known_commit_pos)) {
    THIS_VLOG(1) << "caught up; waiting for commit_pos to advance..."
                 << BATT_INSPECT(known_commit_pos);

    this->driver_->wait_for_commit(/*min_required=*/known_flush_pos + 1);
    return;
  }

  const bool data_copied = this->fill_buffer(known_commit_pos);
  BATT_CHECK(data_copied);

  if (this->get_header()->commit_size > kLogAtomicWriteSize - sizeof(PackedLogPageHeader)) {
    this->flush_tail();
  } else {
    this->flush_head();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline ConstBuffer BasicIoRingLogFlushOp<DriverImpl>::get_writable_data() const
{
  const usize byte_count = batt::round_up_bits(
      kLogAtomicWriteSizeLog2, sizeof(PackedLogPageHeader) + this->get_header()->commit_size);

  return ConstBuffer{this->page_block_.get(), byte_count};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::flush_tail()
{
  THIS_VLOG(1) << "flush_tail()";

  BATT_CHECK(!this->tail_write_range_);

  const ConstBuffer writable_data = this->get_writable_data();

  this->tail_write_range_.emplace();
  this->tail_write_range_->lower_bound = this->flushed_tail_range_.upper_bound;
  this->tail_write_range_->upper_bound =
      sizeof(PackedLogPageHeader) + this->get_header()->commit_size;

  const usize unflushed_tail_offset =
      batt::round_down_bits(kLogAtomicWriteSizeLog2, this->tail_write_range_->lower_bound);

  const ConstBuffer unflushed_tail_data = writable_data + unflushed_tail_offset;

  BATT_CHECK_GT(unflushed_tail_data.size(), 0u);

  const i64 dst_file_offset = this->file_offset_ + unflushed_tail_offset;

  this->write_timer_.emplace(this->metrics_.write_latency);

  this->driver_->async_write_some(dst_file_offset, unflushed_tail_data,
                                  /*buf_index=*/this->self_index(), this->get_flush_tail_handler());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::handle_flush_tail(const StatusOr<i32>& result)
{
  THIS_VLOG(1) << "handle_flush_tail(result=" << result << ")";

  auto* const header = this->get_header();
  {
    auto on_scope_exit = batt::finally([&] {
      this->tail_write_range_ = None;
    });

    if (this->check_for_fatal_failure(result, WritingPart::kTail)) {
      return;
    }

    BATT_CHECK_GT(*result, 0);
    BATT_CHECK(this->tail_write_range_);

    const usize confirmed_upper_bound =
        batt::round_down_bits(kLogAtomicWriteSizeLog2, this->tail_write_range_->lower_bound) +
        *result;

    this->flushed_tail_range_.upper_bound =
        std::min(confirmed_upper_bound, this->tail_write_range_->upper_bound);
  }

  // Try to fetch more data from the driver now.
  //
  if (header->commit_size < this->driver_->calculate().block_capacity()) {
    const slot_offset_type known_commit_pos = this->driver_->get_commit_pos();
    if (slot_less_than(header->slot_offset + header->commit_size, known_commit_pos)) {
      this->fill_buffer(known_commit_pos);
    }
  }

  // If we can, flush more tail data; otherwise write the head to make writes durable.
  //
  if (this->flushed_tail_range_.upper_bound < sizeof(PackedLogPageHeader) + header->commit_size) {
    this->flush_tail();
  } else {
    this->flush_head();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::flush_head()
{
  THIS_VLOG(1) << "flush_head()";

  auto head_data = ConstBuffer{this->page_block_.get(), kLogAtomicWriteSize};

  this->write_timer_.emplace(this->metrics_.write_latency);

  this->driver_->async_write_some(this->file_offset_, head_data, /*buf_index=*/this->self_index(),
                                  this->get_flush_head_handler());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline void BasicIoRingLogFlushOp<DriverImpl>::handle_flush_head(const StatusOr<i32>& result)
{
  THIS_VLOG(1) << "handle_flush_head(result=" << result << ")";

  if (this->check_for_fatal_failure(result, WritingPart::kHead)) {
    return;
  }

  BATT_CHECK_EQ(*result, kLogAtomicWriteSize);

  PackedLogPageHeader* const header = this->get_header();

  // THIS SHOULD BE THE ONLY PLACE WE UPDATE `this->flush_pos_`!
  //
  this->flush_pos_ = header->slot_offset + header->commit_size;
  THIS_VLOG(1) << " -- " << BATT_INSPECT(this->flush_pos_);
  this->driver_->poll_flush_state();

  // Check to see whether we can advance to the next block.
  //
  if (header->commit_size == this->driver_->calculate().block_capacity()) {
    header->commit_size = 0;

    header->slot_offset +=
        this->driver_->calculate().block_capacity() * this->driver_->calculate().queue_depth();

    this->file_offset_ = this->driver_->calculate().block_start_file_offset_from(
        SlotLowerBoundAt{header->slot_offset});

    this->flushed_tail_range_.lower_bound = kLogAtomicWriteSize;
    this->flushed_tail_range_.upper_bound = kLogAtomicWriteSize;
  }

  this->handle_commit(this->driver_->get_commit_pos());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline bool BasicIoRingLogFlushOp<DriverImpl>::check_for_fatal_failure(const StatusOr<i32>& result,
                                                                       WritingPart writing_part)
{
  this->write_timer_ = None;

  if (!result.ok()) {
    if (batt::status_is_retryable(result.status())) {
      THIS_VLOG(1) << "EAGAIN; retrying...";
      if (writing_part == WritingPart::kHead) {
        this->flush_head();
      } else {
        this->flush_tail();
      }
      return false;
    }
    LLFS_LOG_INFO() << "flush failed: " << result.status();
    return true;
  }

  this->metrics_.bytes_written.fetch_add(*result);

  return false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename DriverImpl>
inline usize BasicIoRingLogFlushOp<DriverImpl>::self_index() const
{
  return this->driver_->index_of_flush_op(this);
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
  PackedLogPageHeader* const header = this->get_header();
  const slot_offset_type prior_commit_pos = header->slot_offset + header->commit_size;

  header->trim_pos = this->driver_->get_trim_pos();
  header->flush_pos = this->driver_->get_flush_pos();
  header->commit_pos = known_commit_pos;

  BATT_CHECK(!slot_less_than(known_commit_pos, prior_commit_pos))
      << "commit_pos should never go backwards!";

  auto dst =
      MutableBuffer{
          header + 1,
          this->driver_->calculate().block_capacity(),
      } +
      header->commit_size;

  const usize n_to_copy = std::min(known_commit_pos - prior_commit_pos, dst.size());

  THIS_VLOG(1) << "fill_buffer() -> [" << n_to_copy
               << " bytes], commit_size=" << header->commit_size << "->"
               << (header->commit_size + n_to_copy) << "/"
               << this->driver_->calculate().block_capacity();

  if (n_to_copy == 0) {
    return false;
  }

  // Copy from the ring buffer to this block.
  //
  auto src = this->driver_->get_data(prior_commit_pos);
  std::memcpy(dst.data(), src.data(), n_to_copy);

  // This should be the ONLY place where commit_size is increased!
  //
  header->commit_size += n_to_copy;

  return true;
}

#undef THIS_LOG
#undef THIS_VLOG

}  // namespace llfs

#endif  // LLFS_IORING_LOG_FLUSH_OP_IPP
