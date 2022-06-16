//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_flush_op.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring_log_device.hpp>
#include <llfs/metrics.hpp>

namespace llfs {

#define THIS_VLOG(lvl)                                                                             \
  VLOG(lvl) << "(driver=" << this->driver_->name_ << ") LogFlushOp[" << this->self_index() << "] "

#define THIS_LOG(lvl)                                                                              \
  LOG(lvl) << "(driver=" << this->driver_->name_ << ") LogFlushOp[" << this->self_index() << "] "

void IoRingLogFlushOp::initialize(IoRingLogDriver* driver)
{
  BATT_CHECK_NOT_NULLPTR(driver);

  this->driver_ = driver;
  this->page_block_.reset(new LogPageBuffer[this->driver_->pages_per_block()]);

  BATT_CHECK_EQ(this->driver_->pages_per_block() * sizeof(LogPageBuffer),
                this->driver_->block_size());

  const usize my_index = this->self_index();

  this->file_offset_ = this->driver_->log_start_ + my_index * this->driver_->block_size();

  THIS_VLOG(1) << "initialized";

  const auto metric_name = [&](std::string_view property) {
    return batt::to_string("IoRingLogFlushOp_", my_index, "_", this->driver_->name_, "_", property);
  };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), this->metrics_.n)

  ADD_METRIC_(write_latency);
  ADD_METRIC_(bytes_written);

#undef ADD_METRIC_
}

IoRingLogFlushOp::~IoRingLogFlushOp() noexcept
{
  global_metric_registry()  //
      .remove(this->metrics_.write_latency)
      .remove(this->metrics_.bytes_written);
}

void IoRingLogFlushOp::activate()
{
  const usize my_index = this->self_index();

  PackedPageHeader* header = this->get_header();
  header->magic = PackedPageHeader::kMagic;
  header->slot_offset = my_index * this->driver_->block_capacity();
  header->commit_size = 0;
  header->crc64 = -1;  // TODO [tastolfi 2021-06-17]

  THIS_VLOG(1) << "activated; slot_offset=" << this->get_header()->slot_offset;

  this->poll_commit_pos(this->driver_->commit_pos_.get_value());
}

IoRing& IoRingLogFlushOp::get_ioring()
{
  return this->driver_->ioring_;
}

void IoRingLogFlushOp::poll_commit_state()
{
  this->driver_->poll_commit_state();
}

void IoRingLogFlushOp::poll_commit_pos(slot_offset_type known_commit_pos)
{
  PackedPageHeader* header = this->get_header();

  // The end of the committed range in the current page is the known flush position because
  // `ready_to_write` is empty, and every time we fill it we immediately flush.
  //
  const slot_offset_type known_flush_pos = header->slot_offset + header->commit_size;

  THIS_VLOG(1) << "poll_commit_pos(known_commit_pos=" << known_commit_pos
               << "), known_flush_pos=" << known_flush_pos;

  BATT_CHECK_EQ(this->ready_to_write_.size(), 0u);

  if (!slot_less_than(known_flush_pos, known_commit_pos)) {
    THIS_VLOG(1) << "caught up; waiting for commit_pos to advance...";
    this->wait_for_commit(known_commit_pos, known_flush_pos + 1);
    return;
  }

  const bool data_copied = this->fill_buffer(known_commit_pos);
  BATT_CHECK(data_copied);

  this->start_flush();
}

void IoRingLogFlushOp::wait_for_commit(slot_offset_type known_commit_pos,
                                       slot_offset_type min_required)
{
  THIS_VLOG(1) << "wait_for_commit(known_commit_pos=" << known_commit_pos
               << ", min_required=" << min_required << ")";

  BATT_CHECK_EQ(this->ready_to_write_.size(), 0u);

  (void)known_commit_pos;
  this->driver_->waiting_for_commit_.push(min_required);
}

void IoRingLogFlushOp::start_flush()
{
  BATT_CHECK_NE(this->ready_to_write_.size(), 0u);
  BATT_CHECK_EQ(this->ready_to_write_.size(),
                batt::round_down_bits(kLogAtomicWriteBits, this->ready_to_write_.size()));

  const usize progress = this->next_write_offset_ - this->file_offset_;

  THIS_VLOG(1) << "start_flush(); progress=" << progress
               << " commit_size=" << this->get_header()->commit_size << "/"
               << this->driver_->block_capacity()
               << " ready_to_write.size()=" << this->ready_to_write_.size() << "; async_write("
               << std::hex << "0x" << this->file_offset_ + progress << ", [0x"
               << this->ready_to_write_.size() << "])";
  {
    auto* header = this->get_header();
    header->trim_pos = this->driver_->get_trim_pos();
    header->flush_pos = slot_max(header->flush_pos, this->driver_->get_flush_pos());
  }

  this->write_timer_.emplace(this->metrics_.write_latency);

  this->driver_->file_.async_write_some_fixed(this->next_write_offset_, this->ready_to_write_,
                                              (int)this->self_index(), this->get_flush_handler());
}

void IoRingLogFlushOp::finish_flush()
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

  this->driver_->file_.async_write_some_fixed(this->file_offset_, this->ready_to_write_,
                                              (int)this->self_index(), this->get_flush_handler());
}

void IoRingLogFlushOp::handle_flush(const StatusOr<i32>& result)
{
  this->write_timer_ = None;

  THIS_VLOG(1) << "async_write() -> " << result;

  if (!result.ok()) {
    if (batt::status_is_retryable(result.status())) {
      THIS_VLOG(1) << "EAGAIN; retrying...";
      this->start_flush();
      return;
    }
    LOG(INFO) << "flush failed: " << result.status();
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
    (void)this->fill_buffer(this->driver_->commit_pos_.get_value());

    // Retry.
    //
    this->start_flush();
    return;
  }

  PackedPageHeader* header = this->get_header();

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
               << this->driver_->block_capacity();

  this->flush_pos_ = header->slot_offset + header->commit_size;
  this->driver_->poll_flush_state();

  // We are done with this generation.  Advance slot offset by one complete window
  // (kPageCapacity * kQueueDepth).
  //
  if (header->commit_size == this->driver_->block_capacity()) {
    header->slot_offset += this->driver_->block_capacity() * this->driver_->queue_depth();
    header->commit_size = 0;
    this->file_offset_ = (this->file_offset_ - this->driver_->log_start_ +
                          this->driver_->block_size() * this->driver_->queue_depth()) %
                             (this->driver_->log_end_ - this->driver_->log_start_) +
                         this->driver_->log_start_;
  }

  this->poll_commit_pos(this->driver_->commit_pos_.get_value());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

usize IoRingLogFlushOp::self_index()
{
  return std::distance(this->driver_->flush_ops_.data(), this);
}

auto IoRingLogFlushOp::get_header() const -> PackedPageHeader*
{
  return reinterpret_cast<PackedPageHeader*>(this->page_block_.get());
}

MutableBuffer IoRingLogFlushOp::get_buffer() const
{
  return MutableBuffer{this->page_block_.get(), this->driver_->block_size()};
}

bool IoRingLogFlushOp::fill_buffer(slot_offset_type known_commit_pos)
{
  PackedPageHeader* header = this->get_header();
  slot_offset_type prior_commit_pos = header->slot_offset + header->commit_size;

  BATT_CHECK(!slot_less_than(known_commit_pos, prior_commit_pos))
      << "commit_pos should never go backwards!";

  auto buffer = MutableBuffer{header + 1, this->driver_->block_capacity()} + header->commit_size;

  usize n_to_copy = std::min(known_commit_pos - prior_commit_pos, buffer.size());
  THIS_VLOG(1) << "fill_buffer() -> [" << n_to_copy
               << " bytes], commit_size=" << header->commit_size << "->"
               << (header->commit_size + n_to_copy) << "/" << this->driver_->block_capacity();
  if (n_to_copy == 0) {
    return false;
  }

  // Copy from the ring buffer to this block.
  //
  std::memcpy(buffer.data(), this->driver_->context_.buffer_.get(prior_commit_pos).data(),
              n_to_copy);

  // This should be the ONLY place where commit_size is increased!
  //
  header->commit_size += n_to_copy;

  // If we appended new data to the buffer, which forces an update to the header, then we must
  // re-write the whole page.
  //
  this->ready_to_write_ = ConstBuffer{
      this->page_block_.get(),
      batt::round_up_bits(kLogAtomicWriteBits, sizeof(PackedPageHeader) + header->commit_size)};

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
        << BATT_INSPECT(this->driver_->block_capacity());

    offset = batt::round_down_bits(
        kLogAtomicWriteBits,
        sizeof(PackedPageHeader) + slot_distance(header->slot_offset, this->flush_pos_));
  }
  offset = std::max(kLogAtomicWriteSize, offset);

  BATT_CHECK_LE(offset, this->ready_to_write_.size());

  this->ready_to_write_ += offset;
  this->next_write_offset_ += offset;

  return this->ready_to_write_.size() > 0;
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
