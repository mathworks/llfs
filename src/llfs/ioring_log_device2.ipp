//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE2_IPP
#define LLFS_IORING_LOG_DEVICE2_IPP

#include <llfs/data_reader.hpp>
#include <llfs/slot_writer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline /*explicit*/ IoRingLogDriver2<StorageT>::IoRingLogDriver2(
    LogStorageDriverContext& context,        //
    const IoRingLogConfig2& config,          //
    const LogDeviceRuntimeOptions& options,  //
    StorageT&& storage                       //
    ) noexcept
    : context_{context}
    , config_{config}
    , options_{options}
    , storage_{std::move(storage)}
    , control_block_memory_{new AlignedUnit[(this->data_page_size_ + sizeof(AlignedUnit) - 1) /
                                            sizeof(AlignedUnit)]}
    , control_block_buffer_{this->control_block_memory_.get(), this->data_page_size_}
{
  BATT_CHECK_GE(this->config_.data_alignment_log2, this->config_.device_page_size_log2);
  BATT_CHECK_GE(this->data_page_size_, sizeof(PackedLogControlBlock2));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::open() noexcept
{
  LLFS_VLOG(1) << "open()";

  this->initialize_handler_memory_pool();

  BATT_REQUIRE_OK(this->storage_.register_fd());
  BATT_REQUIRE_OK(this->storage_.register_buffers(
      seq::single_item(MutableBuffer{this->control_block_memory_.get(), this->data_page_size_}) |
      seq::boxed()));

  LLFS_VLOG(1) << "starting event loop";

  this->storage_.on_work_started();
  this->work_started_.store(true);

  this->event_loop_task_.emplace(this->storage_, "IoRingLogDriver2::open()");

  BATT_REQUIRE_OK(this->read_control_block());
  BATT_REQUIRE_OK(this->read_log_data());

  this->storage_.post_to_event_loop([this](auto&&... /*ignored*/) {
    LLFS_VLOG(1) << "initial event";

    this->event_thread_id_ = std::this_thread::get_id();
    this->poll();
  });

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::close()
{
  this->halt();
  this->join();

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline void IoRingLogDriver2<StorageT>::halt()
{
  const bool previously_halted = this->halt_requested_.exchange(true);
  if (!previously_halted) {
    for (batt::Watch<llfs::slot_offset_type>& watch : this->observed_watch_) {
      watch.close();
    }
    this->trim_pos_.close();
    this->flush_pos_.close();

    if (this->work_started_.exchange(false)) {
      this->storage_.on_work_finished();
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline void IoRingLogDriver2<StorageT>::join()
{
  if (this->event_loop_task_) {
    this->event_loop_task_->join();
    this->event_loop_task_ = None;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::read_control_block()
{
  LLFS_VLOG(1) << "read_control_block()";

  const usize control_block_size = this->data_page_size_;

  auto* p_control_block =
      reinterpret_cast<PackedLogControlBlock2*>(this->control_block_memory_.get());

  std::memset(p_control_block, 0, control_block_size);

  MutableBuffer mutable_buffer{p_control_block, control_block_size};

  BATT_REQUIRE_OK(this->storage_.read_all(this->config_.control_block_offset, mutable_buffer));

  const slot_offset_type recovered_trim_pos = p_control_block->trim_pos;
  const slot_offset_type recovered_flush_pos = p_control_block->flush_pos;

  LLFS_VLOG(1) << BATT_INSPECT(recovered_trim_pos) << BATT_INSPECT(recovered_flush_pos)
               << BATT_INSPECT(p_control_block->generation);

  LLFS_CHECK_SLOT_LE(recovered_trim_pos, recovered_flush_pos);

  this->reset_trim_pos(recovered_trim_pos);
  this->reset_flush_pos(recovered_flush_pos);

  this->data_begin_ = this->config_.control_block_offset + this->data_page_size_;
  this->data_end_ = this->data_begin_ + p_control_block->data_size;

  this->control_block_buffer_ = ConstBuffer{
      p_control_block,
      batt::round_up_to<usize>(this->device_page_size_, p_control_block->control_header_size),
  };
  this->control_block_ = p_control_block;

  // TODO [tastolfi 2024-06-11] verify control block values against config where possible.

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::read_log_data()
{
  LLFS_VLOG(1) << "read_log_data()";

  const slot_offset_type recovered_trim_pos = this->trim_pos_.get_value();
  const slot_offset_type recovered_flush_pos = this->flush_pos_.get_value();

  const slot_offset_type aligned_trim_pos =
      batt::round_down_bits(this->config_.device_page_size_log2, recovered_trim_pos);

  const slot_offset_type aligned_flush_pos =
      batt::round_up_bits(this->config_.device_page_size_log2, recovered_flush_pos);

  const i64 read_begin_offset =
      this->data_begin_ + BATT_CHECKED_CAST(i64, aligned_trim_pos % this->context_.buffer_.size());

  MutableBuffer mutable_buffer = resize_buffer(this->context_.buffer_.get_mut(aligned_trim_pos),
                                               aligned_flush_pos - aligned_trim_pos);

  const i64 read_end_offset = read_begin_offset + BATT_CHECKED_CAST(i64, mutable_buffer.size());

  LLFS_VLOG(1) << BATT_INSPECT(recovered_trim_pos) << BATT_INSPECT(recovered_flush_pos);

  if (read_end_offset <= this->data_end_) {
    BATT_REQUIRE_OK(this->storage_.read_all(read_begin_offset, mutable_buffer));

  } else {
    // lower == lower offset in the file.
    //
    const usize lower_part_size = read_end_offset - this->data_end_;
    const usize upper_part_size = this->data_end_ - read_begin_offset;

    BATT_CHECK_EQ(mutable_buffer.size(), lower_part_size + upper_part_size);

    MutableBuffer lower_part_buffer = mutable_buffer + upper_part_size;
    MutableBuffer upper_part_buffer = resize_buffer(mutable_buffer, upper_part_size);

    BATT_CHECK_EQ(lower_part_buffer.size(), lower_part_size);
    BATT_CHECK_EQ(upper_part_buffer.size(), upper_part_size);

    BATT_REQUIRE_OK(this->storage_.read_all(this->data_begin_, lower_part_buffer));
    BATT_REQUIRE_OK(this->storage_.read_all(read_begin_offset, upper_part_buffer));
  }

  return this->recover_flush_pos();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
slot_offset_type IoRingLogDriver2<StorageT>::recover_flushed_commit_point() const noexcept
{
  const slot_offset_type recovered_trim_pos = this->control_block_->trim_pos;
  const slot_offset_type recovered_flush_pos = this->control_block_->flush_pos;

  slot_offset_type slot_offset = recovered_trim_pos;

  std::vector<slot_offset_type> sorted_commit_points(this->control_block_->commit_points.begin(),
                                                     this->control_block_->commit_points.end());

  std::sort(sorted_commit_points.begin(), sorted_commit_points.end(), SlotOffsetOrder{});

  LLFS_VLOG(1) << BATT_INSPECT_RANGE(sorted_commit_points);

  auto iter = std::upper_bound(sorted_commit_points.begin(), sorted_commit_points.end(),
                               recovered_flush_pos, SlotOffsetOrder{});

  if (iter != sorted_commit_points.begin()) {
    --iter;
    slot_offset = slot_max(slot_offset, *iter);
    LLFS_VLOG(1) << " -- using commit point: " << *iter;
  }

  LLFS_CHECK_SLOT_LE(slot_offset, recovered_flush_pos);
  return slot_offset;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
Status IoRingLogDriver2<StorageT>::recover_flush_pos() noexcept
{
  const slot_offset_type recovered_flush_pos = this->flush_pos_.get_value();

  slot_offset_type slot_offset = this->recover_flushed_commit_point();

  ConstBuffer buffer =
      resize_buffer(this->context_.buffer_.get(slot_offset), recovered_flush_pos - slot_offset);

  slot_offset_type confirmed_flush_pos = slot_offset;

  // This should be correct, since commit is called only once per atomic range, and atomic ranges
  // are only recoverable if no part of the range (including the begin/end tokens) has been trimmed.
  //
  bool inside_atomic_range = false;

  for (;;) {
    DataReader reader{buffer};
    const usize bytes_available_before = reader.bytes_available();
    Optional<u64> slot_body_size = reader.read_varint();

    if (!slot_body_size) {
      // Partially committed slot (couldn't even read a whole varint for the slot header!)  Break
      // out of the loop.
      //
      LLFS_VLOG(1) << " -- Incomplete slot header, exiting loop;" << BATT_INSPECT(slot_offset)
                   << BATT_INSPECT(bytes_available_before);
      break;
    }

    const usize bytes_available_after = reader.bytes_available();
    const usize slot_header_size = bytes_available_before - bytes_available_after;
    const usize slot_size = slot_header_size + *slot_body_size;

    if (slot_size > buffer.size()) {
      // Partially committed slot; break out of the loop without updating slot_offset (we're
      // done!)
      //
      LLFS_VLOG(1) << " -- Incomplete slot body, exiting loop;" << BATT_INSPECT(slot_offset)
                   << BATT_INSPECT(bytes_available_before) << BATT_INSPECT(bytes_available_after)
                   << BATT_INSPECT(slot_header_size) << BATT_INSPECT(slot_body_size)
                   << BATT_INSPECT(slot_size);
      break;
    }

    // Check for control token; this indicates the beginning or end of an atomic slot range.
    //
    if (*slot_body_size == 0) {
      if (slot_header_size == SlotWriter::WriterLock::kBeginAtomicRangeTokenSize) {
        inside_atomic_range = true;
      } else if (slot_header_size == SlotWriter::WriterLock::kEndAtomicRangeTokenSize) {
        inside_atomic_range = false;
      }
    }

    buffer += slot_size;
    slot_offset += slot_size;

    // If inside an atomic slot range, we hold off on updating the confirmed_flush_pos, just in
    // case the flushed data is cut off before the end of the atomic range.
    //
    if (!inside_atomic_range) {
      confirmed_flush_pos = slot_offset;
    }
  }

  LLFS_VLOG(1) << " -- Slot scan complete;" << BATT_INSPECT(slot_offset);

  this->reset_flush_pos(confirmed_flush_pos);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::reset_trim_pos(slot_offset_type new_trim_pos)
{
  this->trim_pos_.set_value(new_trim_pos);
  this->observed_watch_[kTargetTrimPos].set_value(new_trim_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::reset_flush_pos(slot_offset_type new_flush_pos)
{
  this->observed_watch_[kCommitPos].set_value(new_flush_pos);
  this->unflushed_lower_bound_ = new_flush_pos;
  this->known_flush_pos_ = new_flush_pos;
  this->flush_pos_.set_value(new_flush_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline void IoRingLogDriver2<StorageT>::poll() noexcept
{
  auto observed_commit_pos = this->observe(CommitPos{});
  auto observed_target_trim_pos = this->observe(TargetTrimPos{});

  LLFS_VLOG(1) << "poll()" << BATT_INSPECT(observed_commit_pos)
               << BATT_INSPECT(observed_target_trim_pos);

  this->start_flush(observed_commit_pos);
  this->start_control_block_update(observed_target_trim_pos);
  this->wait_for_slot_offset_change(TargetTrimPos{observed_target_trim_pos});
  this->wait_for_slot_offset_change(CommitPos{observed_commit_pos});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
template <typename T>
inline void IoRingLogDriver2<StorageT>::wait_for_slot_offset_change(T observed_value) noexcept
{
  static constexpr batt::StaticType<T> kKey;

  if (this->waiting_[kKey]) {
    return;
  }
  this->waiting_[kKey] = true;

  HandlerMemory* const p_mem = this->alloc_handler_memory();

  this->observed_watch_[kKey].async_wait(
      observed_value,

      // Use pre-allocated memory to store the handler in the watch observer list.
      //
      batt::make_custom_alloc_handler(
          *p_mem, [this, p_mem](const StatusOr<slot_offset_type>& new_value) mutable {
            // The callback must run on the IO event loop task thread, so post it
            // here, re-using the pre-allocated handler memory.
            //
            this->storage_.post_to_event_loop(batt::make_custom_alloc_handler(
                *p_mem, [this, p_mem, new_value](const StatusOr<i32>& /*ignored*/) {
                  // We no longer need the handler memory, so free now.
                  //
                  this->free_handler_memory(p_mem);

                  this->waiting_[kKey] = false;

                  if (!new_value.ok()) {
                    this->context_.update_error_status(new_value.status());
                    return;
                  }

                  this->poll();
                }));
          }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline void IoRingLogDriver2<StorageT>::start_flush(slot_offset_type observed_commit_pos)
{
  slot_offset_type flush_upper_bound = this->unflushed_lower_bound_;

  for (usize repeat = 0; repeat < 2; ++repeat) {
    //----- --- -- -  -  -   -

    // Don't start a write if we are at the max concurrent writes limit.
    //
    if (this->writes_pending_ == this->options_.max_concurrent_writes) {
      LLFS_VLOG(1) << "start_flush - at max writes pending";
      break;
    }

    // Don't start a write if we have no data to flush.
    //
    const usize unflushed_size = slot_clamp_distance(flush_upper_bound, observed_commit_pos);
    if (unflushed_size == 0) {
      LLFS_VLOG(1) << "start_flush - unflushed_size == 0";
      break;
    }

    // Don't start a write if there is already a pending write and we aren't at the threshold.
    //
    if (this->writes_pending_ != 0 && unflushed_size < this->options_.flush_delay_threshold) {
      LLFS_VLOG(1) << "start_flush - no action taken: " << BATT_INSPECT(unflushed_size)
                   << BATT_INSPECT(this->writes_pending_);
      break;
    }

    // All conditions for writing have been met; calculate the aligned range and start flushing.
    //
    SlotRange slot_range{
        .lower_bound = flush_upper_bound,
        .upper_bound = observed_commit_pos,
    };

    // Check for split range.
    {
      const usize physical_lower_bound = slot_range.lower_bound % this->context_.buffer_.size();
      const usize physical_upper_bound = slot_range.upper_bound % this->context_.buffer_.size();

      if (physical_lower_bound > physical_upper_bound && physical_upper_bound != 0) {
        const slot_offset_type new_upper_bound =
            slot_range.lower_bound + (this->context_.buffer_.size() - physical_lower_bound);

        LLFS_VLOG(1) << "Clipping: " << slot_range.upper_bound << " -> " << new_upper_bound << ";"
                     << BATT_INSPECT(physical_lower_bound) << BATT_INSPECT(physical_upper_bound);

        slot_range.upper_bound = new_upper_bound;
      }
    }

    SlotRange aligned_range = this->get_aligned_range(slot_range);

    // If this flush would overlap with an ongoing one (at the last device page) then trim the
    // aligned_range so it doesn't.
    //
    if (this->flush_tail_) {
      if (slot_less_than(aligned_range.lower_bound, this->flush_tail_->upper_bound)) {
        aligned_range.lower_bound = this->flush_tail_->upper_bound;
        if (aligned_range.empty()) {
          flush_upper_bound = this->flush_tail_->upper_bound;
          continue;
        }
      }
    }

    // Replace the current flush_tail_ slot range.
    //
    const SlotRange new_flush_tail = this->get_aligned_tail(aligned_range);
    if (this->flush_tail_) {
      BATT_CHECK_NE(new_flush_tail, *this->flush_tail_);
    }
    BATT_CHECK(!new_flush_tail.empty());
    this->flush_tail_.emplace(new_flush_tail);

    // Start writing!
    //
    this->start_flush_write(slot_range, aligned_range);
    flush_upper_bound = this->unflushed_lower_bound_;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline SlotRange IoRingLogDriver2<StorageT>::get_aligned_range(
    const SlotRange& slot_range) const noexcept
{
  return SlotRange{
      .lower_bound = batt::round_down_bits(this->config_.data_alignment_log2,  //
                                           slot_range.lower_bound),
      .upper_bound = batt::round_up_bits(this->config_.data_alignment_log2,  //
                                         slot_range.upper_bound),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline SlotRange IoRingLogDriver2<StorageT>::get_aligned_tail(
    const SlotRange& aligned_range) const noexcept
{
  return SlotRange{
      .lower_bound = aligned_range.upper_bound - this->data_page_size_,
      .upper_bound = aligned_range.upper_bound,
  };
}

//==#==========+=t=+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline void IoRingLogDriver2<StorageT>::start_flush_write(const SlotRange& slot_range,
                                                          const SlotRange& aligned_range) noexcept
{
  LLFS_VLOG(1) << "start_flush_write(" << slot_range << ", " << aligned_range << ")";

  this->unflushed_lower_bound_ = slot_max(this->unflushed_lower_bound_, slot_range.upper_bound);

  const i64 write_offset =
      this->data_begin_ + (aligned_range.lower_bound % this->context_.buffer_.size());

  ConstBuffer buffer =
      resize_buffer(this->context_.buffer_.get(aligned_range.lower_bound), aligned_range.size());

  BATT_CHECK_LE(write_offset + (i64)buffer.size(), this->data_end_);

  LLFS_VLOG(1) << " -- async_write_some(offset=" << write_offset << ".."
               << write_offset + buffer.size() << ", size=" << buffer.size() << ")";

  ++this->writes_pending_;
  this->writes_max_ = std::max(this->writes_max_, this->writes_pending_);

  BATT_CHECK_LE(this->writes_pending_, this->options_.max_concurrent_writes);

  this->storage_.async_write_some(write_offset, buffer,
                                  this->make_write_handler([this, slot_range, aligned_range](
                                                               Self* this_, StatusOr<i32> result) {
                                    --this_->writes_pending_;
                                    this_->handle_flush_write(slot_range, aligned_range, result);
                                  }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline void IoRingLogDriver2<StorageT>::handle_flush_write(const SlotRange& slot_range,
                                                           const SlotRange& aligned_range,
                                                           StatusOr<i32> result)
{
  LLFS_VLOG(1) << "handle_flush_result(result=" << result << ")" << BATT_INSPECT(slot_range);

  const usize bytes_written = result.ok() ? *result : 0;

  SlotRange aligned_tail = this->get_aligned_tail(aligned_range);

  SlotRange flushed_range{
      .lower_bound = slot_max(slot_range.lower_bound, aligned_range.lower_bound),
      .upper_bound = slot_min(aligned_range.lower_bound + bytes_written, slot_range.upper_bound),
  };

  const bool is_tail = (this->flush_tail_ && *this->flush_tail_ == aligned_tail);
  LLFS_DVLOG(1) << BATT_INSPECT(is_tail);
  if (is_tail) {
    this->flush_tail_ = None;
    this->unflushed_lower_bound_ = flushed_range.upper_bound;
  }

  LLFS_DVLOG(1) << BATT_INSPECT(flushed_range);

  if (!result.ok()) {
    LLFS_VLOG(1) << "(handle_flush_write) error: " << result.status();
    this->handle_write_error(result.status());
    return;
  }

  BATT_CHECK(!flushed_range.empty());

  this->update_known_flush_pos(flushed_range);

  const auto observed_commit_pos = this->observe(CommitPos{});

  if (!is_tail) {
    SlotRange updated_range{
        .lower_bound = flushed_range.upper_bound,
        .upper_bound = slot_min(aligned_range.upper_bound, observed_commit_pos),
    };

    if (!updated_range.empty()) {
      this->start_flush_write(updated_range, this->get_aligned_range(updated_range));
    }
  }

  this->poll();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::update_known_flush_pos(const SlotRange& flushed_range) noexcept
{
  // Insert the flushed_range into the min-heap.
  //
  this->flushed_ranges_.emplace_back(flushed_range);
  std::push_heap(this->flushed_ranges_.begin(), this->flushed_ranges_.end(), SlotRangePriority{});

  // Advance this->known_flush_pos_ as long as we see flushed ranges without gaps in between.
  //
  while (!this->flushed_ranges_.empty()) {
    SlotRange& next_range = this->flushed_ranges_.front();

    // Found a gap; we are done!
    //
    if (next_range.lower_bound != this->known_flush_pos_) {
      LLFS_CHECK_SLOT_LT(this->known_flush_pos_, next_range.lower_bound);
      break;
    }

    this->known_flush_pos_ = next_range.upper_bound;

    // Pop the min range off the heap and keep going.
    //
    std::pop_heap(this->flushed_ranges_.begin(), this->flushed_ranges_.end(), SlotRangePriority{});
    this->flushed_ranges_.pop_back();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::start_control_block_update(
    slot_offset_type observed_target_trim_pos) noexcept
{
  if (this->writing_control_block_ || this->trim_pos_.is_closed() || this->flush_pos_.is_closed()) {
    return;
  }

  BATT_CHECK_NOT_NULLPTR(this->control_block_) << "Forgot to call read_control_block()?";

  const slot_offset_type effective_target_trim_pos =
      slot_min(this->known_flush_pos_, observed_target_trim_pos);
  const slot_offset_type observed_trim_pos = this->trim_pos_.get_value();
  const slot_offset_type observed_flush_pos = this->flush_pos_.get_value();

  if (observed_trim_pos == effective_target_trim_pos &&
      observed_flush_pos == this->known_flush_pos_) {
    return;
  }

  LLFS_VLOG(1) << "start_control_block_update():"
               << " trim=" << observed_trim_pos << "->" << observed_target_trim_pos
               << " (effective=" << effective_target_trim_pos << ")"
               << " flush=" << observed_flush_pos << "->" << this->known_flush_pos_;

  BATT_CHECK_EQ(observed_trim_pos, this->control_block_->trim_pos);
  BATT_CHECK_EQ(observed_flush_pos, this->control_block_->flush_pos);

  const slot_offset_type latest_commit_pos = this->observe(CommitPos{});
  auto& next_commit_pos_slot =
      this->control_block_->commit_points[this->control_block_->next_commit_i];

  if (next_commit_pos_slot != latest_commit_pos) {
    next_commit_pos_slot = latest_commit_pos;

    this->control_block_->next_commit_i =
        (this->control_block_->next_commit_i + 1) % this->control_block_->commit_points.size();
  }

  LLFS_CHECK_SLOT_LE(effective_target_trim_pos, this->known_flush_pos_);
  BATT_CHECK_LE(this->known_flush_pos_ - effective_target_trim_pos, this->config_.log_capacity);

  this->control_block_->trim_pos = effective_target_trim_pos;
  this->control_block_->flush_pos = this->known_flush_pos_;
  this->control_block_->generation = this->control_block_->generation + 1;

  this->writing_control_block_ = true;

  this->storage_.async_write_some_fixed(
      this->config_.control_block_offset, this->control_block_buffer_, /*buf_index=*/0,
      this->make_write_handler([](Self* this_, StatusOr<i32> result) {
        this_->handle_control_block_update(result);
      }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::handle_control_block_update(StatusOr<i32> result) noexcept
{
  LLFS_VLOG(1) << "handle_control_block_update(" << result << ")";

  BATT_CHECK(this->writing_control_block_);

  this->writing_control_block_ = false;

  if (!result.ok()) {
    this->handle_write_error(result.status());
    return;
  }

  if (BATT_CHECKED_CAST(usize, *result) != this->control_block_buffer_.size()) {
    LLFS_LOG_ERROR() << "Failed to write entire log control block!";
    this->context_.update_error_status(batt::StatusCode::kInternal);
    return;
  }

  // We can now notify the outside world that the trim/flush pointers have been updated.
  //
  this->trim_pos_.set_value(this->control_block_->trim_pos);
  this->flush_pos_.set_value(this->control_block_->flush_pos);

  this->poll();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::handle_write_error(Status status) noexcept
{
  this->trim_pos_.close();
  this->flush_pos_.close();
  this->context_.update_error_status(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
template <typename HandlerFn>
auto IoRingLogDriver2<StorageT>::make_write_handler(HandlerFn&& handler)
{
  HandlerMemory* const p_mem = this->alloc_handler_memory();

  return batt::make_custom_alloc_handler(
      *p_mem, [this, p_mem, handler = BATT_FORWARD(handler)](const StatusOr<i32>& result) {
        this->free_handler_memory(p_mem);
        handler(this, result);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::initialize_handler_memory_pool()
{
  LLFS_VLOG(1) << "initialize_handler_memory_pool()";

  BATT_CHECK_EQ(this->handler_memory_pool_, nullptr);

  usize pool_size = this->options_.max_concurrent_writes + 1 /* control block write */ +
                    1 /* target_trim_pos_ wait */ + 1 /* commit_pos_ wait */;

  this->handler_memory_.reset(new HandlerMemoryStorage[pool_size]);

  for (usize i = 0; i < pool_size; ++i) {
    HandlerMemoryStorage* p_storage = std::addressof(this->handler_memory_[i]);
    auto pp_next = (HandlerMemoryStorage**)p_storage;
    *pp_next = this->handler_memory_pool_;
    this->handler_memory_pool_ = p_storage;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
auto IoRingLogDriver2<StorageT>::alloc_handler_memory() noexcept -> HandlerMemory*
{
  BATT_CHECK_EQ(this->event_thread_id_, std::this_thread::get_id());

  HandlerMemoryStorage* const p_storage = this->handler_memory_pool_;
  auto pp_next = (HandlerMemoryStorage**)p_storage;
  this->handler_memory_pool_ = *pp_next;
  *pp_next = nullptr;

  return new (p_storage) HandlerMemory{};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::free_handler_memory(HandlerMemory* p_mem) noexcept
{
  BATT_CHECK(!p_mem->in_use());
  BATT_CHECK_EQ(this->event_thread_id_, std::this_thread::get_id());

  p_mem->~HandlerMemory();

  auto p_storage = (HandlerMemoryStorage*)p_mem;
  auto pp_next = (HandlerMemoryStorage**)p_storage;
  *pp_next = this->handler_memory_pool_;
  this->handler_memory_pool_ = p_storage;
}

}  //namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE2_IPP
