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

#include <batteries/metrics/metric_registry.hpp>

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

  using batt::Token;

  // Register all metrics.
  //
  MetricLabelSet labels{
      MetricLabel{Token{"object_type"}, Token{"llfs_IoRingLogDriver2"}},
      MetricLabel{Token{"log_name"}, Token{this->options_.name}},
  };

  this->metrics_.export_to(global_metric_registry(), labels);

  global_metric_registry()  //
      .add("trim_target_pos", this->observed_watch_[Self::kTargetTrimPos], batt::make_copy(labels))
      .add("commit_pos", this->observed_watch_[Self::kCommitPos], batt::make_copy(labels))
      .add("trim_pos", this->trim_pos_, batt::make_copy(labels))
      .add("flush_pos", this->flush_pos_, batt::make_copy(labels));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline IoRingLogDriver2<StorageT>::~IoRingLogDriver2() noexcept
{
  this->metrics_.unexport_from(global_metric_registry());

  global_metric_registry()  //
      .remove(this->observed_watch_[Self::kTargetTrimPos])
      .remove(this->observed_watch_[Self::kCommitPos])
      .remove(this->trim_pos_)
      .remove(this->flush_pos_);
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

  this->data_begin_ = this->config_.control_block_offset + control_block_size;
  this->data_end_ = this->data_begin_ + p_control_block->data_size;

  this->control_block_buffer_ = ConstBuffer{
      p_control_block,
      batt::round_up_to<usize>(this->device_page_size_, p_control_block->control_header_size),
  };
  this->control_block_ = p_control_block;

  // TODO [tastolfi 2024-06-11] verify control block values against config where possible.

  if (this->control_block_->magic != PackedLogControlBlock2::kMagic) {
    return ::llfs::make_status(::llfs::StatusCode::kLogControlBlockBadMagic);
  }

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
  this->known_flushed_commit_pos_ = new_flush_pos;
  this->flush_pos_.set_value(new_flush_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline void IoRingLogDriver2<StorageT>::poll() noexcept
{
  CommitPos observed_commit_pos = this->observe(CommitPos{});
  TargetTrimPos observed_target_trim_pos = this->observe(TargetTrimPos{});

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
inline void IoRingLogDriver2<StorageT>::start_flush(const CommitPos observed_commit_pos)
{
  // Unflushed data comes after flushed.
  //
  slot_offset_type flush_upper_bound = this->unflushed_lower_bound_;

  // Repeat is for when unflushed data wraps around the end of the ring buffer, back to the
  // beginning; in this case we start two writes.
  //
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
        this->metrics_.flush_write_split_wrap_count.add(1);
      }
    }

    // If burst mode optimization is enabled, then round slot_range.upper_bound *down* to get the
    // aligned upper bound (instead of up, the default); this way, we are less likely to need to
    // issue another I/O to fill in partial data in flush_tail_.
    //
    if (this->flush_tail_ && this->options_.optimize_burst_mode) {
      this->metrics_.burst_mode_checked.add(1);

      const usize new_upper_bound = slot_max(
          slot_range.lower_bound,
          batt::round_down_bits(this->config_.data_alignment_log2, slot_range.upper_bound));

      if (slot_range.upper_bound != new_upper_bound) {
        this->metrics_.burst_mode_applied.add(1);
      }

      slot_range.upper_bound = new_upper_bound;
    }

    // Align to block boundaries for direct I/O
    //
    SlotRange aligned_range = this->get_aligned_range(slot_range);

    // If this flush would overlap with an ongoing one (at the last device page) then trim the
    // aligned_range (on the lower end) so it doesn't.
    //
    if (this->flush_tail_ &&
        slot_less_than(aligned_range.lower_bound, this->flush_tail_->upper_bound)) {
      aligned_range.lower_bound = this->flush_tail_->upper_bound;
      this->metrics_.flush_write_tail_collision_count.add(1);

      if (aligned_range.empty()) {
        flush_upper_bound = this->flush_tail_->upper_bound;
        continue;
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

    // Save this observed commit pos.
    //
    if (this->observed_commit_offsets_.empty() ||
        this->observed_commit_offsets_.back() != observed_commit_pos) {
      // Sanity check: verify that observed commit offsets are in non-decreasing order.
      //
      if (!this->observed_commit_offsets_.empty()) {
        LLFS_CHECK_SLOT_LT(this->observed_commit_offsets_.back(), observed_commit_pos);
      }

      this->observed_commit_offsets_.push_back(observed_commit_pos);
    }

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

  BATT_CHECK_LE(write_offset + (i64)buffer.size(), this->data_end_)
      << "Data to flush extends beyond the end of the storage extent; forgot to handle wrap-around "
         "case?";

  LLFS_VLOG(1) << " -- async_write_some(offset=" << write_offset << ".."
               << write_offset + buffer.size() << ", size=" << buffer.size() << ")";

  ++this->writes_pending_;
  this->metrics_.max_concurrent_writes.clamp_min(this->writes_pending_);

  BATT_CHECK_LE(this->writes_pending_, this->options_.max_concurrent_writes);

  this->metrics_.total_write_count.add(1);
  this->metrics_.flush_write_count.add(1);

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

  // Determine whether this write is the current flush_tail_, and clear it if so.
  //
  const bool is_tail = [&] {
    SlotRange aligned_tail = this->get_aligned_tail(aligned_range);
    return (this->flush_tail_ && *this->flush_tail_ == aligned_tail);
  }();
  LLFS_DVLOG(1) << BATT_INSPECT(is_tail);

  if (is_tail) {
    this->flush_tail_ = None;
  }

  // Write errors are fatal.
  //
  if (!result.ok()) {
    LLFS_VLOG(1) << "(handle_flush_write) error: " << result.status();
    this->handle_write_error(result.status());
    return;
  }

  // Calculate the (non-aligned) offset range of flushed data.
  //
  const usize bytes_written = *result;

  SlotRange flushed_range{
      // Use the logical (non-aligned) lower bound.
      //
      .lower_bound = slot_max(slot_range.lower_bound, aligned_range.lower_bound),

      // Take minimum here to account for short writes.
      //
      .upper_bound = slot_min(aligned_range.lower_bound + bytes_written, slot_range.upper_bound),
  };
  BATT_CHECK(!flushed_range.empty());

  this->metrics_.bytes_written.add(bytes_written);
  this->metrics_.bytes_flushed.add(flushed_range.size());

  LLFS_DVLOG(1) << BATT_INSPECT(flushed_range);

  // Update this->known_flush_pos_ and this->known_flushed_commit_pos_ to reflect the write.
  //
  this->update_known_flush_pos(flushed_range);

  // If there was no write after this one and the write was short, then we must adjust
  // this->unflushed_lower_bound_ down to the actual end of flushed data so that when we call
  // poll(), the remainder of `slot_range` is written.
  //
  if (is_tail) {
    this->unflushed_lower_bound_ = flushed_range.upper_bound;

  } else {
    // If is_tail is false, then there was a write initiated *after* this portion of the log.  In
    // this case, if the write was short, this function needs to initiate writing the remainder of
    // the data.
    //
    const CommitPos observed_commit_pos = this->observe(CommitPos{});

    SlotRange unflushed_remainder{
        .lower_bound = flushed_range.upper_bound,
        .upper_bound = slot_min(aligned_range.upper_bound, observed_commit_pos),
    };

    if (!unflushed_remainder.empty()) {
      this->metrics_.flush_tail_rewrite_count.add(1);
      this->start_flush_write(unflushed_remainder, this->get_aligned_range(unflushed_remainder));
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

  // Advance known_flushed_commit_pos_ by consuming values from this->observed_commit_offsets_.
  //
  while (!this->observed_commit_offsets_.empty()) {
    const CommitPos previously_observed_commit_pos = this->observed_commit_offsets_.front();

    // If the next observed commit pos is not yet flushed, we are done.
    //
    if (slot_less_than(this->known_flush_pos_, previously_observed_commit_pos)) {
      break;
    }

    // Sanity check: verify that observed commit offsets are in non-decreasing order.
    //
    LLFS_CHECK_SLOT_LE(this->known_flushed_commit_pos_, previously_observed_commit_pos);

    this->known_flushed_commit_pos_ = previously_observed_commit_pos;
    this->observed_commit_offsets_.pop_front();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::start_control_block_update(
    TargetTrimPos observed_target_trim_pos) noexcept
{
  if (this->writing_control_block_ || this->trim_pos_.is_closed() || this->flush_pos_.is_closed()) {
    return;
  }

  BATT_CHECK_NOT_NULLPTR(this->control_block_) << "Forgot to call read_control_block()?";

  const slot_offset_type effective_target_flush_pos = this->known_flushed_commit_pos_;
  const slot_offset_type effective_target_trim_pos =
      slot_min(effective_target_flush_pos, observed_target_trim_pos);

  const slot_offset_type observed_trim_pos = this->trim_pos_.get_value();
  const slot_offset_type observed_flush_pos = this->flush_pos_.get_value();

  if (observed_trim_pos == effective_target_trim_pos &&
      observed_flush_pos == effective_target_flush_pos) {
    return;
  }

  LLFS_VLOG(1) << "start_control_block_update():"
               << " trim=" << observed_trim_pos << "->" << observed_target_trim_pos
               << " (effective=" << effective_target_trim_pos << ")"
               << " flush=" << observed_flush_pos << "->" << effective_target_flush_pos;

  BATT_CHECK_EQ(observed_trim_pos, this->control_block_->trim_pos);
  BATT_CHECK_EQ(observed_flush_pos, this->control_block_->flush_pos);
  LLFS_CHECK_SLOT_LE(effective_target_trim_pos, effective_target_flush_pos);
  BATT_CHECK_LE(effective_target_flush_pos - effective_target_trim_pos, this->config_.log_capacity);

  this->control_block_->trim_pos = effective_target_trim_pos;
  this->control_block_->flush_pos = effective_target_flush_pos;
  this->control_block_->generation = this->control_block_->generation + 1;

  this->writing_control_block_ = true;

  this->metrics_.total_write_count.add(1);
  this->metrics_.control_block_write_count.add(1);

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

  const usize bytes_written = BATT_CHECKED_CAST(usize, *result);

  this->metrics_.bytes_written.add(bytes_written);

  if (bytes_written != this->control_block_buffer_.size()) {
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
