//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_LOG_BLOCK_CALCULATOR_HPP
#define LLFS_LOG_BLOCK_CALCULATOR_HPP

#include <llfs/int_types.hpp>
#include <llfs/ioring_log_config.hpp>
#include <llfs/ioring_log_driver_options.hpp>
#include <llfs/log_device.hpp>
#include <llfs/slot.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/strong_typedef.hpp>

namespace llfs {

// Calculates derived values from IoRing log config and options.
//
class LogBlockCalculator
{
 public:
  BATT_STRONG_TYPEDEF(usize, LogicalBlockIndex);
  BATT_STRONG_TYPEDEF(usize, PhysicalBlockIndex);
  BATT_STRONG_TYPEDEF(i64, PhysicalFileOffset);
  BATT_STRONG_TYPEDEF(usize, FlushOpIndex);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static usize disk_size_required_for_log_size(u64 logical_size, u64 block_size)
  {
    const u64 block_capacity = block_size - sizeof(PackedLogPageHeader);
    const u64 block_count =
        (logical_size + block_capacity - 1) / block_capacity + (1 /* for wrap-around */);

    return block_size * block_count;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit LogBlockCalculator(const IoRingLogConfig& config,
                              const IoRingLogDriverOptions& options) noexcept
      : config_{config}
      , options_{options}
  {
  }

  usize pages_per_block() const
  {
    return this->config_.pages_per_block();
  }

  // Returns the size in bytes of a log block.
  //
  usize block_size() const
  {
    return this->block_size_;
  }

  // Returns the size in bytes of the payload section of a log block; this is the block size minus
  // the log page header.
  //
  usize block_capacity() const
  {
    return this->block_capacity_;
  }

  // Returns the total number of blocks in the log.g
  //
  usize block_count() const
  {
    return this->block_count_;
  }

  // Returns the size (capacity) in bytes of the log.
  //
  u64 logical_size() const
  {
    return this->config_.logical_size;
  }

  // Returns the size in bytes of the on-disk image of the log.
  //
  u64 physical_size() const
  {
    return this->log_end_ - this->log_start_;
  }

  // Returns the number of concurrent flush operations configured for this log.
  //
  usize queue_depth() const
  {
    return this->queue_depth_;
  }

  // The starting offset within the file of the beginning of the log blocks.
  //
  i64 begin_file_offset() const
  {
    return this->log_start_;
  }

  // The file offset that is one past the last byte of the last log block.
  //
  i64 end_file_offset() const
  {
    return this->log_end_;
  }

  // Returns the index of the logical block that contains the given slot offset.
  //
  LogicalBlockIndex logical_block_index_from(SlotLowerBoundAt slot_lower_bound) const
  {
    return LogicalBlockIndex{slot_lower_bound.offset / this->block_capacity()};
  }

  // Returns the index of the highest-offset logical block that is bounded by `slot_upper_bound`.
  //
  LogicalBlockIndex logical_block_index_from(SlotUpperBoundAt slot_upper_bound) const
  {
    return this->logical_block_index_from(SlotLowerBoundAt{
        .offset = slot_upper_bound.offset - 1,
    });
  }

  PhysicalBlockIndex physical_block_index_from(LogicalBlockIndex logical_block_index) const
  {
    return PhysicalBlockIndex{logical_block_index.value() % this->block_count()};
  }

  // Returns the index of the physical block that contains the given slot offset.
  //
  PhysicalBlockIndex physical_block_index_from(SlotLowerBoundAt slot_lower_bound) const
  {
    return this->physical_block_index_from(this->logical_block_index_from(slot_lower_bound));
  }

  // Returns the index of the highest-offset physical block that is bounded by `slot_upper_bound`.
  //
  PhysicalBlockIndex physical_block_index_from(SlotUpperBoundAt slot_upper_bound) const
  {
    return this->physical_block_index_from(this->logical_block_index_from(slot_upper_bound));
  }

  // Returns the index of the physical block containing the passed file offset.
  //
  PhysicalBlockIndex physical_block_index_from(PhysicalFileOffset file_offset) const
  {
    BATT_CHECK_GE(file_offset, this->log_start_.value());
    BATT_CHECK_LT(file_offset, this->log_end_.value());

    return PhysicalBlockIndex{(file_offset - this->log_start_.value()) / this->block_size()};
  }

  // Returns the SlotRange of the specified block.
  //
  SlotRange block_slot_range_from(LogicalBlockIndex logical_block_index) const
  {
    const slot_offset_type lower_bound = logical_block_index.value() * this->block_capacity();

    return SlotRange{
        .lower_bound = lower_bound,
        .upper_bound = lower_bound + this->block_capacity(),
    };
  }

  // Returns the SlotRange of the highest-offset block that contains `slot_lower_bound`.
  //
  SlotRange block_slot_range_from(SlotLowerBoundAt slot_lower_bound) const
  {
    return this->block_slot_range_from(this->logical_block_index_from(slot_lower_bound));
  }

  // Returns the SlotRange of the highest-offset block that is bounded by `slot_upper_bound`.
  //
  SlotRange block_slot_range_from(SlotUpperBoundAt slot_upper_bound) const
  {
    return this->block_slot_range_from(this->logical_block_index_from(slot_upper_bound));
  }

  FlushOpIndex flush_op_index_from(LogicalBlockIndex logical_block_index) const
  {
    return FlushOpIndex{logical_block_index.value() & this->queue_depth_mask_};
  }

  // Returns the index of the flush op object assigned to handle flushing from the given log offset.
  //
  FlushOpIndex flush_op_index_from(SlotLowerBoundAt slot_lower_bound) const
  {
    return this->flush_op_index_from(this->logical_block_index_from(slot_lower_bound));
  }

  // Returns the index of the flush op object assigned to handle flushing from the given log offset.
  //
  FlushOpIndex flush_op_index_from(SlotUpperBoundAt slot_upper_bound) const
  {
    return this->flush_op_index_from(this->logical_block_index_from(slot_upper_bound));
  }

  // Returns the next flush_op index, wrapping around at `queue_depth`.
  //
  FlushOpIndex next_flush_op_index(FlushOpIndex index) const
  {
    return FlushOpIndex{(index.value() + 1) & this->queue_depth_mask_};
  }

  // Returns the physical file offset of the specified physical block.
  //
  PhysicalFileOffset block_start_file_offset_from(PhysicalBlockIndex physical_block_index) const
  {
    return PhysicalFileOffset{BATT_CHECKED_CAST(
        i64, this->log_start_.value() + physical_block_index.value() * this->block_size())};
  }

  // Returns the physical file offset of the specified logical block.
  //
  PhysicalFileOffset block_start_file_offset_from(LogicalBlockIndex logical_block_index) const
  {
    return this->block_start_file_offset_from(this->physical_block_index_from(logical_block_index));
  }

  // Returns the physical file offset of the block that contains the given logical slot offset.
  //
  PhysicalFileOffset block_start_file_offset_from(SlotLowerBoundAt slot_lower_bound) const
  {
    return this->block_start_file_offset_from(this->physical_block_index_from(slot_lower_bound));
  }

  // Returns the physical file offset of the highest index block that is bounded by the given slot
  // offset.
  //
  PhysicalFileOffset block_start_file_offset_from(SlotUpperBoundAt slot_upper_bound) const
  {
    return this->block_start_file_offset_from(this->physical_block_index_from(slot_upper_bound));
  }

  PhysicalFileOffset block_start_file_offset_from(PhysicalFileOffset file_offset) const
  {
    return block_start_file_offset_from(this->physical_block_index_from(file_offset));
  }

 private:
  IoRingLogConfig config_;
  PhysicalFileOffset log_start_{this->config_.physical_offset};
  PhysicalFileOffset log_end_{this->log_start_ +
                              BATT_CHECKED_CAST(i64, this->config_.physical_size)};
  usize block_size_ = this->config_.block_size();
  usize block_capacity_ = this->config_.block_capacity();
  usize block_count_ = this->config_.block_count();

  // Runtime options for the log driver plus cached derived values.
  //
  IoRingLogDriverOptions options_;
  usize queue_depth_ = this->options_.queue_depth();
  usize queue_depth_mask_ = this->options_.queue_depth_mask();
};

}  // namespace llfs

#endif  // LLFS_LOG_BLOCK_CALCULATOR_HPP
