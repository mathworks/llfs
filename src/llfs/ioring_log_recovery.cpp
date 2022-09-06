//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_recovery.hpp>
//
#include <llfs/data_reader.hpp>
#include <llfs/logging.hpp>

#include <batteries/stream_util.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingLogRecovery::IoRingLogRecovery(const IoRingLogConfig& config,
                                                  RingBuffer& ring_buffer, ReadDataFn&& read_data)
    : config_{config}
    , ring_buffer_{ring_buffer}
    , read_data_{std::move(read_data)}
    , block_storage_{
          new PackedLogPageBuffer[this->config_.block_size() / sizeof(PackedLogPageBuffer)]}
{
  BATT_CHECK_EQ(this->config_.block_size() % sizeof(PackedLogPageBuffer), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingLogRecovery::run()
{
  LLFS_VLOG(1) << "IoRingLogRecovery::run()";
  i64 file_offset = 0;
  Optional<slot_offset_type> old_trim_pos = this->trim_pos_;

  for (usize block_i = 0; block_i < this->config_.block_count();
       ++block_i, file_offset += this->config_.block_size()) {
    //----- --- -- -  -  -   -
    LLFS_VLOG(2) << "Reading " << BATT_INSPECT(block_i) << " from " << BATT_INSPECT(file_offset);

    // Read the next block into the buffer.
    //
    Status read_status = this->read_data_(file_offset, this->block_buffer());
    BATT_REQUIRE_OK(read_status);

    // Run data integrity checks.
    //
    Status block_valid = this->validate_block();
    BATT_REQUIRE_OK(block_valid);

    // Update the trim position (this must be valid later when we find the "true" flush_pos).
    //
    clamp_min_slot(&this->trim_pos_, this->block_header().trim_pos);
    if (this->trim_pos_ != old_trim_pos) {
      LLFS_VLOG(1) << "Updating trim_pos: " << old_trim_pos << " => " << this->trim_pos_;
      old_trim_pos = this->trim_pos_;
    } else {
      LLFS_VLOG(2) << BATT_INSPECT(this->trim_pos_);
    }

    // Copy data from this block into the ring buffer, if possible.
    //
    this->recover_block_data();
  }

  // Scan recovered data to find the true flushed upper bound.
  //
  this->recover_flush_pos();

  // Done!
  //
  LLFS_VLOG(1) << "Finished log recovery;" << BATT_INSPECT(this->trim_pos_)
               << BATT_INSPECT(this->flush_pos_)
               << " logical_size=" << batt::dump_size_exact(this->config_.logical_size)
               << " ring_buffer.size=" << batt::dump_size_exact(this->ring_buffer_.size());

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingLogRecovery::validate_block() const
{
  if (this->block_header().magic != PackedLogPageHeader::kMagic) {
    // TODO [tastolfi 2022-02-09] specific error message/code
    return {batt::StatusCode::kDataLoss};
  }
  if (this->block_header().commit_size > this->config_.block_capacity()) {
    // TODO [tastolfi 2022-02-09] specific error message/code
    return {batt::StatusCode::kDataLoss};
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer IoRingLogRecovery::block_buffer()
{
  return MutableBuffer{(void*)this->block_storage_.get(), this->config_.block_size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedLogPageHeader& IoRingLogRecovery::block_header() const
{
  return this->block_storage_[0].header;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogRecovery::recover_block_data()
{
  const auto& header = this->block_header();

  LLFS_VLOG(1) << "IoRingLogRecovery::recover_block_data()" << BATT_INSPECT(header.slot_offset)
               << BATT_INSPECT(header.commit_size);

  // Record this slot interval so we know how much contiguous data we have starting at the trim pos
  // upper bound when we are done.
  //
  const auto slot_offset_range = OffsetRange{
      .lower_bound = (isize)header.slot_offset,
      .upper_bound = (isize)(header.slot_offset + header.commit_size),
  };
  LLFS_VLOG(2) << " -- " << BATT_INSPECT(slot_offset_range);

  this->committed_data_.update(slot_offset_range, 1);
  LLFS_VLOG(2) << " -- " << BATT_INSPECT(this->committed_data_);

  // If there is no committed data in this block, we are done here.
  //
  if (header.commit_size == 0) {
    LLFS_VLOG(2) << " -- (no data; returning)";
    return;
  }

  LLFS_VLOG(1) << "Found committed data;" << BATT_INSPECT(slot_offset_range);

  const u64 begin_offset = header.slot_offset;
  const u64 end_offset = begin_offset + header.commit_size;

  const batt::Interval<isize> logical_slots{
      .lower_bound = BATT_CHECKED_CAST(isize, begin_offset),
      .upper_bound = BATT_CHECKED_CAST(isize, end_offset),
  };

  ConstBuffer data = this->block_payload();

  // It may seem cumbersome to explicitly account for buffer wrap-around here, potentially
  // splitting our logical data slice into multiple parts, but handling that here helps prevent
  // SlotIntervalMap from getting too complicated.
  //
  for (const batt::Interval<isize> slice :
       this->ring_buffer_.physical_offsets_from_logical(logical_slots)) {
    //
    // Update the map and then read back our physical interval to see which parts contain
    // up-to-date data that should be copied to the ring buffer.
    //
    this->latest_slot_range_.update(slice, header.slot_offset);

    LLFS_VLOG(1) << " -- ring buffer " << BATT_INSPECT(slice);

    for (const SlotIntervalMap::Entry& entry : this->latest_slot_range_.query(slice)) {
      LLFS_VLOG(1) << " ---- " << BATT_INSPECT(entry);
      if (entry.slot == header.slot_offset) {
        MutableBuffer dst = this->ring_buffer_.get_mut(entry.offset_range.lower_bound);
        auto src_i = entry.offset_range.shift_down(slice.lower_bound);
        ConstBuffer src = slice_buffer(data, src_i);
        LLFS_VLOG(1) << " ---- " << BATT_INSPECT(src_i) << BATT_INSPECT(src.size());
        std::memcpy(dst.data(), src.data(), src.size());
      } else {
        LLFS_VLOG(1) << " ---- skipping entry;" << BATT_INSPECT(entry.slot)
                     << BATT_INSPECT(header.slot_offset);
      }
    }

    data += slice.size();
  }

  LLFS_VLOG(1) << "Updated " << BATT_INSPECT(this->committed_data_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogRecovery::recover_flush_pos()
{
  LLFS_VLOG(1) << "IoRingLogRecovery::recover_flush_pos()";

  // Start at the trim pos and step forward through the recovered data, slot by slot, to discover
  // the true flushed upper bound.

  slot_offset_type slot_offset = this->get_trim_pos();

  // Start at the recovered trim_pos and query up to a full buffer's worth of commit ranges.
  //
  const auto query_range = OffsetRange{
      .lower_bound = static_cast<isize>(slot_offset),
      .upper_bound = static_cast<isize>(slot_offset + this->ring_buffer_.size()),
  };

  // The first entry of the returned container represents the largest contiguous interval of
  // committed data slot offsets recovered from all log blocks.
  //
  const auto committed_ranges = this->committed_data_.query(query_range);

  LLFS_VLOG(1) << " -- " << BATT_INSPECT_RANGE(committed_ranges);

  // If there is no committed data at the recovered trim pos, we are done here.
  //
  if (committed_ranges.empty()) {
    this->flush_pos_ = this->trim_pos_;
    return;
  }

  BATT_CHECK_EQ(committed_ranges.front().offset_range.lower_bound, static_cast<isize>(slot_offset));

  if (committed_ranges.size() > 1) {
    BATT_CHECK(slot_less_than(committed_ranges[0].offset_range.upper_bound,
                              committed_ranges[1].offset_range.lower_bound))
        << "SlotIntervalMap should have merged these two intervals!";
  }

  constexpr usize kUpdateCadence = 500;

  ConstBuffer committed_bytes = resize_buffer(this->ring_buffer_.get(slot_offset),
                                              committed_ranges.front().offset_range.size());
  for (;;) {
    LLFS_VLOG_EVERY_N(2, kUpdateCadence)
        << " -- attempting to recover log entry at " << BATT_INSPECT(slot_offset);

    DataReader reader{committed_bytes};
    const usize bytes_available_before = reader.bytes_available();
    Optional<u64> slot_body_size = reader.read_varint();

    LLFS_VLOG_EVERY_N(2, kUpdateCadence) << " -- " << BATT_INSPECT(slot_body_size);

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

    LLFS_VLOG_EVERY_N(2, kUpdateCadence)
        << " -- " << BATT_INSPECT(slot_size) << BATT_INSPECT(committed_bytes.size());

    if (slot_size > committed_bytes.size()) {
      // Partially committed slot; break out of the loop without updating slot_offset (we're
      // done!)
      //
      LLFS_VLOG(1) << " -- Incomplete slot body, exiting loop;" << BATT_INSPECT(slot_offset)
                   << BATT_INSPECT(bytes_available_before) << BATT_INSPECT(bytes_available_after)
                   << BATT_INSPECT(slot_header_size) << BATT_INSPECT(slot_body_size)
                   << BATT_INSPECT(slot_size);
      break;
    }
    committed_bytes += slot_size;
    slot_offset += slot_size;
  }

  LLFS_VLOG(1) << " -- Slot scan complete;" << BATT_INSPECT(slot_offset);

  this->flush_pos_ = slot_offset;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer IoRingLogRecovery::block_payload() const
{
  return ConstBuffer{this->block_storage_.get(),
                     sizeof(PackedLogPageHeader) + this->block_header().commit_size} +
         sizeof(PackedLogPageHeader);
}

}  // namespace llfs
