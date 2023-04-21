//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_RAW_VOLUME_LOG_DATA_PARSER_IPP
#define LLFS_RAW_VOLUME_LOG_DATA_PARSER_IPP

#include <llfs/raw_volume_log_data_parser.hpp>
//
#include <llfs/buffered_log_data_reader.hpp>
#include <llfs/volume_slot_demuxer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SlotVisitorFn /*= Status(SlotParse, std::string_view)*/>
inline StatusOr<slot_offset_type> RawVolumeLogDataParser::parse_chunk(
    const SlotRange& slot_range, const ConstBuffer& buffer, SlotVisitorFn&& slot_visitor_fn)
{
  // Update the user slot upper bound.
  //
  this->user_slot_upper_bound_ = slot_range.lower_bound;

  // Set the current chunk slot range while we are in this function.
  //
  this->current_chunk_slot_range_ = slot_range;
  auto on_scope_exit = batt::finally([&] {
    this->current_chunk_slot_range_ = None;
  });

  // Create a lambda wrapper to visit slots.
  //
  auto wrapped_visitor_fn = [&slot_visitor_fn, this](
                                const SlotParse& slot,
                                const std::string_view& user_data) -> batt::Status {
    if (slot_greater_than(slot.offset.upper_bound, this->current_chunk_slot_range_->upper_bound)) {
      return ::llfs::make_status(::llfs::StatusCode::kBreakSlotReaderLoop);
    }
    BATT_REQUIRE_OK(slot_visitor_fn(slot, user_data));
    clamp_min_slot(&this->user_slot_upper_bound_, slot.offset.upper_bound);
    return batt::OkStatus();
  };

  VolumeSlotDemuxer<NoneType, decltype(wrapped_visitor_fn)> demuxer{wrapped_visitor_fn,
                                                                    this->pending_jobs_};

  BufferedLogDataReader log_data_reader{slot_range.lower_bound, buffer};

  TypedSlotReader<VolumeEventVariant> slot_reader{log_data_reader};

  BATT_REQUIRE_OK(slot_reader.run(batt::WaitForResource::kFalse, demuxer));

  this->visited_upper_bound_ = demuxer.get_visited_upper_bound();

  return this->visited_upper_bound_.value_or(*this->user_slot_upper_bound_);
}

}  //namespace llfs

#endif  // LLFS_RAW_VOLUME_LOG_DATA_PARSER_IPP
