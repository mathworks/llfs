//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_IPP
#define LLFS_VOLUME_IPP

#include <llfs/buffered_log_data_reader.hpp>
#include <llfs/volume_slot_demuxer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline StatusOr<TypedVolumeReader<T>> Volume::typed_reader(const SlotRangeSpec& slot_range,
                                                           LogReadMode mode, batt::StaticType<T>)
{
  StatusOr<VolumeReader> reader = this->reader(slot_range, mode);
  BATT_REQUIRE_OK(reader);

  return TypedVolumeReader<T>{std::move(*reader)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SlotVisitorFn>
batt::StatusOr<slot_offset_type> parse_raw_volume_log_data(const SlotReadLock& read_lock,
                                                           const ConstBuffer& buffer,
                                                           SlotVisitorFn&& slot_visitor_fn)
{
  const SlotRange locked_range = read_lock.slot_range();
  slot_offset_type parsed_upper_bound = locked_range.lower_bound;

  auto wrapped_visitor_fn = [&slot_visitor_fn, &parsed_upper_bound, &locked_range](
                                const SlotParse& slot,
                                const std::string_view& user_data) -> batt::Status {
    if (slot_greater_than(slot.offset.upper_bound, locked_range.upper_bound)) {
      return ::llfs::make_status(::llfs::StatusCode::kBreakSlotReaderLoop);
    }
    BATT_REQUIRE_OK(slot_visitor_fn(slot, user_data));
    parsed_upper_bound = slot_max(parsed_upper_bound, slot.offset.upper_bound);
    return batt::OkStatus();
  };

  VolumePendingJobsMap pending_jobs;

  VolumeSlotDemuxer<NoneType, decltype(wrapped_visitor_fn)> demuxer{wrapped_visitor_fn,
                                                                    pending_jobs};

  BufferedLogDataReader log_data_reader{buffer};

  TypedSlotReader<VolumeEventVariant> slot_reader{log_data_reader};

  BATT_REQUIRE_OK(slot_reader.run(batt::WaitForResource::kFalse, demuxer));

  return parsed_upper_bound;
}

}  // namespace llfs

#endif  // LLFS_VOLUME_IPP
