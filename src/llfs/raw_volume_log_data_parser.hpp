//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_RAW_VOLUME_LOG_DATA_PARSER_HPP
#define LLFS_RAW_VOLUME_LOG_DATA_PARSER_HPP

#include <llfs/config.hpp>
//
#include <llfs/buffer.hpp>
#include <llfs/optional.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_parse.hpp>
#include <llfs/status.hpp>
#include <llfs/volume_pending_jobs_map.hpp>

namespace llfs {

class RawVolumeLogDataParser
{
 public:
  RawVolumeLogDataParser() = default;

  /** \brief For parsing raw log data, like that returned by Volume::get_root_log_data.
   *
   * \return the slot upper bound of the last slot parsed.
   */
  template <typename SlotVisitorFn = batt::Status(const SlotParse& slot,
                                                  const std::string_view& user_data)>
  StatusOr<slot_offset_type> parse_chunk(const SlotRange& slot_range, const ConstBuffer& buffer,
                                         SlotVisitorFn&& slot_visitor_fn);

 private:
  Optional<slot_offset_type> user_slot_upper_bound_;

  Optional<SlotRange> current_chunk_slot_range_;

  VolumePendingJobsMap pending_jobs_;

  Optional<slot_offset_type> visited_upper_bound_;
};

}  //namespace llfs

#endif  // LLFS_RAW_VOLUME_LOG_DATA_PARSER_HPP

#include <llfs/raw_volume_log_data_parser.ipp>
