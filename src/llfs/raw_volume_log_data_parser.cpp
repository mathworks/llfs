//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/raw_volume_log_data_parser.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<slot_offset_type> RawVolumeLogDataParser::get_visited_upper_bound() const noexcept
{
  if (this->visited_upper_bound_) {
    return this->visited_upper_bound_;
  } else {
    return this->user_slot_upper_bound_;
  }
}

}  //namespace llfs
