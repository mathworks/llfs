//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_cancelled_job_tracker.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeCancelledJobTracker::set_cancelled(slot_offset_type prepare_slot_offset)
{
  auto locked = this->state_.lock();

  locked->emplace(prepare_slot_offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool VolumeCancelledJobTracker::is_cancelled(slot_offset_type prepare_slot_offset)
{
  auto locked = this->state_.lock();

  return locked->count(prepare_slot_offset) != 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeCancelledJobTracker::trim(slot_offset_type new_trim_pos)
{
  auto locked = this->state_.lock();

  auto trimmed_end =
      std::find_if(locked->begin(), locked->end(), [new_trim_pos](slot_offset_type job_slot) {
        return !slot_less_than(job_slot, new_trim_pos);
      });

  locked->erase(locked->begin(), trimmed_end);
}

}  //namespace llfs
