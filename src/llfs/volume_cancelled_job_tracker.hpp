//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_CANCELLED_JOB_TRACKER_HPP
#define LLFS_VOLUME_CANCELLED_JOB_TRACKER_HPP

#include <llfs/config.hpp>
//
#include <llfs/slot.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/shared_ptr.hpp>

#include <set>

namespace llfs {

class VolumeCancelledJobTracker : public batt::RefCounted<VolumeCancelledJobTracker>
{
 public:
  VolumeCancelledJobTracker() = default;

  VolumeCancelledJobTracker(const VolumeCancelledJobTracker&) = delete;
  VolumeCancelledJobTracker& operator=(const VolumeCancelledJobTracker&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void set_cancelled(slot_offset_type prepare_slot_offset);

  bool is_cancelled(slot_offset_type prepare_slot_offset);

  void trim(slot_offset_type new_trim_pos);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  using State = std::set<slot_offset_type>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::Mutex<State> state_;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_CANCELLED_JOB_TRACKER_HPP
