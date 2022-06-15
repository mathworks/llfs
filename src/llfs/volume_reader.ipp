//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_READER_IPP
#define LLFS_VOLUME_READER_IPP

#include <llfs/page_cache_job.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/volume.hpp>
#include <llfs/volume_events.hpp>
#include <llfs/volume_slot_demuxer.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Implementation of VolumeReader.
//
class VolumeReader::Impl
{
 public:
  // Construct a VolumeReader::Impl for the given Volume with the specified slot range locked.
  //
  explicit Impl(Volume& volume, SlotReadLock&& read_lock, LogReadMode mode) noexcept;

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Volume& volume_;
  SlotReadLock read_lock_;
  std::unique_ptr<PageCacheJob> job_;
  std::unique_ptr<LogDevice::Reader> log_reader_;
  TypedSlotReader<VolumeEventVariant> slot_reader_;
  bool paused_;
  VolumePendingJobsMap pending_jobs_;
  slot_offset_type trim_lock_update_lower_bound_;
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename F>
inline StatusOr<usize> VolumeReader::visit_next(batt::WaitForResource wait_for_commit,
                                                F&& visitor_fn)
{
  usize n_user_slots_visited = 0;

  // Unpause for one slot; the pre_slot_fn we installed in the Reader ctor will reset paused to true
  // after the visitor function is invoked once.
  //
  this->impl_->paused_ = false;
  auto on_scope_exit = batt::finally([&] {
    this->impl_->paused_ = true;  // `paused_` may already be set to true by this point.
  });

  auto wrapped_visitor_fn = [this, &visitor_fn, &n_user_slots_visited](
                                const SlotParse& slot, const std::string_view& user_data) {
    this->impl_->paused_ = true;
    n_user_slots_visited += 1;
    return visitor_fn(slot, user_data);
  };

  VolumeSlotDemuxer<NoneType, decltype(wrapped_visitor_fn)&> demuxer{wrapped_visitor_fn,
                                                                     this->impl_->pending_jobs_};

  StatusOr<usize> visited =
      this->impl_->slot_reader_.run(wait_for_commit, [&demuxer](auto&&... args) -> Status {
        return demuxer(BATT_FORWARD(args)...).status();
      });

  BATT_REQUIRE_OK(visited);

  return n_user_slots_visited;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename F>
inline StatusOr<usize> VolumeReader::consume_slots(batt::WaitForResource wait_for_commit,
                                                   F&& visitor_fn)
{
  usize n_user_slots_visited = 0;

  // Unpause the reader just while we are consuming slots.
  //
  this->impl_->paused_ = false;
  auto on_scope_exit = batt::finally([&] {
    this->impl_->paused_ = true;
  });

  auto wrapped_visitor_fn = [&visitor_fn, &n_user_slots_visited](auto&&... args) -> decltype(auto) {
    n_user_slots_visited += 1;
    return visitor_fn(BATT_FORWARD(args)...);
  };

  VolumeSlotDemuxer<NoneType, decltype(wrapped_visitor_fn)&> demuxer{wrapped_visitor_fn,
                                                                     this->impl_->pending_jobs_};

  // SlotReader::run will block for as long as it can unpack and visit new slots from the log.
  //
  StatusOr<usize> result = this->impl_->slot_reader_.run(
      wait_for_commit,
      [this, &demuxer, &n_user_slots_visited](const SlotParse& slot, auto&& payload) -> Status {
        // Check to see whether the trim lock for this reader can be updated yet.
        //
        if (BATT_HINT_FALSE(!slot_less_than(slot.offset.lower_bound,
                                            this->impl_->trim_lock_update_lower_bound_))) {
          // Advance the lock update lower bound to delay the next update.
          //
          this->impl_->trim_lock_update_lower_bound_ =
              slot.offset.upper_bound + this->volume_options().trim_lock_update_interval;

          // The demuxer must be consulted because pending jobs can affect when it is safe to update
          // the trim lock.
          //
          Optional<slot_offset_type> new_trim_pos = demuxer.get_safe_trim_pos();
          if (new_trim_pos) {
            Status trim_status = this->trim(*new_trim_pos);
            BATT_REQUIRE_OK(trim_status);
          }
        }

        // Visit the slot!  (This is safe to do after trimming because the demuxer tracks the
        // greatest slot offset it has processed.)
        //
        return demuxer(slot, BATT_FORWARD(payload)).status();
      });

  BATT_REQUIRE_OK(result);

  return n_user_slots_visited;
}

}  // namespace llfs

#endif  // LLFS_VOLUME_READER_IPP
