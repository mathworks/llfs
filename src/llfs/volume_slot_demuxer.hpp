//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_SLOT_DEMUXER_HPP
#define LLFS_VOLUME_SLOT_DEMUXER_HPP

#include <llfs/slot.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/volume_event_visitor.hpp>
#include <llfs/volume_pending_jobs_map.hpp>
#include <llfs/volume_reader.hpp>

namespace llfs {

using VolumePendingJobsMap =
    std::map<slot_offset_type, SlotParseWithPayload<Ref<const PackedPrepareJob>>, SlotLess>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Demultiplexer for Volume root logs; splits a single stream of Volume events into Volume events
// plus user slots.
//
// Only known committed jobs are passed on to the user-level slot visitor.
//
template <typename R, typename Fn = VolumeReader::SlotVisitorFn>
class VolumeSlotDemuxer : public VolumeEventVisitor<StatusOr<R>>
{
 public:
  template <typename FnArg>
  explicit VolumeSlotDemuxer(
      FnArg&& slot_visitor_fn, VolumePendingJobsMap& pending_jobs,
      VolumeEventVisitor<R>& base = VolumeEventVisitor<R>::null_impl()) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void reset_pending_jobs()
  {
    this->pending_jobs_.clear();
  }

  const VolumePendingJobsMap& get_pending_jobs() const noexcept
  {
    return this->pending_jobs_;
  }

  // Tracks the pending jobs to make sure that no prepare slots are trimmed before the commit slot.
  //
  Optional<slot_offset_type> get_safe_trim_pos() const;

  // Returns the upper bound offset of the last visited slot.
  //
  Optional<slot_offset_type> get_visited_upper_bound() const noexcept
  {
    return this->visited_upper_bound_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<R> on_raw_data(const SlotParse&, const Ref<const PackedRawData>&) override;

  //----- --- -- -  -  -   -

  StatusOr<R> on_prepare_job(const SlotParse&, const Ref<const PackedPrepareJob>&) override;

  StatusOr<R> on_commit_job(const SlotParse&, const PackedCommitJob&) override;

  StatusOr<R> on_rollback_job(const SlotParse&, const PackedRollbackJob&) override;

  //----- --- -- -  -  -   -

  StatusOr<R> on_volume_attach(const SlotParse&, const PackedVolumeAttachEvent&) override;

  StatusOr<R> on_volume_detach(const SlotParse&, const PackedVolumeDetachEvent&) override;

  StatusOr<R> on_volume_ids(const SlotParse&, const PackedVolumeIds&) override;

  StatusOr<R> on_volume_recovered(const SlotParse&, const PackedVolumeRecovered&) override;

  StatusOr<R> on_volume_format_upgrade(const SlotParse&, const PackedVolumeFormatUpgrade&) override;

  StatusOr<R> on_volume_trim(const SlotParse&, const VolumeTrimEvent&) override;

 private:
  // Updates internal state to reflect having visited the given slot.
  //
  void mark_slot_visited(const SlotParse&);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The function to call on each user-visible slot.
  //
  Fn visitor_fn_;

  // The next event visitor in the chain.
  //
  VolumeEventVisitor<R>& base_;

  // The pending jobs for this volume.
  //
  VolumePendingJobsMap& pending_jobs_;

  // The upper bound offset of the last visited slot.
  //
  Optional<slot_offset_type> visited_upper_bound_;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_SLOT_DEMUXER_HPP

#include <llfs/volume_slot_demuxer.ipp>
