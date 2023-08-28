//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_TRIMMER_RECOVERY_VISITOR_HPP
#define LLFS_VOLUME_TRIMMER_RECOVERY_VISITOR_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/ref.hpp>
#include <llfs/slot_parse.hpp>
#include <llfs/status.hpp>
#include <llfs/volume_event_visitor.hpp>
#include <llfs/volume_events.hpp>
#include <llfs/volume_trimmer.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Reconstructs trimmer state during crash recovery.
 */
class VolumeTrimmerRecoveryVisitor : public VolumeEventVisitor<Status>
{
 public:
  explicit VolumeTrimmerRecoveryVisitor(slot_offset_type trim_pos) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // VolumeEventVisitor methods.
  //
  Status on_raw_data(const SlotParse&, const Ref<const PackedRawData>&) override;

  Status on_prepare_job(const SlotParse&, const Ref<const PackedPrepareJob>&) override;

  Status on_commit_job(const SlotParse&, const Ref<const PackedCommitJob>&) override;

  Status on_rollback_job(const SlotParse&, const PackedRollbackJob&) override;

  Status on_volume_attach(const SlotParse& slot, const PackedVolumeAttachEvent& attach) override;

  Status on_volume_detach(const SlotParse& slot, const PackedVolumeDetachEvent& detach) override;

  Status on_volume_ids(const SlotParse& slot, const PackedVolumeIds&) override;

  Status on_volume_recovered(const SlotParse&, const PackedVolumeRecovered&) override;

  Status on_volume_format_upgrade(const SlotParse&, const PackedVolumeFormatUpgrade&) override;

  Status on_volume_trim(const SlotParse&, const VolumeTrimEvent&) override;
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns information about the most recent trim event slot.
   */
  const Optional<VolumeTrimEventInfo>& get_trim_event_info() const noexcept
  {
    return this->trim_event_info_;
  }

 private:
  slot_offset_type log_trim_pos_;
  Optional<VolumeTrimEventInfo> trim_event_info_;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_TRIMMER_RECOVERY_VISITOR_HPP
