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

  Status on_commit_job(const SlotParse&, const PackedCommitJob&) override;

  Status on_rollback_job(const SlotParse&, const PackedRollbackJob&) override;

  Status on_volume_attach(const SlotParse& slot, const PackedVolumeAttachEvent& attach) override;

  Status on_volume_detach(const SlotParse& slot, const PackedVolumeDetachEvent& detach) override;

  Status on_volume_ids(const SlotParse& slot, const PackedVolumeIds&) override;

  Status on_volume_recovered(const SlotParse&, const PackedVolumeRecovered&) override;

  Status on_volume_format_upgrade(const SlotParse&, const PackedVolumeFormatUpgrade&) override;

  Status on_volume_trim(const SlotParse&, const VolumeTrimEvent&) override;
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the current last-known refresh information for all Volume metadata.
   */
  const VolumeMetadataRefreshInfo& get_refresh_info() const noexcept
  {
    return this->refresh_info_;
  }

  /** \brief Returns information about the most recent trim event slot.
   */
  const Optional<VolumeTrimEventInfo>& get_trim_event_info() const noexcept
  {
    return this->trim_event_info_;
  }

  /** \brief Returns the page transaction jobs that have been trimmed but not yet resolved, indexed
   * by their prepare slot.
   */
  const VolumePendingJobsUMap& get_pending_jobs() const noexcept
  {
    return this->pending_jobs_;
  }

  /** \brief Returns the size of the grant required by the VolumeTrimmer task.
   */
  usize get_trimmer_grant_size() const noexcept
  {
    return this->trimmer_grant_size_;
  }

  /** \brief Returns the number of grant bytes that should be reserved when the pending trim
   * operation resolves.
   */
  usize get_reserve_from_pending_trim() const noexcept
  {
    return this->reserve_from_pending_trim_;
  }

  auto debug_info() const noexcept
  {
    return [this](std::ostream& out) {
      out << std::endl
          << BATT_INSPECT(this->prepare_total_size_) << std::endl
          << BATT_INSPECT(this->commit_total_size_) << std::endl
          << BATT_INSPECT(this->rollback_total_size_) << std::endl
          << BATT_INSPECT(this->ids_total_size_) << std::endl
          << BATT_INSPECT(this->attach_total_size_) << std::endl
          << BATT_INSPECT(this->trim_total_size_) << "(" << BATT_INSPECT(trim_count_) << ")";
      ;
    };
  }

 private:
  slot_offset_type log_trim_pos_;
  VolumeMetadataRefreshInfo refresh_info_;
  Optional<VolumeTrimEventInfo> trim_event_info_;
  VolumePendingJobsUMap pending_jobs_;
  usize trimmer_grant_size_ =
      packed_sizeof_slot_with_payload_size(sizeof(PackedVolumeTrimEvent)) * 2;
  usize reserve_from_pending_trim_ = 0;
  bool first_trim_event_ = true;

  usize prepare_total_size_ = 0;
  usize commit_total_size_ = 0;
  usize rollback_total_size_ = 0;
  usize ids_total_size_ = 0;
  usize attach_total_size_ = 0;
  usize trim_total_size_ = 0;
  usize trim_count_ = 0;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_TRIMMER_RECOVERY_VISITOR_HPP
