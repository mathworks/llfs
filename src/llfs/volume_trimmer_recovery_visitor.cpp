//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_trimmer_recovery_visitor.hpp>
//

namespace llfs {

/*explicit*/ VolumeTrimmerRecoveryVisitor::VolumeTrimmerRecoveryVisitor(
    slot_offset_type trim_pos) noexcept
    : log_trim_pos_{trim_pos}
{
  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor created;" << BATT_INSPECT(trim_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_raw_data(const SlotParse&,
                                                 const Ref<const PackedRawData>&) /*override*/
{
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_prepare_job(
    const SlotParse& slot, const Ref<const PackedPrepareJob>& /*prepare*/) /*override*/
{
  const usize slot_size = slot.size_in_bytes();

  this->trimmer_grant_size_ += slot_size;
  this->prepare_total_size_ += slot_size;

  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_prepare_job(slot=" << slot.offset
               << "); trimmer_grant_size " << (this->trimmer_grant_size_ - slot_size) << " -> "
               << this->trimmer_grant_size_ << BATT_INSPECT(slot_size);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_commit_job(const SlotParse& slot,
                                                   const PackedCommitJob& /*commit*/) /*override*/
{
  const usize slot_size = slot.size_in_bytes();

  this->trimmer_grant_size_ += slot_size;
  this->commit_total_size_ += slot_size;

  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_commit_job(slot=" << slot.offset
               << "); trimmer_grant_size " << (this->trimmer_grant_size_ - slot_size) << " -> "
               << this->trimmer_grant_size_ << BATT_INSPECT(slot_size);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_rollback_job(const SlotParse& slot,
                                                     const PackedRollbackJob&) /*override*/
{
  const usize slot_size = slot.size_in_bytes();

  this->trimmer_grant_size_ += slot_size;
  this->rollback_total_size_ += slot_size;

  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_rollback_job(slot=" << slot.offset
               << "); trimmer_grant_size " << (this->trimmer_grant_size_ - slot_size) << " -> "
               << this->trimmer_grant_size_ << BATT_INSPECT(slot_size);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_attach(
    const SlotParse& slot, const PackedVolumeAttachEvent& attach) /*override*/
{
  const usize slot_size = slot.size_in_bytes();
  const bool refreshed_by_pending_trim =
      slot_less_than(this->log_trim_pos_, attach.trim_slot_offset);

  if (refreshed_by_pending_trim) {
    this->reserve_from_pending_trim_ += slot_size;
  } else {
    this->trimmer_grant_size_ += slot_size;
    this->attach_total_size_ += slot_size;
  }

  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_volume_attach(slot=" << slot.offset
               << "); trimmer_grant_size " << (this->trimmer_grant_size_ - slot_size) << " -> "
               << this->trimmer_grant_size_ << BATT_INSPECT(slot_size);

  auto iter = this->refresh_info_.most_recent_attach_slot.find(attach.base.id);
  if (iter == this->refresh_info_.most_recent_attach_slot.end()) {
    this->refresh_info_.most_recent_attach_slot.emplace(attach.base.id, slot.offset.lower_bound);
  } else {
    LLFS_VLOG(1) << "duplicate attachment";
    iter->second = slot.offset.lower_bound;
  }

  this->refresh_info_.most_recent_attach_slot[attach.base.id] = slot.offset.lower_bound;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_detach(
    const SlotParse& slot, const PackedVolumeDetachEvent& detach) /*override*/
{
  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_volume_detach(slot=" << slot.offset << ")";

  this->refresh_info_.most_recent_attach_slot[detach.id] = slot.offset.lower_bound;
  //
  // No need to increment the trimmer_grant_size_, since detach events don't need to be refreshed.

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_ids(const SlotParse& slot,
                                                   const PackedVolumeIds& ids) /*override*/
{
  const u64 slot_size = slot.size_in_bytes();
  const bool refreshed_by_pending_trim = slot_less_than(this->log_trim_pos_, ids.trim_slot_offset);

  if (refreshed_by_pending_trim) {
    this->reserve_from_pending_trim_ += slot_size;
  } else {
    this->trimmer_grant_size_ += slot_size;
    this->ids_total_size_ += slot_size;
  }

  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_volume_ids(slot=" << slot.offset
               << "); trimmer_grant_size " << (this->trimmer_grant_size_ - slot_size) << " -> "
               << this->trimmer_grant_size_ << BATT_INSPECT(slot_size);

  if (this->refresh_info_.most_recent_ids_slot) {
    LLFS_VLOG(1) << "duplicate ids; old_slot=" << *this->refresh_info_.most_recent_ids_slot
                 << ", new_slot=" << slot.offset.lower_bound;
  }

  this->refresh_info_.most_recent_ids_slot = slot.offset.lower_bound;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_recovered(const SlotParse&,
                                                         const PackedVolumeRecovered&) /*override*/
{
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_format_upgrade(
    const SlotParse&, const PackedVolumeFormatUpgrade&) /*override*/
{
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_trim(const SlotParse& slot,
                                                    const VolumeTrimEvent& trim_event) /*override*/
{
  const bool is_pending = slot_less_than(this->log_trim_pos_, trim_event.new_trim_pos);
  const u64 slot_size = slot.size_in_bytes();

  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_volume_trim(slot=" << slot.offset
               << ") trimmed_region == "
               << SlotRange{trim_event.old_trim_pos, trim_event.new_trim_pos}
               << BATT_INSPECT(is_pending) <<
      [&](std::ostream& out) {
        if (is_pending) {
          out << "; reserve_from_pending_trim " << (this->reserve_from_pending_trim_) << " -> "
              << (this->reserve_from_pending_trim_ + slot_size) << BATT_INSPECT(slot_size)
              << std::endl;
        } else {
          out << "; trimmer_grant_size " << (this->trimmer_grant_size_) << " -> "
              << (this->trimmer_grant_size_ + slot_size) << BATT_INSPECT(slot_size) << std::endl;
        }
      } << trim_event;

  if (is_pending) {
    this->reserve_from_pending_trim_ += slot_size;
  } else {
    this->trimmer_grant_size_ += slot_size;
    this->trim_total_size_ += slot_size;
    this->trim_count_ += 1;
  }

  if (is_pending) {
    if (this->trim_event_info_ != None) {
      LLFS_LOG_WARNING() << "Multiple pending trim events found!  Likely corrupted log...";
      //
      // TODO [tastolfi 2022-11-28] Force recovery to fail?
    }

    this->trim_event_info_.emplace();
    this->trim_event_info_->trim_event_slot = slot.offset;
    this->trim_event_info_->trimmed_region_slot_range = SlotRange{
        trim_event.old_trim_pos,
        trim_event.new_trim_pos,
    };
  } else {
    // If the trim is pending (branch above), then the trimmer will rescan the trimmed region and
    // recover the information in the trim event.  Otherwise, we need to apply this information
    // here.
    //
    batt::make_copy(trim_event.trimmed_prepare_jobs) |
        batt::seq::for_each([this](const TrimmedPrepareJob& job) {
          auto page_ids = batt::make_copy(job.page_ids) | batt::seq::collect_vec();

          LLFS_VLOG(1) << " -- " << BATT_INSPECT(job.prepare_slot) << ","
                       << BATT_INSPECT_RANGE(page_ids);

          this->pending_jobs_.emplace(job.prepare_slot, std::move(page_ids));
        });

    batt::make_copy(trim_event.committed_jobs) |
        batt::seq::for_each([this](slot_offset_type prepare_slot) {
          LLFS_VLOG(1) << " -- commit{prepare_slot=" << prepare_slot << "}";
          this->pending_jobs_.erase(prepare_slot);
        });
  }

  return batt::OkStatus();
}

}  //namespace llfs
