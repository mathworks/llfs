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
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_prepare_job(const SlotParse&,
                                                    const Ref<const PackedPrepareJob>&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_commit_job(const SlotParse&,
                                                   const Ref<const PackedCommitJob>&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_rollback_job(const SlotParse&,
                                                     const PackedRollbackJob&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_attach(const SlotParse&,
                                                      const PackedVolumeAttachEvent&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_detach(const SlotParse&,
                                                      const PackedVolumeDetachEvent&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_ids(const SlotParse&,
                                                   const PackedVolumeIds&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_recovered(const SlotParse&,
                                                         const PackedVolumeRecovered&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_format_upgrade(
    const SlotParse&, const PackedVolumeFormatUpgrade&) /*override*/
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmerRecoveryVisitor::on_volume_trim(const SlotParse& slot,
                                                    const VolumeTrimEvent& trim_event) /*override*/
{
  const bool is_pending = slot_less_than(this->log_trim_pos_, trim_event.new_trim_pos);

  LLFS_VLOG(1) << "VolumeTrimmerRecoveryVisitor::on_volume_trim(slot=" << slot.offset
               << ") trimmed_region == "
               << SlotRange{trim_event.old_trim_pos, trim_event.new_trim_pos}
               << BATT_INSPECT(is_pending) << BATT_INSPECT(this->log_trim_pos_) << trim_event;

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
  }

  return OkStatus();
}

}  //namespace llfs
