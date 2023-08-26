//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_metadata_recovery_visitor.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeMetadataRecoveryVisitor::VolumeMetadataRecoveryVisitor(
    VolumeMetadata& metadata) noexcept
    : metadata_{metadata}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRecoveryVisitor::on_volume_ids(const SlotParse& slot,
                                                    const PackedVolumeIds& ids) /*override*/
{
  this->metadata_.ids = ids;
  if (this->metadata_.ids_last_refresh) {
    this->ids_duplicated_ = true;
  }
  this->metadata_.ids_last_refresh = slot.offset.lower_bound;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRecoveryVisitor::on_volume_attach(
    const SlotParse& slot, const PackedVolumeAttachEvent& attach) /*override*/
{
  VolumeMetadata::AttachInfo& attach_info = this->metadata_.attachments[attach.id];

  if (attach_info.last_refresh) {
    this->attachment_duplicated_.emplace(attach.id);
  }

  attach_info = VolumeMetadata::AttachInfo{
      .last_refresh = slot.offset.lower_bound,
      .event = attach,
  };

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRecoveryVisitor::on_volume_detach(
    const SlotParse&, const PackedVolumeDetachEvent& detach) /*override*/
{
  this->metadata_.attachments.erase(detach.id);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize VolumeMetadataRecoveryVisitor::calculate_initial_refresh_grant() const noexcept
{
  const usize grant_size = this->metadata_.grant_target();

  const usize will_be_reclaimed_via_trim =
      (this->ids_duplicated_ ? VolumeMetadata::kVolumeIdsGrantSize : 0) +
      (this->attachment_duplicated_.size() * VolumeMetadata::kAttachmentGrantSize);

  if (will_be_reclaimed_via_trim < grant_size) {
    return grant_size - will_be_reclaimed_via_trim;
  }
  return 0;
}

}  //namespace llfs
