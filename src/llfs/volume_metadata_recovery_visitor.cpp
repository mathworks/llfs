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
  this->metadata_.ids_last_refresh = slot.offset.lower_bound;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRecoveryVisitor::on_volume_attach(
    const SlotParse& slot, const PackedVolumeAttachEvent& attach) /*override*/
{
  this->metadata_.attachments[attach.id] = VolumeMetadata::AttachInfo{
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

}  //namespace llfs
