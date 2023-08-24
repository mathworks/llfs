//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_METADATA_RECOVERY_VISITOR_HPP
#define LLFS_VOLUME_METADATA_RECOVERY_VISITOR_HPP

#include <llfs/config.hpp>
//
#include <llfs/status.hpp>
#include <llfs/volume_event_visitor.hpp>
#include <llfs/volume_metadata.hpp>

namespace llfs {

class VolumeMetadataRecoveryVisitor : public VolumeEventVisitor<Status>::NullImpl
{
 public:
  explicit VolumeMetadataRecoveryVisitor(VolumeMetadata& metadata) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // VolumeEventVisitor methods.
  //
  Status on_volume_attach(const SlotParse& slot, const PackedVolumeAttachEvent& attach) override;

  Status on_volume_detach(const SlotParse& slot, const PackedVolumeDetachEvent& detach) override;

  Status on_volume_ids(const SlotParse& slot, const PackedVolumeIds&) override;
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  VolumeMetadata& metadata_;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_METADATA_RECOVERY_VISITOR_HPP
