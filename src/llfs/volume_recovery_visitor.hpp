//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_RECOVERY_VISITOR_HPP
#define LLFS_VOLUME_RECOVERY_VISITOR_HPP

#include <llfs/optional.hpp>
#include <llfs/status.hpp>
#include <llfs/volume_reader.hpp>
#include <llfs/volume_slot_demuxer.hpp>

#include <map>
#include <unordered_set>

namespace llfs {

class VolumeRecoveryVisitor : public VolumeSlotDemuxer<NoneType>
{
 public:
  explicit VolumeRecoveryVisitor(VolumeReader::SlotVisitorFn&& slot_recovery_fn,
                                 VolumePendingJobsMap& pending_jobs) noexcept;

  Status resolve_pending_jobs(PageCache& cache, PageRecycler& recycler,
                              const boost::uuids::uuid& volume_uuid,
                              TypedSlotWriter<VolumeEventVariant>& slot_writer, batt::Grant& grant);

  StatusOr<NoneType> on_volume_attach(const SlotParse&, const PackedVolumeAttachEvent&) override;

  StatusOr<NoneType> on_volume_detach(const SlotParse&, const PackedVolumeDetachEvent&) override;

  StatusOr<NoneType> on_volume_ids(const SlotParse&, const PackedVolumeIds&) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // public data
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The device attachments for this volume.
  //
  std::unordered_set<PackedVolumeAttachEvent, PackedVolumeAttachEvent::Hash> device_attachments;

  // The uuids for the volume.
  //
  Optional<SlotWithPayload<PackedVolumeIds>> ids;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_RECOVERY_VISITOR_HPP

#include <llfs/volume_recovery_visitor.ipp>
