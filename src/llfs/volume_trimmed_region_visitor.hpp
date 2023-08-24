//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_TRIMMED_REGION_VISITOR_HPP
#define LLFS_VOLUME_TRIMMED_REGION_VISITOR_HPP

#include <llfs/config.hpp>
//
#include <llfs/slot.hpp>
#include <llfs/slot_parse.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/status.hpp>
#include <llfs/volume_event_visitor.hpp>
#include <llfs/volume_events.hpp>
#include <llfs/volume_trimmed_region_info.hpp>

namespace llfs {

/** \brief Reads slots from the passed reader, up to the given slot upper bound, collecting the
 * information needed to trim the log.
 */
StatusOr<VolumeTrimmedRegionInfo> read_trimmed_region(
    TypedSlotReader<VolumeEventVariant>& slot_reader, slot_offset_type trim_upper_bound);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Scans regions to be trimmed, saving information in a VolumeTrimmedRegionInfo struct.
 */
class VolumeTrimmedRegionVisitor : public VolumeEventVisitor<Status>::NullImpl
{
 public:
  explicit VolumeTrimmedRegionVisitor(VolumeTrimmedRegionInfo& trimmed_region) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // VolumeEventVisitor methods.
  //
  Status on_commit_job(const SlotParse&, const Ref<const PackedCommitJob>&) override;
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  VolumeTrimmedRegionInfo& trimmed_region_;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_TRIMMED_REGION_VISITOR_HPP
