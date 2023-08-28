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
#include <llfs/volume_metadata_refresher.hpp>
#include <llfs/volume_trimmed_region_info.hpp>

#include <batteries/strong_typedef.hpp>

namespace llfs {

BATT_STRONG_TYPEDEF(bool, HaveTrimEventGrant);

/** \brief Reads slots from the passed reader, up to the given slot upper bound, collecting the
 * information needed to trim the log.
 */
StatusOr<VolumeTrimmedRegionInfo> read_trimmed_region(
    TypedSlotReader<VolumeEventVariant>& slot_reader, VolumeMetadataRefresher& metadata_refresher,
    HaveTrimEventGrant have_trim_event_grant, slot_offset_type trim_upper_bound);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Scans regions to be trimmed, saving information in a VolumeTrimmedRegionInfo struct.
 */
class VolumeTrimmedRegionVisitor : public VolumeEventVisitor<Status>::NullImpl
{
 public:
  using Super = VolumeEventVisitor<Status>::NullImpl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit VolumeTrimmedRegionVisitor(VolumeTrimmedRegionInfo& trimmed_region,
                                      VolumeMetadataRefresher& metadata_refresher,
                                      HaveTrimEventGrant have_trim_event_grant) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // VolumeEventVisitor methods.
  //
  template <typename T>
  Status operator()(const SlotParse& slot, const T& event)
  {
    if (this->must_stop_and_trim()) {
      return ::llfs::make_status(StatusCode::kBreakSlotReaderLoop);
    }
    return this->Super::operator()(slot, event);
  }

  Status on_commit_job(const SlotParse&, const Ref<const PackedCommitJob>&) override;

  Status on_volume_attach(const SlotParse& slot, const PackedVolumeAttachEvent& attach) override;

  Status on_volume_detach(const SlotParse& slot, const PackedVolumeDetachEvent& detach) override;

  Status on_volume_ids(const SlotParse& slot, const PackedVolumeIds&) override;
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns true iff this visitor stopped the scan early because we need to trim in order
   * to replentish the metadata refresh grant (with the trimmed space) before going further.
   */
  bool must_stop_and_trim() const noexcept
  {
    return this->must_stop_and_trim_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void update_metadata_flush_grant_needed(const SlotParse& slot,
                                          const Optional<slot_offset_type> last_refresh_slot,
                                          usize slot_grant_size) noexcept;

  Status check_for_must_stop() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  VolumeTrimmedRegionInfo& trimmed_region_;

  VolumeMetadataRefresher& metadata_refresher_;

  HaveTrimEventGrant have_trim_event_grant_;

  u64 metadata_flush_grant_needed_ = 0;

  bool must_stop_and_trim_ = false;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_TRIMMED_REGION_VISITOR_HPP
