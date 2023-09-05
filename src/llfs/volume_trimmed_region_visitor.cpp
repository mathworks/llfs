//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_trimmed_region_visitor.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<VolumeTrimmedRegionInfo> read_trimmed_region(
    TypedSlotReader<VolumeEventVariant>& slot_reader, VolumeMetadataRefresher& metadata_refresher,
    HaveTrimEventGrant have_trim_event_grant, slot_offset_type trim_upper_bound)
{
  VolumeTrimmedRegionInfo trimmed_region;
  trimmed_region.slot_range.lower_bound = slot_reader.next_slot_offset();
  trimmed_region.slot_range.upper_bound = trimmed_region.slot_range.lower_bound;

  VolumeTrimmedRegionVisitor visit_slot{trimmed_region, metadata_refresher, have_trim_event_grant};

  bool reached_end = false;

  StatusOr<usize> read_status = slot_reader.run(
      batt::WaitForResource::kTrue, [&](const SlotParse& slot, const auto& payload) -> Status {
        const SlotRange& slot_range = slot.offset;
        const u64 slot_size = slot.size_in_bytes();

        const bool starts_before_trim_pos =  //
            slot_less_than(slot_range.lower_bound, trim_upper_bound);

        const bool ends_after_trim_pos =  //
            slot_less_than(trim_upper_bound, slot_range.upper_bound);

        const bool will_visit = starts_before_trim_pos && !ends_after_trim_pos;

        LLFS_VLOG(1) << "read slot: " << BATT_INSPECT(slot_range) << BATT_INSPECT(slot_size)
                     << BATT_INSPECT(starts_before_trim_pos) << BATT_INSPECT(ends_after_trim_pos)
                     << BATT_INSPECT(will_visit);

        if (!will_visit) {
          reached_end = true;
          return ::llfs::make_status(StatusCode::kBreakSlotReaderLoop);
        }

        BATT_REQUIRE_OK(visit_slot(slot, payload));

        trimmed_region.slot_range.upper_bound =
            slot_max(trimmed_region.slot_range.upper_bound, slot_range.upper_bound);

        return OkStatus();
      });

  if (!read_status.ok() && !(read_status.status() == StatusCode::kBreakSlotReaderLoop &&
                             (reached_end || visit_slot.must_stop_and_trim()))) {
    if (read_status.status() == StatusCode::kBreakSlotReaderLoop &&
        visit_slot.must_stop_and_trim()) {
      BATT_CHECK_NE(trimmed_region.slot_range.lower_bound, trimmed_region.slot_range.upper_bound)
          << "VolumeTrimmedRegionVisitor says must_stop_and_trim() == true, but the size of the "
             "region to trim is 0!  We are deadlocked.";
    }
    BATT_REQUIRE_OK(read_status);
  }

  return {std::move(trimmed_region)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeTrimmedRegionVisitor::VolumeTrimmedRegionVisitor(
    VolumeTrimmedRegionInfo& trimmed_region, VolumeMetadataRefresher& metadata_refresher,
    HaveTrimEventGrant have_trim_event_grant) noexcept
    : trimmed_region_{trimmed_region}
    , metadata_refresher_{metadata_refresher}
    , have_trim_event_grant_{have_trim_event_grant}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmedRegionVisitor::on_commit_job(
    const SlotParse& slot, const Ref<const PackedCommitJob>& commit_ref) /*override*/
{
  const PackedCommitJob& commit = commit_ref.get();

  LLFS_VLOG(1) << "VolumeTrimmedRegionVisitor::on_commit_job(slot=" << slot.offset << ");"
               << BATT_INSPECT(slot.size_in_bytes());

  if (commit.root_page_ids) {
    if (!this->have_trim_event_grant_) {
      this->must_stop_and_trim_ = true;
      return ::llfs::make_status(StatusCode::kBreakSlotReaderLoop);
    }
    for (const PackedPageId& packed_page_id : *commit.root_page_ids) {
      this->trimmed_region_.obsolete_roots.emplace_back(packed_page_id.unpack());
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmedRegionVisitor::on_volume_attach(
    const SlotParse& slot, const PackedVolumeAttachEvent& attach) /*override*/
{
  this->update_metadata_flush_grant_needed(                                     //
      slot, this->metadata_refresher_.attachment_last_refresh_slot(attach.id),  //
      VolumeMetadata::kAttachmentGrantSize);

  return this->check_for_must_stop();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmedRegionVisitor::on_volume_detach(
    const SlotParse& slot, const PackedVolumeDetachEvent& detach) /*override*/
{
  this->update_metadata_flush_grant_needed(                                     //
      slot, this->metadata_refresher_.attachment_last_refresh_slot(detach.id),  //
      VolumeMetadata::kAttachmentGrantSize);

  return this->check_for_must_stop();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmedRegionVisitor::on_volume_ids(const SlotParse& slot,
                                                 const PackedVolumeIds&) /*override*/
{
  this->update_metadata_flush_grant_needed(                     //
      slot, this->metadata_refresher_.ids_last_refresh_slot(),  //
      VolumeMetadata::kVolumeIdsGrantSize);

  return this->check_for_must_stop();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmedRegionVisitor::update_metadata_flush_grant_needed(
    const SlotParse& slot, const Optional<slot_offset_type> last_refresh_slot,
    usize slot_grant_size) noexcept
{
  if (last_refresh_slot && !slot_less_than(slot.offset.lower_bound, *last_refresh_slot)) {
    this->metadata_flush_grant_needed_ += slot_grant_size;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmedRegionVisitor::check_for_must_stop() noexcept
{
  if (this->metadata_flush_grant_needed_ > this->metadata_refresher_.grant_size()) {
    this->must_stop_and_trim_ = true;
    return ::llfs::make_status(StatusCode::kBreakSlotReaderLoop);
  }
  return OkStatus();
}

}  //namespace llfs
