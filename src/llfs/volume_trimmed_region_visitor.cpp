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
    TypedSlotReader<VolumeEventVariant>& slot_reader, slot_offset_type trim_upper_bound)
{
  VolumeTrimmedRegionInfo trimmed_region;
  trimmed_region.slot_range.lower_bound = slot_reader.next_slot_offset();
  trimmed_region.slot_range.upper_bound = trimmed_region.slot_range.lower_bound;

  VolumeTrimmedRegionVisitor visit_slot{trimmed_region};

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

        trimmed_region.slot_range.upper_bound =
            slot_max(trimmed_region.slot_range.upper_bound, slot_range.upper_bound);

        return visit_slot(slot, payload);
      });

  if (!read_status.ok() &&
      (!reached_end || read_status.status() != StatusCode::kBreakSlotReaderLoop)) {
    BATT_REQUIRE_OK(read_status);
  }

  return {std::move(trimmed_region)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeTrimmedRegionVisitor::VolumeTrimmedRegionVisitor(
    VolumeTrimmedRegionInfo& trimmed_region) noexcept
    : trimmed_region_{trimmed_region}
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
    for (const PackedPageId& packed_page_id : *commit.root_page_ids) {
      this->trimmed_region_.obsolete_roots.emplace_back(packed_page_id.unpack());
    }
  }

  return OkStatus();
}

}  //namespace llfs
