//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_recovery_visitor.hpp>
//

#include <map>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeRecoveryVisitor::VolumeRecoveryVisitor(
    VolumeReader::SlotVisitorFn&& slot_recovery_fn, VolumePendingJobsMap& pending_jobs) noexcept
    : VolumeSlotDemuxer<NoneType>{std::move(slot_recovery_fn), pending_jobs}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<NoneType> VolumeRecoveryVisitor::on_volume_attach(const SlotParse& /*slot*/,
                                                           const PackedVolumeAttachEvent& attach)
{
  this->device_attachments.emplace(attach);
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<NoneType> VolumeRecoveryVisitor::on_volume_detach(const SlotParse& /*slot*/,
                                                           const PackedVolumeDetachEvent& detach)
{
  PackedVolumeAttachEvent attach{{
      .client_uuid = detach.client_uuid,
      .device_id = detach.device_id,
      .user_slot_offset = 0 /* doesn't matter; this field doesn't affect equality groups */,
  }};
  this->device_attachments.erase(attach);
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<NoneType> VolumeRecoveryVisitor::on_volume_ids(const SlotParse& slot,
                                                        const PackedVolumeIds& ids)
{
  this->ids = SlotWithPayload<PackedVolumeIds>{
      .slot_range = slot.offset,
      .payload = ids,
  };
  return None;
}

TODO[tastolfi 2022 - 11 - 18] on_volume_trim;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeRecoveryVisitor::resolve_pending_jobs(PageCache& cache, PageRecycler& recycler,
                                                   const boost::uuids::uuid& volume_uuid,
                                                   TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                                   batt::Grant& grant)
{
  Optional<slot_offset_type> slot_upper_bound;

  const auto& unsorted_pending_jobs = this->get_pending_jobs();
  std::map<slot_offset_type, SlotParseWithPayload<Ref<const PackedPrepareJob>>, SlotLess>
      pending_jobs(unsorted_pending_jobs.begin(), unsorted_pending_jobs.end());

  while (!pending_jobs.empty()) {
    const auto& [prepare_slot, slot_with_payload] = *pending_jobs.begin();
    const Ref<const PackedPrepareJob> pending_job = slot_with_payload.payload;

    // 1. Create a page cache job
    //
    std::unique_ptr<PageCacheJob> job = cache.new_job();

    // 2. Call recover_page for all the new pages.
    //
    std::vector<PageId> found_pages;
    Status overall_status = OkStatus();
    as_seq(*pending_job.get().new_page_ids) |
        seq::for_each([&](const PackedPageId& packed_page_id) {
          auto page_id = packed_page_id.as_page_id();
          Status recover_status = job->recover_page(page_id, volume_uuid, prepare_slot);
          if (recover_status.ok()) {
            found_pages.emplace_back(page_id);
          }
          overall_status.Update(recover_status);
        });

    // 3. If all the recover_page ops succeed, commit the job
    //
    if (overall_status.ok()) {
      Status commit_status = commit(std::move(job),
                                    JobCommitParams{
                                        .caller_uuid = &volume_uuid,
                                        .caller_slot = prepare_slot,
                                        .recycler = as_ref(recycler),
                                        .recycle_grant = nullptr,
                                        .recycle_depth = -1,
                                    },
                                    Caller::Unknown);

      if (commit_status.ok()) {
        StatusOr<SlotRange> commit_slot =
            slot_writer.append(grant, PackedCommitJob{
                                          .prepare_slot = prepare_slot,
                                      });

        BATT_REQUIRE_OK(commit_slot);

        clamp_min_slot(&slot_upper_bound, commit_slot->upper_bound);
        pending_jobs.erase(prepare_slot);
        continue;
      }
      // else - attempt to rollback instead...
    }

    // Else (maybe) delete any pages that _did_ succeed, and then-and-only-then write a
    // rollback job slot.
    //
    Status drop_status = parallel_drop_pages(found_pages, cache, job->job_id, Caller::Unknown);

    BATT_REQUIRE_OK(drop_status);

    StatusOr<SlotRange> rollback_slot = slot_writer.append(grant, PackedRollbackJob{
                                                                      .prepare_slot = prepare_slot,
                                                                  });

    BATT_REQUIRE_OK(rollback_slot);

    clamp_min_slot(&slot_upper_bound, rollback_slot->upper_bound);
    pending_jobs.erase(prepare_slot);

  }  // while (!pending_jobs.empty())

  // Make sure we flush any newly appended slots to the log.
  //
  if (slot_upper_bound) {
    return slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{.offset = *slot_upper_bound});
  }

  return OkStatus();
}

}  // namespace llfs
