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
  this->device_attachments.emplace(attach.id);
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<NoneType> VolumeRecoveryVisitor::on_volume_detach(const SlotParse& /*slot*/,
                                                           const PackedVolumeDetachEvent& detach)
{
  this->device_attachments.erase(detach.id);
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

// TODO[tastolfi 2022-11-18] on_volume_trim;

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
    const Ref<const PackedPrepareJob> packed_prepare_job = slot_with_payload.payload;

    // Whether we commit or roll back, we will need a job.
    //
    std::unique_ptr<PageCacheJob> job = cache.new_job();

    // Keep track of the first fatal error we see.
    //
    Status overall_status = OkStatus();

    // See whether any ref count updates were flushed.
    //
    bool found_ref_count_updates =
        as_seq(*packed_prepare_job.get().page_device_ids)  //
        | seq::map([&](page_device_id_int page_device_id) -> bool {
            PageAllocator& page_allocator = cache.arena_for_device_id(page_device_id).allocator();
            Optional<PageAllocatorAttachmentStatus> attachment =
                page_allocator.get_client_attachment_status(volume_uuid);
            if (!attachment) {
              overall_status.Update(::llfs::make_status(StatusCode::kPageAllocatorNotAttached));
              return false;
            }
            return slot_at_least(attachment->user_slot, prepare_slot);
          })  //
        | seq::any_true();

    // If any device is not attached, its a fatal failure.
    //
    BATT_REQUIRE_OK(overall_status);

    // If any ref count update is found, then this job must have successfully written all of its new
    // pages and we will attempt to commit the job; otherwise, we roll it back.
    //
    if (!found_ref_count_updates) {
      //----- --- -- -  -  -   -
      LLFS_VLOG(1) << "Rolling back job at slot " << prepare_slot << "...";

      // Delete any pages that were successfully written, and then-and-only-then write a
      // rollback job slot.
      //
      Status drop_status = parallel_drop_pages(as_seq(*packed_prepare_job.get().new_page_ids)  //
                                                   | seq::map(BATT_OVERLOADS_OF(get_page_id))  //
                                                   | seq::collect_vec(),
                                               cache, job->job_id, Caller::Unknown);

      BATT_REQUIRE_OK(drop_status);

      StatusOr<SlotRange> rollback_slot =
          slot_writer.append(grant, PackedRollbackJob{
                                        .prepare_slot = prepare_slot,
                                    });

      BATT_REQUIRE_OK(rollback_slot);

      clamp_min_slot(&slot_upper_bound, rollback_slot->upper_bound);

      LLFS_VLOG(1) << "Successfully rolled back job at slot " << prepare_slot;

    } else {
      LLFS_VLOG(1) << "Committing job at slot " << prepare_slot << "...";

      //----- --- -- -  -  -   -
      // Call recover_page for all the new pages.
      //
      as_seq(*packed_prepare_job.get().new_page_ids)  //
          | seq::for_each([&](const PackedPageId& packed_page_id) {
              auto page_id = packed_page_id.as_page_id();
              Status recover_status = job->recover_page(page_id, volume_uuid, prepare_slot);
              overall_status.Update(recover_status);
            });

      // If any pages failed to load, that's a fatal failure.
      //
      BATT_REQUIRE_OK(overall_status);

      // Add the root page ids to the job so that ref counts will be recalculated correctly.
      //
      as_seq(*packed_prepare_job.get().root_page_ids)  //
          | seq::map(BATT_OVERLOADS_OF(get_page_id))   //
          | seq::for_each([&job](PageId page_id) {
              job->new_root(page_id);
            });

      // Commit the job!
      //
      Status commit_status = commit(std::move(job),
                                    JobCommitParams{
                                        .caller_uuid = &volume_uuid,
                                        .caller_slot = prepare_slot,
                                        .recycler = as_ref(recycler),
                                        .recycle_grant = nullptr,
                                        .recycle_depth = -1,
                                    },
                                    Caller::Unknown);

      BATT_REQUIRE_OK(commit_status);

      // Now we can write the commit slot.
      //
      StatusOr<SlotRange> commit_slot = slot_writer.append(grant, PackedCommitJob{
                                                                      .reserved_ = {},
                                                                      .prepare_slot = prepare_slot,
                                                                  });

      BATT_REQUIRE_OK(commit_slot);

      clamp_min_slot(&slot_upper_bound, commit_slot->upper_bound);

      LLFS_VLOG(1) << "Successfully recovered (committed) job at slot " << prepare_slot;
    }

    pending_jobs.erase(prepare_slot);

    // Make sure we flush any newly appended slots to the log.
    //
    if (slot_upper_bound) {
      BATT_REQUIRE_OK(slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{
                                                                  .offset = *slot_upper_bound,
                                                              }));
    }

  }  // while (!pending_jobs.empty())

  return OkStatus();
}

}  // namespace llfs
