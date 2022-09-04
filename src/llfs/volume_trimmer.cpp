//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_trimmer.hpp>
//

#include <llfs/page_cache_job.hpp>
#include <llfs/slot_reader.hpp>

#include <batteries/stream_util.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeTrimmer::VolumeTrimmer(PageCache& cache, SlotLockManager& trim_control,
                                          PageRecycler& recycler,
                                          const boost::uuids::uuid& trimmer_uuid,
                                          std::unique_ptr<LogDevice::Reader>&& log_reader,
                                          TypedSlotWriter<VolumeEventVariant>& slot_writer) noexcept
    : cache_{cache}
    , recycler_{recycler}
    , trimmer_uuid_{trimmer_uuid}
    , trim_control_{trim_control}
    , log_reader_{std::move(log_reader)}
    , slot_reader_{*this->log_reader_}
    , slot_writer_{slot_writer}
    , roots_per_pending_job_{}
    , obsolete_roots_{}
    , ids_to_refresh_{}
    , id_refresh_grant_{BATT_OK_RESULT_OR_PANIC(this->slot_writer_.reserve(
          packed_sizeof_slot(PackedVolumeIds{}), batt::WaitForResource::kFalse))}
{
  BATT_CHECK_EQ(this->id_refresh_grant_.size(), packed_sizeof_slot(PackedVolumeIds{}));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmer::halt()
{
  LLFS_VLOG(1) << "VolumeTrimmer::halt";

  this->halt_requested_ = true;  // IMPORTANT: must come first!
  this->id_refresh_grant_.revoke();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::run()
{
  LLFS_VLOG(1) << "VolumeTrimmer::run";

  slot_offset_type trim_lower_bound = this->log_reader_->slot_offset();
  slot_offset_type least_upper_bound = trim_lower_bound + 1;
  usize bytes_trimmed = 0;

  // Main trimmer loop.
  //
  for (usize loop_counter = 0;; loop_counter += 1) {
    if (this->id_refresh_grant_.size() != packed_sizeof_slot(PackedVolumeIds{}) ||
        this->halt_requested_) {
      BATT_CHECK(this->halt_requested_);
      return OkStatus();
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Wait for the trim point to advance.
    //
    BATT_DEBUG_INFO("waiting for trim target to advance; "
                    << BATT_INSPECT(this->trim_control_.get_lower_bound())
                    << BATT_INSPECT(least_upper_bound) << BATT_INSPECT(bytes_trimmed)
                    << BATT_INSPECT(trim_lower_bound) << std::endl
                    << BATT_INSPECT(this->trim_control_.debug_info()) << BATT_INSPECT(loop_counter)
                    << BATT_INSPECT(this->trim_control_.is_closed()));

    StatusOr<slot_offset_type> trim_upper_bound =
        this->trim_control_.await_lower_bound(least_upper_bound);

    BATT_REQUIRE_OK(trim_upper_bound);

    least_upper_bound = *trim_upper_bound + 1;

    slot_offset_type new_trim_target = trim_lower_bound;

    LLFS_VLOG(1) << "new value for " << BATT_INSPECT(*trim_upper_bound) << "; scanning log slots ["
                 << this->slot_reader_.next_slot_offset() << "," << (*trim_upper_bound) << ")";

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Scan log slots up to the new unlocked slot offset, collecting page refs to release.
    //
    StatusOr<usize> read_status = this->slot_reader_.run(
        batt::WaitForResource::kTrue, [&](const SlotParse& slot, const auto& payload) -> Status {
          const SlotRange& slot_range = slot.offset;

          LLFS_VLOG(2) << "read slot: " << BATT_INSPECT(slot_range)
                       << BATT_INSPECT(!slot_less_than(slot_range.lower_bound, *trim_upper_bound))
                       << BATT_INSPECT(slot_less_than(*trim_upper_bound, slot_range.upper_bound));

          if (!slot_less_than(slot_range.lower_bound, *trim_upper_bound) ||
              slot_less_than(*trim_upper_bound, slot_range.upper_bound)) {
            return StatusCode::kBreakSlotReaderLoop;
          }

          LLFS_VLOG(2) << "visiting slot...";

          new_trim_target = slot_max(new_trim_target, slot_range.upper_bound);
          return this->visit_slot(slot, payload);
        });

    if (!read_status.ok() && read_status.status() != StatusCode::kBreakSlotReaderLoop) {
      BATT_REQUIRE_OK(read_status);
    }

    LLFS_VLOG(1) << "(" << BATT_INSPECT(new_trim_target)
                 << ") trimmed segment read; releasing obsolete pages";

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // If we found some obsolete jobs in the newly trimmed log segment, then collect up all root set
    // ref counts to release.
    //
    if (!this->obsolete_roots_.empty()) {
      std::vector<PageId> roots_to_trim;
      std::swap(roots_to_trim, this->obsolete_roots_);

      // Create a job for the root set ref count updates.
      //
      std::unique_ptr<PageCacheJob> job = this->cache_.new_job();

      for (PageId page_id : roots_to_trim) {
        LLFS_VLOG(1) << " -- " << page_id;
        job->delete_root(page_id);
      }

      LLFS_VLOG(1) << "committing job";

      BATT_DEBUG_INFO("[VolumeTrimmer] commit(PageCacheJob)");

      Status job_status = commit(std::move(job),
                                 JobCommitParams{
                                     .caller_uuid = &this->trimmer_uuid_,
                                     .caller_slot = new_trim_target,
                                     .recycler = as_ref(this->recycler_),
                                     .recycle_grant = nullptr,
                                     .recycle_depth = -1,
                                 },
                                 Caller::Tablet_checkpoint_reaper_task_main);

      // BATT_UNTESTED_COND(!job_status.ok());
      BATT_REQUIRE_OK(job_status);
    }
    // ** IMPORTANT ** It is only safe to trim the log after `commit(PageCacheJob, ...)` returns;
    // otherwise we will lose information about which root set page references to remove!

    LLFS_VLOG(1) << "checking for ids to refresh";

    // If the trimmed log segment contained a PackedVolumeIds slot, then refresh it before trimming.
    //
    if (this->ids_to_refresh_) {
      LLFS_VLOG(1) << " -- " << *this->ids_to_refresh_;

      StatusOr<SlotRange> id_refresh_slot =
          this->slot_writer_.append(this->id_refresh_grant_, *this->ids_to_refresh_);

      // BATT_UNTESTED_COND(!id_refresh_slot.ok());
      BATT_REQUIRE_OK(id_refresh_slot);

      Status flush_status = [&] {
        BATT_DEBUG_INFO("[VolumeTrimmer] SlotWriter::sync(SlotUpperBoundAt{"
                        << id_refresh_slot->upper_bound << "})");
        return this->slot_writer_.sync(LogReadMode::kDurable,
                                       SlotUpperBoundAt{id_refresh_slot->upper_bound});
      }();

      // BATT_UNTESTED_COND(!flush_status.ok());
      BATT_REQUIRE_OK(flush_status);

      this->ids_to_refresh_ = None;
    }

    LLFS_VLOG(1) << "trimming the log";

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Now that we have fully committed any ref_count deltas, trim the log and continue.
    //
    StatusOr<batt::Grant> trimmed = [&] {
      BATT_DEBUG_INFO("[VolumeTrimmer] SlotWriter::trim_and_reserve(" << new_trim_target << ")");
      return this->slot_writer_.trim_and_reserve(new_trim_target);
    }();

    // BATT_UNTESTED_COND(!trimmed.ok());
    BATT_REQUIRE_OK(trimmed);

    // Top off id_refresh_grant_ to bring it up to the required minimum size.
    //
    if (this->id_refresh_grant_.size() < packed_sizeof_slot(PackedVolumeIds{})) {
      StatusOr<batt::Grant> spent =
          trimmed->spend(packed_sizeof_slot(PackedVolumeIds{}) - this->id_refresh_grant_.size());

      // `spend` may be None if we are shutting down.
      //
      if (this->halt_requested_) {
        return OkStatus();
      }

      BATT_CHECK_OK(spent) << BATT_INSPECT(packed_sizeof_slot(PackedVolumeIds{}))
                           << BATT_INSPECT(this->id_refresh_grant_.size()) << " required="
                           << (packed_sizeof_slot(PackedVolumeIds{}) -
                               this->id_refresh_grant_.size())
                           << BATT_INSPECT(trimmed->size()) << BATT_INSPECT(this->halt_requested_);

      BATT_CHECK_EQ(this->id_refresh_grant_.get_issuer(), spent->get_issuer());
      this->id_refresh_grant_.subsume(std::move(*spent));
    }

    bytes_trimmed += (new_trim_target - trim_lower_bound);
    trim_lower_bound = new_trim_target;

    LLFS_VLOG(1) << "trim is complete; awaiting new target (" << BATT_INSPECT(least_upper_bound)
                 << ")";
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& slot, const Ref<const PackedPrepareJob>& prepare)
{
  std::vector<PageId> root_page_ids = as_seq(*prepare.get().root_page_ids) |
                                      seq::map([](const PackedPageId& packed) -> PageId {
                                        return packed.as_page_id();
                                      }) |
                                      seq::collect_vec();

  LLFS_VLOG(1) << "visit_slot(" << BATT_INSPECT(slot.offset)
               << ", PrepareJob) root_page_ids=" << batt::dump_range(root_page_ids);

  const auto& [iter, inserted] =
      this->roots_per_pending_job_.emplace(slot.offset.lower_bound, std::move(root_page_ids));
  (void)iter;
  if (!inserted) {
    BATT_UNTESTED_LINE();
    LLFS_LOG_WARNING() << "duplicate prepare job found at " << BATT_INSPECT(slot.offset);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& slot, const PackedCommitJob& commit)
{
  LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", CommitJob)";

  auto iter = this->roots_per_pending_job_.find(commit.prepare_slot);
  if (iter == this->roots_per_pending_job_.end()) {
    BATT_UNTESTED_LINE();
    LLFS_LOG_WARNING() << "commit slot found for missing prepare: "
                       << BATT_INSPECT(commit.prepare_slot);
  } else {
    LLFS_VLOG(1) << "Found obsolete PageId roots: " << batt::dump_range(iter->second);

    this->obsolete_roots_.insert(this->obsolete_roots_.end(),  //
                                 iter->second.begin(), iter->second.end());

    this->roots_per_pending_job_.erase(iter);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& /*slot*/, const PackedRollbackJob& rollback)
{
  BATT_UNTESTED_LINE();
  this->roots_per_pending_job_.erase(rollback.prepare_slot);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& /*slot*/, const PackedVolumeIds& ids)
{
  LLFS_VLOG(1) << "Found ids to refresh: " << ids;
  this->ids_to_refresh_ = ids;
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& /*slot*/, const PackedVolumeAttachEvent& attach)
{
  const auto& [iter, inserted] = this->attachments_to_refresh_.emplace(attach);
  if (!inserted) {
    BATT_CHECK_EQ(iter->user_slot_offset, attach.user_slot_offset);
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& /*slot*/, const PackedVolumeDetachEvent& detach)
{
  const auto& [iter, inserted] = this->detachments_to_refresh_.emplace(detach);
  if (!inserted) {
    BATT_CHECK_EQ(iter->user_slot_offset, detach.user_slot_offset);
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& /*slot*/,
                                 const PackedVolumeFormatUpgrade& /*upgrade*/)
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& /*slot*/,
                                 const PackedVolumeRecovered& /*recovered*/)
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::visit_slot(const SlotParse& /*slot*/, const Ref<const PackedRawData>& /*raw*/)
{
  return OkStatus();
}

}  // namespace llfs
