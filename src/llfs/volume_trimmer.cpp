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
/*static*/ VolumeTrimmer::DropRootsFn VolumeTrimmer::make_default_drop_roots_fn(
    PageCache& cache, PageRecycler& recycler, const boost::uuids::uuid& trimmer_uuid)
{
  return [&cache, &recycler, trimmer_uuid](slot_offset_type slot_offset,
                                           Slice<const PageId> roots_to_trim) -> Status {
    // Create a job for the root set ref count updates.
    //
    std::unique_ptr<PageCacheJob> job = cache.new_job();

    for (PageId page_id : roots_to_trim) {
      LLFS_VLOG(1) << " -- " << page_id;
      job->delete_root(page_id);
    }

    LLFS_VLOG(1) << "committing job";
    BATT_DEBUG_INFO("[VolumeTrimmer] commit(PageCacheJob)");

    return commit(std::move(job),
                  JobCommitParams{
                      .caller_uuid = &trimmer_uuid,
                      .caller_slot = slot_offset,
                      .recycler = as_ref(recycler),
                      .recycle_grant = nullptr,
                      .recycle_depth = -1,
                  },
                  Caller::Tablet_checkpoint_reaper_task_main);
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeTrimmer::VolumeTrimmer(const boost::uuids::uuid& trimmer_uuid,
                                          SlotLockManager& trim_control,
                                          std::unique_ptr<LogDevice::Reader>&& log_reader,
                                          TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                          DropRootsFn&& drop_roots) noexcept
    : trimmer_uuid_{trimmer_uuid}
    , trim_control_{trim_control}
    , log_reader_{std::move(log_reader)}
    , slot_reader_{*this->log_reader_}
    , slot_writer_{slot_writer}
    , drop_roots_{std::move(drop_roots)}
    , trimmer_grant_{BATT_OK_RESULT_OR_PANIC(this->slot_writer_.reserve(
          (packed_sizeof_slot_with_payload_size(sizeof(PackedVolumeIds)) +
           packed_sizeof_slot_with_payload_size(sizeof(PackedVolumeTrimEvent))),
          batt::WaitForResource::kFalse))}
    , pending_jobs_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmer::halt()
{
  LLFS_VLOG(1) << "VolumeTrimmer::halt";

  this->halt_requested_ = true;  // IMPORTANT: must come first!
  this->trimmer_grant_.revoke();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmer::push_grant(batt::Grant&& grant) noexcept
{
  LLFS_VLOG(1) << "trimmer grant: " << this->trimmer_grant_.size() << " -> "
               << (this->trimmer_grant_.size() + grant.size());
  this->trimmer_grant_.subsume(std::move(grant));
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

    LLFS_VLOG(1) << "new value for " << BATT_INSPECT(*trim_upper_bound) << "; scanning log slots ["
                 << this->slot_reader_.next_slot_offset() << "," << (*trim_upper_bound) << ")"
                 << BATT_INSPECT(this->trimmer_grant_.size());

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Create a new TrimJob to track the state of this trim.
    //
    TrimJob trim_job{trim_lower_bound, *trim_upper_bound, this->pending_jobs_};

    // First we must scan the trimmed region to figure out what needs to be included in the
    // TrimEvent slot.
    //
    BATT_REQUIRE_OK(trim_job.scan_trimmed_region(this->slot_reader_));

    // Refresh any Volume metadata discovered in the trimmed region.
    //
    BATT_REQUIRE_OK(trim_job.refresh_ids(this->trimmer_grant_, this->slot_writer_));
    BATT_REQUIRE_OK(trim_job.refresh_attachment_events(this->trimmer_grant_, this->slot_writer_));

    // Write the TrimEvent slot ahead of updating ref counts for any dropped roots (PageId) in the
    // trimmed region; the slot offset of this event is used as the slot to prevent double-updates
    // in the PageAllocators.
    //
    StatusOr<SlotRange> trim_event_slot =
        trim_job.write_trim_event(this->trimmer_grant_, this->slot_writer_);
    BATT_REQUIRE_OK(trim_event_slot);

    // After the TrimEvent has been flushed, decrement ref counts for all root page refs found in
    // the trimmed region.
    //
    BATT_REQUIRE_OK(
        trim_job.drop_obsolete_roots(this->slot_writer_, *trim_event_slot, this->drop_roots_));

    // Now that we have successfully updated ref counts and refreshed past events, there is no
    // information in the trimmed region that we need; trim the log!
    //
    BATT_REQUIRE_OK(trim_job.trim_log(this->trimmer_grant_, this->slot_writer_));

    // Update the trim position and repeat the loop.
    //
    const slot_offset_type new_trim_target = trim_job.new_trim_pos();

    bytes_trimmed += (new_trim_target - trim_lower_bound);
    trim_lower_bound = new_trim_target;

    LLFS_VLOG(1) << "trim is complete; awaiting new target (" << BATT_INSPECT(least_upper_bound)
                 << ")";
  }
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeTrimmer::TrimJob::TrimJob(slot_offset_type old_trim_pos,
                                             slot_offset_type new_trim_pos,
                                             VolumeTrimmer::PendingJobsMap& pending_jobs) noexcept
    : old_trim_pos_{old_trim_pos}
    , new_trim_pos_{new_trim_pos}
    , prior_pending_jobs_{pending_jobs}
    , trimmed_pending_jobs_{}
    , obsolete_roots_{}
    , ids_to_refresh_{}
    , grant_size_to_reserve_{0}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::scan_trimmed_region(
    TypedSlotReader<VolumeEventVariant>& slot_reader) noexcept
{
  usize new_trim_target = this->old_trim_pos_;

  StatusOr<usize> read_status = slot_reader.run(
      batt::WaitForResource::kTrue,
      [this, &new_trim_target](const SlotParse& slot, const auto& payload) -> Status {
        const SlotRange& slot_range = slot.offset;

        LLFS_VLOG(2) << "read slot: " << BATT_INSPECT(slot_range)
                     << BATT_INSPECT(!slot_less_than(slot_range.lower_bound, this->new_trim_pos_))
                     << BATT_INSPECT(slot_less_than(this->new_trim_pos_, slot_range.upper_bound));

        if (!slot_less_than(slot_range.lower_bound, this->new_trim_pos_) ||
            slot_less_than(this->new_trim_pos_, slot_range.upper_bound)) {
          return ::llfs::make_status(StatusCode::kBreakSlotReaderLoop);
        }

        LLFS_VLOG(2) << "visiting slot...";

        new_trim_target = slot_max(new_trim_target, slot_range.upper_bound);
        return this->visit_slot(slot, payload);
      });

  if (!read_status.ok() &&
      read_status.status() != ::llfs::make_status(StatusCode::kBreakSlotReaderLoop)) {
    BATT_REQUIRE_OK(read_status);
  }

  this->new_trim_pos_ = new_trim_target;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::refresh_ids(batt::Grant& trimmer_grant,
                                           TypedSlotWriter<VolumeEventVariant>& slot_writer)
{
  // If the trimmed log segment contained a PackedVolumeIds slot, then refresh it before trimming.
  //
  if (this->ids_to_refresh_) {
    LLFS_VLOG(1) << "Refreshing volume ids: " << *this->ids_to_refresh_;

    this->grant_size_to_reserve_ += packed_sizeof_slot(*this->ids_to_refresh_);

    StatusOr<SlotRange> id_refresh_slot = slot_writer.append(trimmer_grant, *this->ids_to_refresh_);

    BATT_REQUIRE_OK(id_refresh_slot);

    Status flush_status = [&] {
      BATT_DEBUG_INFO("[VolumeTrimmer] SlotWriter::sync(SlotUpperBoundAt{"
                      << id_refresh_slot->upper_bound << "})");
      return slot_writer.sync(LogReadMode::kDurable,
                              SlotUpperBoundAt{id_refresh_slot->upper_bound});
    }();

    BATT_REQUIRE_OK(flush_status);

    this->ids_to_refresh_ = None;
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::refresh_attachment_events(
    batt::Grant& trimmer_grant, TypedSlotWriter<VolumeEventVariant>& slot_writer)
{
  LLFS_VLOG(1) << "Refreshing device attachments";

  Optional<slot_offset_type> slot_upper_bound = None;

  //----- --- -- -  -  -   -
  const auto refresh_events = [&slot_upper_bound, this, &trimmer_grant,
                               &slot_writer](auto& events_to_refresh) -> Status {
    for (const auto& [uuid, event] : events_to_refresh) {
      this->grant_size_to_reserve_ += packed_sizeof_slot(event);

      StatusOr<SlotRange> slot_range = slot_writer.append(trimmer_grant, event);
      BATT_REQUIRE_OK(slot_range);

      clamp_min_slot(&slot_upper_bound, slot_range->upper_bound);
    }

    events_to_refresh.clear();

    return batt::OkStatus();
  };
  //----- --- -- -  -  -   -

  BATT_REQUIRE_OK(refresh_events(this->attachments_to_refresh_));
  BATT_REQUIRE_OK(refresh_events(this->detachments_to_refresh_));

  if (slot_upper_bound) {
    BATT_REQUIRE_OK(slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{*slot_upper_bound}));
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> VolumeTrimmer::TrimJob::write_trim_event(
    batt::Grant& trimmer_grant, TypedSlotWriter<VolumeEventVariant>& slot_writer) noexcept
{
  VolumeTrimEvent event;

  event.old_trim_pos = this->old_trim_pos_;
  event.new_trim_pos = this->new_trim_pos_;
  event.trimmed_prepare_jobs =
      batt::as_seq(this->trimmed_pending_jobs_.begin(), this->trimmed_pending_jobs_.end())  //
      | batt::seq::map([](const std::pair<const slot_offset_type, std::vector<PageId>>& kvp) {
          return TrimmedPrepareJob{
              .prepare_slot = kvp.first,
              .page_ids = batt::as_seq(kvp.second)  //
                          | batt::seq::decayed()    //
                          | batt::seq::boxed(),
          };
        })  //
      | batt::seq::boxed();

  this->grant_size_to_reserve_ += packed_sizeof_slot(event) * 2;

  StatusOr<SlotRange> result = slot_writer.append(trimmer_grant, std::move(event));

  // Balance the books.
  //
  {
    const usize common_size = std::min(this->grant_size_to_reserve_, this->grant_size_to_release_);
    this->grant_size_to_reserve_ -= common_size;
    this->grant_size_to_release_ -= common_size;
  }
  if (this->grant_size_to_release_ > 0) {
    usize release_size = 0;
    std::swap(release_size, this->grant_size_to_release_);
    trimmer_grant.spend(release_size, batt::WaitForResource::kFalse).IgnoreError();
  }

  // Transfer any trimmed pending jobs to the prior_pending_jobs_ map.
  //
  this->prior_pending_jobs_.insert(std::make_move_iterator(this->trimmed_pending_jobs_.begin()),
                                   std::make_move_iterator(this->trimmed_pending_jobs_.end()));
  this->trimmed_pending_jobs_.clear();

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::drop_obsolete_roots(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                                   const SlotRange& trim_event_slot,
                                                   const DropRootsFn& drop_roots) noexcept
{
  LLFS_VLOG(1) << "flushing trim event...";
  BATT_REQUIRE_OK(
      slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{trim_event_slot.upper_bound}));

  // If we found some obsolete jobs in the newly trimmed log segment, then collect up all root set
  // ref counts to release.
  //
  if (!this->obsolete_roots_.empty()) {
    std::vector<PageId> roots_to_trim;
    std::swap(roots_to_trim, this->obsolete_roots_);

    BATT_REQUIRE_OK(drop_roots(trim_event_slot.lower_bound, as_slice(roots_to_trim)));
  }
  // ** IMPORTANT ** It is only safe to trim the log after `commit(PageCacheJob, ...)` returns;
  // otherwise we will lose information about which root set page references to remove!

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::trim_log(batt::Grant& trimmer_grant,
                                        TypedSlotWriter<VolumeEventVariant>& slot_writer)
{
  StatusOr<batt::Grant> trimmed = [this, &slot_writer] {
    BATT_DEBUG_INFO("[VolumeTrimmer] SlotWriter::trim_and_reserve(" << this->new_trim_pos_ << ")");
    return slot_writer.trim_and_reserve(this->new_trim_pos_);
  }();
  BATT_REQUIRE_OK(trimmed);

  BATT_CHECK_LE(this->grant_size_to_reserve_, trimmed->size());
  if (this->grant_size_to_reserve_ > 0) {
    StatusOr<batt::Grant> retained = trimmed->spend(this->grant_size_to_reserve_);
    BATT_REQUIRE_OK(retained);

    trimmer_grant.subsume(std::move(*retained));
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& slot,
                                          const Ref<const PackedPrepareJob>& prepare)
{
  std::vector<PageId> root_page_ids = as_seq(*prepare.get().root_page_ids) |
                                      seq::map([](const PackedPageId& packed) -> PageId {
                                        return packed.as_page_id();
                                      }) |
                                      seq::collect_vec();

  LLFS_VLOG(1) << "visit_slot(" << BATT_INSPECT(slot.offset)
               << ", PrepareJob) root_page_ids=" << batt::dump_range(root_page_ids);

  const auto& [iter, inserted] =
      this->trimmed_pending_jobs_.emplace(slot.offset.lower_bound, std::move(root_page_ids));
  if (!inserted) {
    BATT_UNTESTED_LINE();
    LLFS_LOG_WARNING() << "duplicate prepare job found at " << BATT_INSPECT(slot.offset);
  } else {
    this->grant_size_to_release_ += packed_sizeof_slot(prepare.get());
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& slot, const PackedCommitJob& commit)
{
  LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", CommitJob)";

  const auto extract_pending_job = [&commit, this](PendingJobsMap& from) -> bool {
    auto iter = from.find(commit.prepare_slot);
    if (iter == from.end()) {
      return false;
    }

    this->obsolete_roots_.insert(this->obsolete_roots_.end(),  //
                                 iter->second.begin(), iter->second.end());

    from.erase(iter);

    return true;
  };

  if (!extract_pending_job(this->prior_pending_jobs_) &&
      !extract_pending_job(this->trimmed_pending_jobs_)) {
    LLFS_LOG_WARNING() << "commit slot found for missing prepare: "
                       << BATT_INSPECT(commit.prepare_slot);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/,
                                          const PackedRollbackJob& rollback)
{
  // The job has been resolved (rolled back); remove it from the maps.
  //
  this->prior_pending_jobs_.erase(rollback.prepare_slot);
  this->trimmed_pending_jobs_.erase(rollback.prepare_slot);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/, const PackedVolumeIds& ids)
{
  LLFS_VLOG(1) << "Found ids to refresh: " << ids;
  this->ids_to_refresh_ = ids;
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/,
                                          const PackedVolumeAttachEvent& attach)
{
  const auto& [iter, inserted] = this->attachments_to_refresh_.emplace(attach.client_uuid, attach);
  if (!inserted) {
    BATT_CHECK_EQ(iter->second.user_slot_offset, attach.user_slot_offset);
  }
  this->detachments_to_refresh_.erase(attach.client_uuid);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/,
                                          const PackedVolumeDetachEvent& detach)
{
  const auto& [iter, inserted] = this->detachments_to_refresh_.emplace(detach.client_uuid, detach);
  if (!inserted) {
    BATT_CHECK_EQ(iter->second.user_slot_offset, detach.user_slot_offset);
  }
  this->attachments_to_refresh_.erase(detach.client_uuid);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/,
                                          const PackedVolumeFormatUpgrade& /*upgrade*/)
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/,
                                          const PackedVolumeRecovered& /*recovered*/)
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/,
                                          const Ref<const PackedRawData>& /*raw*/)
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::TrimJob::visit_slot(const SlotParse& /*slot*/,
                                          const VolumeTrimEvent& trim_event)
{
  batt::make_copy(trim_event.trimmed_prepare_jobs)  //
      | batt::seq::for_each([this](const TrimmedPrepareJob& job) {
          // If the pending job from this past trim event is *still* pending as of the start of the
          // current trim job, then we transfer it to the `trimmed_pending_jobs_` map and treat it
          // like it was discovered in this trim.
          //
          auto iter = this->prior_pending_jobs_.find(job.prepare_slot);
          if (iter != this->prior_pending_jobs_.end()) {
            this->trimmed_pending_jobs_.emplace(iter->first, std::move(iter->second));
            this->prior_pending_jobs_.erase(iter);
          }
        });

  return OkStatus();
}

}  // namespace llfs
