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
/*static*/ VolumeDropRootsFn VolumeTrimmer::make_default_drop_roots_fn(
    PageCache& cache, PageRecycler& recycler, const boost::uuids::uuid& trimmer_uuid)
{
  return [&cache, &recycler, trimmer_uuid](slot_offset_type slot_offset,
                                           Slice<const PageId> roots_to_trim) -> Status {
    // Create a job for the root set ref count updates.
    //
    std::unique_ptr<PageCacheJob> job = cache.new_job();

    LLFS_VLOG(1) << "Dropping PageId roots from the log..." << BATT_INSPECT(slot_offset);
    for (PageId page_id : roots_to_trim) {
      LLFS_VLOG(1) << " -- " << page_id;
      job->delete_root(page_id);
    }

    LLFS_VLOG(1) << "Committing job";
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
                                          VolumeDropRootsFn&& drop_roots,
                                          const RecoveryVisitor& recovery_visitor) noexcept
    : trimmer_uuid_{trimmer_uuid}
    , trim_control_{trim_control}
    , log_reader_{std::move(log_reader)}
    , slot_reader_{*this->log_reader_}
    , slot_writer_{slot_writer}
    , drop_roots_{std::move(drop_roots)}
    , trimmer_grant_{BATT_OK_RESULT_OR_PANIC(this->slot_writer_.reserve(
          (packed_sizeof_slot_with_payload_size(sizeof(PackedVolumeIds)) +
           packed_sizeof_slot_with_payload_size(sizeof(PackedVolumeTrimEvent)) * 2 +
           recovery_visitor.get_trimmer_grant_size()),
          batt::WaitForResource::kFalse))}
    , pending_jobs_{recovery_visitor.get_pending_jobs()}
    , refresh_info_{recovery_visitor.get_refresh_info()}
    , latest_trim_event_{recovery_visitor.get_trim_event_info()}
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
  LLFS_VLOG(2) << "trimmer grant: " << this->trimmer_grant_.size() << " -> "
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

  // If we found a pending trim event during recovery, use its trim upper bound first.
  //
  if (this->latest_trim_event_) {
    BATT_CHECK_EQ(trim_lower_bound,
                  this->latest_trim_event_->trimmed_region_slot_range.lower_bound);

    least_upper_bound = this->latest_trim_event_->trimmed_region_slot_range.upper_bound;
  }

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

    // If we are recovering a previously initiated trim, then limit the trim upper bound.
    //
    if (this->latest_trim_event_) {
      *trim_upper_bound = this->latest_trim_event_->trimmed_region_slot_range.upper_bound;
    }

    // The next time we wait for a new trim target, it should be at least one past the previously
    // observed offset.
    //
    least_upper_bound = *trim_upper_bound + 1;

    LLFS_VLOG(1) << "new value for " << BATT_INSPECT(*trim_upper_bound) << "; scanning log slots ["
                 << this->slot_reader_.next_slot_offset() << "," << (*trim_upper_bound) << ")"
                 << BATT_INSPECT(this->trimmer_grant_.size());

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Scan the trimmed region if necessary.
    //
    if (!this->trimmed_region_info_) {
      StatusOr<VolumeTrimmedRegionInfo> info =
          read_trimmed_region(this->slot_reader_, *trim_upper_bound, this->pending_jobs_);
      BATT_REQUIRE_OK(info);
      this->trimmed_region_info_.emplace(std::move(*info));
    }
    BATT_CHECK(this->trimmed_region_info_);

    if (this->trimmed_region_info_->slot_range.empty()) {
      LLFS_VLOG(1) << "Trimmed region too small; waiting for more to be trimmed,"
                   << BATT_INSPECT(least_upper_bound);
      this->trimmed_region_info_ = batt::None;
      continue;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Make sure all Volume metadata has been refreshed.
    //
    BATT_REQUIRE_OK(refresh_volume_metadata(this->slot_writer_, this->trimmer_grant_,
                                            this->refresh_info_, *this->trimmed_region_info_));

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Write a TrimEvent to the log if necessary.
    //
    if (this->trimmed_region_info_->requires_trim_event_slot() && !this->latest_trim_event_) {
      BATT_ASSIGN_OK_RESULT(this->latest_trim_event_,
                            write_trim_event(this->slot_writer_, this->trimmer_grant_,
                                             *this->trimmed_region_info_, this->pending_jobs_));
      BATT_CHECK(this->latest_trim_event_);
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Trim the log.
    //
    const slot_offset_type new_trim_target = this->trimmed_region_info_->slot_range.upper_bound;

    Status trim_result = trim_volume_log(
        this->slot_writer_, this->trimmer_grant_, std::move(this->latest_trim_event_),
        std::move(*this->trimmed_region_info_), this->drop_roots_, this->pending_jobs_);

    this->latest_trim_event_ = batt::None;
    this->trimmed_region_info_ = batt::None;

    BATT_REQUIRE_OK(trim_result);

    // Update the trim position and repeat the loop.
    //
    bytes_trimmed += (new_trim_target - trim_lower_bound);
    trim_lower_bound = new_trim_target;

    LLFS_VLOG(1) << "Trim(" << new_trim_target << ") is complete; awaiting new target ("
                 << BATT_INSPECT(least_upper_bound) << ")";
  }
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<VolumeTrimmedRegionInfo> read_trimmed_region(
    TypedSlotReader<VolumeEventVariant>& slot_reader, slot_offset_type trim_upper_bound,
    VolumePendingJobsUMap& prior_pending_jobs)
{
  StatusOr<VolumeTrimmedRegionInfo> result = VolumeTrimmedRegionInfo{};

  result->slot_range.lower_bound = slot_reader.next_slot_offset();
  result->slot_range.upper_bound = result->slot_range.lower_bound;

  StatusOr<usize> read_status = slot_reader.run(
      batt::WaitForResource::kTrue,
      [trim_upper_bound, &result, &prior_pending_jobs](const SlotParse& slot,
                                                       const auto& payload) -> Status {
        const SlotRange& slot_range = slot.offset;

        LLFS_VLOG(2) << "read slot: " << BATT_INSPECT(slot_range)
                     << BATT_INSPECT(!slot_less_than(slot_range.lower_bound, trim_upper_bound))
                     << BATT_INSPECT(slot_less_than(trim_upper_bound, slot_range.upper_bound));

        if (!slot_less_than(slot_range.lower_bound, trim_upper_bound) ||
            slot_less_than(trim_upper_bound, slot_range.upper_bound)) {
          return ::llfs::make_status(StatusCode::kBreakSlotReaderLoop);
        }

        LLFS_VLOG(2) << "visiting slot...";

        result->slot_range.upper_bound =
            slot_max(result->slot_range.upper_bound, slot_range.upper_bound);

        return batt::make_case_of_visitor(
            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const SlotParse& slot, const Ref<const PackedPrepareJob>& prepare) {
              std::vector<PageId> root_page_ids =
                  as_seq(*prepare.get().root_page_ids) |
                  seq::map([](const PackedPageId& packed) -> PageId {
                    return packed.as_page_id();
                  }) |
                  seq::collect_vec();

              LLFS_VLOG(1) << "visit_slot(" << BATT_INSPECT(slot.offset)
                           << ", PrepareJob) root_page_ids=" << batt::dump_range(root_page_ids);

              result->grant_size_to_release += packed_sizeof_slot(prepare.get());

              const auto& [iter, inserted] =
                  result->pending_jobs.emplace(slot.offset.lower_bound, std::move(root_page_ids));
              if (!inserted) {
                BATT_UNTESTED_LINE();
                LLFS_LOG_WARNING()
                    << "duplicate prepare job found at " << BATT_INSPECT(slot.offset);
              }

              return OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const SlotParse& slot, const PackedCommitJob& commit) {
              LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", CommitJob)";

              //----- --- -- -  -  -   -
              const auto extract_pending_job = [&](VolumePendingJobsUMap& from) -> bool {
                auto iter = from.find(commit.prepare_slot);
                if (iter == from.end()) {
                  return false;
                }

                result->obsolete_roots.insert(result->obsolete_roots.end(),  //
                                              iter->second.begin(), iter->second.end());

                from.erase(iter);

                return true;
              };
              //----- --- -- -  -  -   -

              result->grant_size_to_release += packed_sizeof_slot(commit);

              // Check the pending PrepareJob slots from before this trim.
              //
              if (extract_pending_job(prior_pending_jobs)) {
                // Sanity check: if this commit's prepare slot is in _prior_ pending jobs, then the
                // prepare slot offset should be before the old trim pos (otherwise we would be
                // finding it in trimmed_pending_jobs_).
                //
                BATT_CHECK(slot_less_than(commit.prepare_slot, result->slot_range.lower_bound))
                    << BATT_INSPECT(commit.prepare_slot) << BATT_INSPECT(result->slot_range);

                result->resolved_jobs.emplace_back(commit.prepare_slot);

                return batt::OkStatus();
              }

              // Check the current PrepareJob slots for a match.
              //
              if (extract_pending_job(result->pending_jobs)) {
                return batt::OkStatus();
              }

              LLFS_LOG_WARNING() << "commit slot found for missing prepare: "
                                 << BATT_INSPECT(commit.prepare_slot) << BATT_INSPECT(slot.offset);

              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const SlotParse&, const PackedRollbackJob& rollback) {
              // The job has been resolved (rolled back); remove it from the maps.
              //
              LLFS_VLOG(1) << "Rolling back pending job;" << BATT_INSPECT(rollback.prepare_slot);
              if (prior_pending_jobs.erase(rollback.prepare_slot) == 1u) {
                result->resolved_jobs.emplace_back(rollback.prepare_slot);
              }
              result->pending_jobs.erase(rollback.prepare_slot);

              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const SlotParse& slot, const PackedVolumeIds& ids) {
              LLFS_VLOG(1) << "Found ids to refresh: " << ids << BATT_INSPECT(slot.offset);
              result->ids_to_refresh = ids;
              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const SlotParse&, const PackedVolumeAttachEvent& attach) {
              const auto& [iter, inserted] =
                  result->attachments_to_refresh.emplace(attach.id, attach);
              if (!inserted) {
                BATT_CHECK_EQ(iter->second.user_slot_offset, attach.user_slot_offset);
              }
              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const SlotParse&, const PackedVolumeDetachEvent& detach) {
              result->attachments_to_refresh.erase(detach.id);
              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const SlotParse&, const VolumeTrimEvent& trim_event) {
              batt::make_copy(trim_event.trimmed_prepare_jobs)  //
                  | batt::seq::for_each([&](const TrimmedPrepareJob& job) {
                      // If the pending job from this past trim event is *still* pending as of the
                      // start of the current trim job, then we transfer it to the
                      // pending jobs map and treat it like it was discovered in this
                      // trim.
                      //
                      auto iter = prior_pending_jobs.find(job.prepare_slot);
                      if (iter != prior_pending_jobs.end()) {
                        result->pending_jobs.emplace(iter->first, std::move(iter->second));
                        prior_pending_jobs.erase(iter);
                      }
                    });

              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [](const SlotParse&, const auto& /*payload*/) {
              return batt::OkStatus();
            }
            //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
            )(slot, payload);
      });

  LLFS_VLOG(1) << "read_trimmed_region: done visiting slots," << BATT_INSPECT(read_status);

  if (!read_status.ok() &&
      read_status.status() != ::llfs::make_status(StatusCode::kBreakSlotReaderLoop)) {
    BATT_REQUIRE_OK(read_status);
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status refresh_volume_metadata(TypedSlotWriter<VolumeEventVariant>& slot_writer, batt::Grant& grant,
                               VolumeMetadataRefreshInfo& refresh_info,
                               VolumeTrimmedRegionInfo& trimmed_region)
{
  LLFS_VLOG(1) << "Refreshing Volume metadata";

  Optional<slot_offset_type> sync_point;

  // If the trimmed log segment contained a PackedVolumeIds slot, then refresh it before trimming.
  //
  if (trimmed_region.ids_to_refresh) {
    LLFS_VLOG(1) << "Refreshing volume ids: " << *trimmed_region.ids_to_refresh;

    trimmed_region.grant_size_to_reserve += packed_sizeof_slot(*trimmed_region.ids_to_refresh);

    const slot_offset_type last_refresh_slot =
        refresh_info.most_recent_ids_slot.value_or(trimmed_region.slot_range.lower_bound);

    if (slot_less_than(last_refresh_slot, trimmed_region.slot_range.upper_bound)) {
      StatusOr<SlotRange> id_refresh_slot =
          slot_writer.append(grant, *trimmed_region.ids_to_refresh);

      BATT_REQUIRE_OK(id_refresh_slot);

      refresh_info.most_recent_ids_slot = id_refresh_slot->lower_bound;
      clamp_min_slot(&sync_point, id_refresh_slot->upper_bound);
    }

    LLFS_VLOG(1) << " -- " << BATT_INSPECT(sync_point);
  }

  // Refresh any attachments that will be lost in this trim.
  //
  for (const auto& [attach_id, event] : trimmed_region.attachments_to_refresh) {
    trimmed_region.grant_size_to_reserve += packed_sizeof_slot(event);

    // Skip this attachment if we know it has been refreshed at a higher slot.
    //
    {
      auto iter = refresh_info.most_recent_attach_slot.find(attach_id);
      if (iter != refresh_info.most_recent_attach_slot.find(attach_id)) {
        if (!slot_less_than(iter->second, trimmed_region.slot_range.upper_bound)) {
          continue;
        }
      }
    }

    LLFS_VLOG(1) << "Refreshing attachment " << attach_id;

    StatusOr<SlotRange> slot_range = slot_writer.append(grant, event);
    BATT_REQUIRE_OK(slot_range);

    // Update the latest refresh slot for this attachment.
    //
    refresh_info.most_recent_attach_slot.emplace(attach_id, slot_range->lower_bound);

    clamp_min_slot(&sync_point, slot_range->upper_bound);

    LLFS_VLOG(1) << " -- " << BATT_INSPECT(sync_point);
  }

  // Make sure all refreshed slots are flushed before returning.
  //
  if (sync_point) {
    Status flush_status = [&] {
      BATT_DEBUG_INFO("[VolumeTrimmer] SlotWriter::sync(SlotUpperBoundAt{" << *sync_point << "})");
      return slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{*sync_point});
    }();

    BATT_REQUIRE_OK(flush_status);
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<VolumeTrimEventInfo> write_trim_event(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                               batt::Grant& grant,
                                               VolumeTrimmedRegionInfo& trimmed_region,
                                               VolumePendingJobsUMap& /*prior_pending_jobs*/)
{
  // TODO [tastolfi 2023-05-22] use or remove `prior_pending_jobs` arg

  VolumeTrimEvent event;

  event.old_trim_pos = trimmed_region.slot_range.lower_bound;
  event.new_trim_pos = trimmed_region.slot_range.upper_bound;

  event.committed_jobs = batt::as_seq(trimmed_region.resolved_jobs)  //
                         | batt::seq::decayed()                      //
                         | batt::seq::boxed();

  event.trimmed_prepare_jobs =
      batt::as_seq(trimmed_region.pending_jobs.begin(), trimmed_region.pending_jobs.end())  //
      | batt::seq::map([](const std::pair<const slot_offset_type, std::vector<PageId>>& kvp) {
          return TrimmedPrepareJob{
              .prepare_slot = kvp.first,
              .page_ids = batt::as_seq(kvp.second)  //
                          | batt::seq::decayed()    //
                          | batt::seq::boxed(),
          };
        })  //
      | batt::seq::boxed();

  trimmed_region.grant_size_to_reserve += packed_sizeof_slot(event);

  StatusOr<SlotRange> result = slot_writer.append(grant, std::move(event));
  BATT_REQUIRE_OK(result);

  LLFS_VLOG(1) << "Flushing trim event at slot_range=" << *result
               << BATT_INSPECT(trimmed_region.slot_range);

  BATT_REQUIRE_OK(slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{result->upper_bound}));

  return VolumeTrimEventInfo{
      .trim_event_slot = *result,
      .trimmed_region_slot_range = trimmed_region.slot_range,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status trim_volume_log(TypedSlotWriter<VolumeEventVariant>& slot_writer, batt::Grant& grant,
                       Optional<VolumeTrimEventInfo>&& trim_event,
                       VolumeTrimmedRegionInfo&& trimmed_region,
                       const VolumeDropRootsFn& drop_roots,
                       VolumePendingJobsUMap& prior_pending_jobs)
{
  BATT_CHECK_IMPLIES(!trim_event, trimmed_region.obsolete_roots.empty());

  // If we found some obsolete jobs in the newly trimmed log segment, then collect up all root set
  // ref counts to release.
  //
  if (!trimmed_region.obsolete_roots.empty()) {
    std::vector<PageId> roots_to_trim;
    std::swap(roots_to_trim, trimmed_region.obsolete_roots);

    BATT_REQUIRE_OK(drop_roots(trim_event->trim_event_slot.lower_bound, as_slice(roots_to_trim)));
  }
  // ** IMPORTANT ** It is only safe to trim the log after `commit(PageCacheJob, ...)` returns;
  // otherwise we will lose information about which root set page references to remove!

  StatusOr<batt::Grant> trimmed_space = [&slot_writer, &trimmed_region] {
    BATT_DEBUG_INFO("[VolumeTrimmer] SlotWriter::trim_and_reserve("
                    << trimmed_region.slot_range.upper_bound << ")");
    return slot_writer.trim_and_reserve(trimmed_region.slot_range.upper_bound);
  }();
  BATT_REQUIRE_OK(trimmed_space);

  // Balance the books.
  //
  {
    // Because grant_size_to_reserve and grant_size_to_release are counted against each other, we
    // can cancel out the smaller of the two from both.
    //
    const usize common_size = std::min(trimmed_region.grant_size_to_reserve,  //
                                       trimmed_region.grant_size_to_release);

    trimmed_region.grant_size_to_reserve -= common_size;
    trimmed_region.grant_size_to_release -= common_size;
  }

  if (trimmed_region.grant_size_to_reserve > 0) {
    usize reserve_size = 0;
    std::swap(reserve_size, trimmed_region.grant_size_to_reserve);
    StatusOr<batt::Grant> reserved =
        trimmed_space->spend(reserve_size, batt::WaitForResource::kFalse);
    BATT_REQUIRE_OK(reserved) << BATT_INSPECT(reserve_size) << BATT_INSPECT(trimmed_space->size());

    grant.subsume(std::move(*reserved));
  }

  if (trimmed_region.grant_size_to_release > 0) {
    usize release_size = 0;
    std::swap(release_size, trimmed_region.grant_size_to_release);
    StatusOr<batt::Grant> released = grant.spend(release_size, batt::WaitForResource::kFalse);
    BATT_REQUIRE_OK(released);
  }

  // Move pending jobs from the trimmed region to `prior_pending_jobs`
  //
  prior_pending_jobs.insert(std::make_move_iterator(trimmed_region.pending_jobs.begin()),
                            std::make_move_iterator(trimmed_region.pending_jobs.end()));

  return batt::OkStatus();
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

/*explicit*/ VolumeTrimmer::RecoveryVisitor::RecoveryVisitor(slot_offset_type trim_pos) noexcept
    : log_trim_pos_{trim_pos}
{
  LLFS_VLOG(1) << "RecoveryVisitor created;" << BATT_INSPECT(trim_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_raw_data(const SlotParse&,
                                                   const Ref<const PackedRawData>&) /*override*/
{
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_prepare_job(
    const SlotParse& slot, const Ref<const PackedPrepareJob>& prepare) /*override*/
{
  const usize slot_size = packed_sizeof_slot(prepare.get());

  this->trimmer_grant_size_ += slot_size;

  LLFS_VLOG(1) << "RecoveryVisitor::on_prepare_job(slot=" << slot.offset << "); trimmer_grant_size "
               << (this->trimmer_grant_size_ - slot_size) << " -> " << this->trimmer_grant_size_;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_commit_job(const SlotParse& slot,
                                                     const PackedCommitJob& commit) /*override*/
{
  const usize slot_size = packed_sizeof_slot(commit);

  this->trimmer_grant_size_ += slot_size;

  LLFS_VLOG(1) << "RecoveryVisitor::on_commit_job(slot=" << slot.offset << "); trimmer_grant_size "
               << (this->trimmer_grant_size_ - slot_size) << " -> " << this->trimmer_grant_size_;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_rollback_job(const SlotParse&,
                                                       const PackedRollbackJob&) /*override*/
{
  // TODO [tastolfi 2022-11-23] Figure out whether we need to do anything here to avoid leaking log
  // grant...
  //
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_volume_attach(
    const SlotParse& slot, const PackedVolumeAttachEvent& attach) /*override*/
{
  this->refresh_info_.most_recent_attach_slot[attach.id] = slot.offset.lower_bound;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_volume_detach(
    const SlotParse& slot, const PackedVolumeDetachEvent& detach) /*override*/
{
  this->refresh_info_.most_recent_attach_slot[detach.id] = slot.offset.lower_bound;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_volume_ids(const SlotParse& slot,
                                                     const PackedVolumeIds&) /*override*/
{
  LLFS_VLOG(1) << "RecoveryVisitor::on_volume_ids(slot=" << slot.offset << ")";

  this->refresh_info_.most_recent_ids_slot = slot.offset.lower_bound;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_volume_recovered(
    const SlotParse&, const PackedVolumeRecovered&) /*override*/
{
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_volume_format_upgrade(
    const SlotParse&, const PackedVolumeFormatUpgrade&) /*override*/
{
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeTrimmer::RecoveryVisitor::on_volume_trim(
    const SlotParse& slot, const VolumeTrimEvent& trim_event) /*override*/
{
  const bool is_pending = slot_less_than(this->log_trim_pos_, trim_event.new_trim_pos);

  LLFS_VLOG(1) << "RecoveryVisitor::on_volume_trim(slot=" << slot.offset << ") trimmed_region == "
               << SlotRange{trim_event.old_trim_pos, trim_event.new_trim_pos}
               << BATT_INSPECT(is_pending);

  this->trimmer_grant_size_ += packed_sizeof_slot(trim_event);

  if (is_pending) {
    if (this->trim_event_info_ != None) {
      LLFS_LOG_WARNING() << "Multiple pending trim events found!  Likely corrupted log...";
      //
      // TODO [tastolfi 2022-11-28] Fail recovery?
    }

    this->trim_event_info_.emplace();
    this->trim_event_info_->trim_event_slot = slot.offset;
    this->trim_event_info_->trimmed_region_slot_range = SlotRange{
        trim_event.old_trim_pos,
        trim_event.new_trim_pos,
    };
  } else {
    // If the trim is pending (branch above), then the trimmer will rescan the trimmed region and
    // recover the information in the trim event.  Otherwise, we need to apply this information
    // here.
    //
    batt::make_copy(trim_event.trimmed_prepare_jobs) |
        batt::seq::for_each([this](const TrimmedPrepareJob& job) {
          auto page_ids = batt::make_copy(job.page_ids) | batt::seq::collect_vec();

          LLFS_VLOG(1) << " -- " << BATT_INSPECT(job.prepare_slot) << ","
                       << BATT_INSPECT_RANGE(page_ids);

          this->pending_jobs_.emplace(job.prepare_slot, std::move(page_ids));
        });

    batt::make_copy(trim_event.committed_jobs) |
        batt::seq::for_each([this](slot_offset_type prepare_slot) {
          LLFS_VLOG(1) << " -- commit{prepare_slot=" << prepare_slot << "}";
          this->pending_jobs_.erase(prepare_slot);
        });
  }

  return batt::OkStatus();
}

}  // namespace llfs
