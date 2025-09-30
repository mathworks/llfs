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
#include <llfs/volume_trimmed_region_visitor.hpp>
#include <llfs/volume_trimmer_recovery_visitor.hpp>

#include <batteries/stream_util.hpp>

namespace llfs {
namespace {

const usize kTrimEventGrantSize =
    packed_sizeof_slot_with_payload_size(sizeof(PackedVolumeTrimEvent));

}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ VolumeDropRootsFn VolumeTrimmer::make_default_drop_roots_fn(PageCache& cache,
                                                                       PageRecycler& recycler)
{
  return [&cache, &recycler](const boost::uuids::uuid& trimmer_uuid, slot_offset_type slot_offset,
                             Slice<const PageId> roots_to_trim) -> Status {
    // Create a job for the root set ref count updates.
    //
    std::unique_ptr<PageCacheJob> job = cache.new_job();

    LLFS_VLOG(1) << "Dropping PageId roots from the log..." << BATT_INSPECT(slot_offset)
                 << BATT_INSPECT(roots_to_trim.size());

    for (PageId page_id : roots_to_trim) {
      LLFS_VLOG(2) << " -- " << page_id;
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
/*static*/ StatusOr<std::unique_ptr<VolumeTrimmer>> VolumeTrimmer::recover(
    const boost::uuids::uuid& trimmer_uuid,            //
    std::string&& name,                                //
    TrimDelayByteCount trim_delay,                     //
    LogDevice& volume_root_log,                        //
    TypedSlotWriter<VolumeEventVariant>& slot_writer,  //
    VolumeDropRootsFn&& drop_roots,                    //
    SlotLockManager& trim_control,                     //
    VolumeMetadataRefresher& metadata_refresher)
{
  // Recover VolumeTrimmer state.  It is important that we do this only once all jobs have been
  // resolved and metadata (such as attachments) has been appended.
  //
  std::unique_ptr<LogDevice::Reader> log_reader =
      volume_root_log.new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable);

  VolumeTrimmerRecoveryVisitor trimmer_visitor{/*trim_pos=*/log_reader->slot_offset()};
  {
    TypedSlotReader<VolumeEventVariant> slot_reader{*log_reader};
    StatusOr<usize> slots_read = slot_reader.run(batt::WaitForResource::kFalse,
                                                 [&trimmer_visitor](auto&&... args) -> Status {
                                                   BATT_REQUIRE_OK(trimmer_visitor(args...));
                                                   return batt::OkStatus();
                                                 });
    BATT_REQUIRE_OK(slots_read);
  };

  Optional<VolumeTrimEventInfo> trim_event_info = trimmer_visitor.get_trim_event_info();
  if (!trim_event_info) {
    log_reader = volume_root_log.new_reader(
        /*slot_lower_bound=*/None, LogReadMode::kDurable);

  } else {
    LLFS_VLOG(1) << "[VolumeTrimmer::recover] resolving partial trim:"
                 << BATT_INSPECT(trim_event_info);

    // A pending VolumeTrimEventInfo record was recovered from the log.  Scan the trimmed region and
    // complete the trim before creating the VolumeTrimmer object.
    //
    log_reader = volume_root_log.new_reader(
        /*slot_lower_bound=*/trim_event_info->trimmed_region_slot_range.lower_bound,
        LogReadMode::kDurable);

    const slot_offset_type trim_upper_bound =
        trim_event_info->trimmed_region_slot_range.upper_bound;

    TypedSlotReader<VolumeEventVariant> slot_reader{*log_reader};

    // have_trim_event_grant arg is set to true because, evidently, we had a trim event grant when
    // this was written!
    //
    StatusOr<VolumeTrimmedRegionInfo> trimmed_region_info = read_trimmed_region(
        slot_reader, metadata_refresher, HaveTrimEventGrant{true}, trim_upper_bound);

    BATT_REQUIRE_OK(trimmed_region_info);

    //----- --- -- -  -  -   -
    // Metadata is refreshed *after* writing the trim event, so it has already been done!
    //----- --- -- -  -  -   -

    // Sanity check; the scanned slot range should be the same as it was when the trim event
    // record was written.
    //
    BATT_CHECK_EQ(trim_event_info->trimmed_region_slot_range, trimmed_region_info->slot_range);

    // Normally the grant passed to `trim_volume_log` would subsume any portion of the trimmed log
    // region necessary to pay for the next trim event, but since we're going to allocate a new
    // grant inside VolumeTrimmer::VolumeTrimmer anyhow, this is just a formality.
    //
    // We preallocate the grant here because the trim even was already written, prior to recovery.
    //
    batt::Grant grant =
        BATT_OK_RESULT_OR_PANIC(slot_writer.reserve(0, batt::WaitForResource::kFalse));

    // Finish the partially completed trim.
    //
    LLFS_VLOG(1) << "finishing partial trim...";
    BATT_REQUIRE_OK(trim_volume_log(trimmer_uuid, slot_writer, grant, std::move(*trim_event_info),
                                    std::move(*trimmed_region_info), metadata_refresher, drop_roots,
                                    batt::LogLevel::kWarning));
  }

  // The trim state is now clean!  Create and return the VolumeTrimmer.
  //
  LLFS_VLOG(1) << "creating the VolumeTrimmer object...";
  return std::make_unique<VolumeTrimmer>(trimmer_uuid, std::move(name), trim_control, trim_delay,
                                         std::move(log_reader), slot_writer, std::move(drop_roots),
                                         metadata_refresher);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeTrimmer::VolumeTrimmer(const boost::uuids::uuid& trimmer_uuid,            //
                                          std::string&& name,                                //
                                          SlotLockManager& trim_control,                     //
                                          TrimDelayByteCount trim_delay,                     //
                                          std::unique_ptr<LogDevice::Reader>&& log_reader,   //
                                          TypedSlotWriter<VolumeEventVariant>& slot_writer,  //
                                          VolumeDropRootsFn&& drop_roots_fn,                 //
                                          VolumeMetadataRefresher& metadata_refresher) noexcept
    : trimmer_uuid_{trimmer_uuid}
    , name_{std::move(name)}
    , trim_control_{trim_control}
    , trim_delay_{trim_delay}
    , log_reader_{std::move(log_reader)}
    , slot_reader_{*this->log_reader_}
    , slot_writer_{slot_writer}
    , drop_roots_{std::move(drop_roots_fn)}
    , trimmer_grant_{BATT_OK_RESULT_OR_PANIC(
          this->slot_writer_.reserve(0, batt::WaitForResource::kFalse))}
    , metadata_refresher_{metadata_refresher}
{
  LLFS_VLOG(1) << "VolumeTrimmer::VolumeTrimmer" << BATT_INSPECT_STR(this->name_);

  StatusOr<batt::Grant> init_grant = this->slot_writer_.reserve(
      std::min<usize>(this->slot_writer_.pool_size(), kTrimEventGrantSize),
      batt::WaitForResource::kFalse);

  BATT_CHECK_OK(init_grant) << BATT_INSPECT(kTrimEventGrantSize)
                            << BATT_INSPECT(this->slot_writer_.log_size())
                            << BATT_INSPECT(this->slot_writer_.log_capacity())
                            << BATT_INSPECT(this->slot_writer_.pool_size());

  this->trimmer_grant_.subsume(std::move(*init_grant));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
VolumeTrimmer::~VolumeTrimmer() noexcept
{
  this->halt();
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
    BATT_DEBUG_INFO(
        "waiting for trim target to advance; "
        << BATT_INSPECT(this->trim_control_.get_lower_bound()) << BATT_INSPECT(least_upper_bound)
        << BATT_INSPECT(bytes_trimmed) << BATT_INSPECT(trim_lower_bound) << std::endl
        << BATT_INSPECT(this->trim_control_.debug_info()) << BATT_INSPECT(loop_counter)
        << BATT_INSPECT(this->trim_control_.is_closed()) << BATT_INSPECT(this->trim_delay_));

    LLFS_VLOG(1) << "VolumeTrimmer::run > await_trim_target: " << BATT_INSPECT(least_upper_bound);
    StatusOr<slot_offset_type> trim_upper_bound = this->await_trim_target(least_upper_bound);

    BATT_REQUIRE_OK(trim_upper_bound) << this->error_log_level_.load();

    LLFS_VLOG(1) << BATT_INSPECT(trim_upper_bound) << BATT_INSPECT(this->trim_control_.is_closed())
                 << BATT_INSPECT_STR(this->name_) << BATT_INSPECT(least_upper_bound);

    // Whenever we get a new trim_upper_bound, always check first to see if we are shutting down. If
    // so, don't start a new trim, as this creates race conditions with the current/former holders
    // of trim locks.
    //
    if (this->trim_control_.is_closed()) {
      return OkStatus();
    }

    // The next time we wait for a new trim target, it should be at least one past the previously
    // observed offset.
    //
    least_upper_bound = *trim_upper_bound + 1;

    LLFS_VLOG(1) << "new value for " << BATT_INSPECT(*trim_upper_bound) << "; scanning log slots ["
                 << this->slot_reader_.next_slot_offset() << "," << (*trim_upper_bound) << ")"
                 << BATT_INSPECT(this->trimmer_grant_.size());

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Scan the trimmed region.
    //
    LLFS_VLOG(1) << "[VolumeTrimmer] read_trimmed_region";
    //
    StatusOr<VolumeTrimmedRegionInfo> trimmed_region_info = read_trimmed_region(
        this->slot_reader_, this->metadata_refresher_,
        HaveTrimEventGrant{this->trimmer_grant_.size() >= kTrimEventGrantSize}, *trim_upper_bound);

    BATT_REQUIRE_OK(trimmed_region_info);

    if (trimmed_region_info->slot_range.empty()) {
      LLFS_VLOG(1) << "Trimmed region too small; waiting for more to be trimmed,"
                   << BATT_INSPECT(least_upper_bound);
      continue;
    }

    Optional<VolumeTrimEventInfo> trim_event_info;

    {
      Optional<slot_offset_type> sync_point;

      // Invalidate the new trim pos with the metadata refresher, since it might make the difference
      // between writing a trim event and not.
      //
      LLFS_VLOG(1) << "[VolumeTrimmer] metadata_refresher.invalidate(" << *trim_upper_bound << ")"
                   << BATT_INSPECT(this->metadata_refresher_.grant_target())
                   << BATT_INSPECT(this->metadata_refresher_.grant_required())
                   << BATT_INSPECT(this->metadata_refresher_.grant_size());

      {
        BATT_DEBUG_INFO("Invalidating metadata");

        BATT_REQUIRE_OK(this->metadata_refresher_.invalidate(*trim_upper_bound))
            << this->error_log_level_.load();
      }

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      // Make sure all Volume metadata has been refreshed.
      //
      if (this->metadata_refresher_.needs_flush()) {
        Optional<SlotRange> metadata_slot_range;
        BATT_DEBUG_INFO("Refreshing metadata" << BATT_INSPECT(metadata_slot_range)
                                              << BATT_INSPECT(sync_point));

        StatusOr<SlotRange> metadata_slots = this->metadata_refresher_.flush();
        BATT_REQUIRE_OK(metadata_slots) << this->error_log_level_.load();
        metadata_slot_range = *metadata_slots;

        clamp_min_slot(&sync_point, metadata_slots->upper_bound);
      }

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      // Write a TrimEvent to the log if necessary.
      //  IMPORTANT: this must come BEFORE refreshing metadata so that we don't refresh metadata,
      //  crash, then forget there was a trim; this could exhaust available grant!
      //
      if (trimmed_region_info->requires_trim_event_slot()) {
        BATT_DEBUG_INFO("Writing TrimEvent");

        LLFS_VLOG(1) << "[VolumeTrimmer] write_trim_event";
        //
        BATT_ASSIGN_OK_RESULT(
            trim_event_info,
            write_trim_event(this->slot_writer_, this->trimmer_grant_, *trimmed_region_info));

        clamp_min_slot(&sync_point, trim_event_info->trim_event_slot.upper_bound);
      }

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      // Sync if necessary.
      //
      if (sync_point) {
        BATT_DEBUG_INFO("Flushing TrimEvent and metadata");

        LLFS_VLOG(1) << "Flushing trim event," << BATT_INSPECT(*sync_point) << ";"
                     << BATT_INSPECT(trimmed_region_info->slot_range);
        //
        BATT_REQUIRE_OK(
            this->slot_writer_.sync(LogReadMode::kDurable, SlotUpperBoundAt{*sync_point}))
            << this->error_log_level_.load();
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Trim the log.
    //
    BATT_DEBUG_INFO("Trimming: " << trimmed_region_info->slot_range);

    const slot_offset_type new_trim_target = trimmed_region_info->slot_range.upper_bound;

    BATT_DEBUG_INFO("VolumeTrimmer -> trim_volume_log;" << BATT_INSPECT(this->name_));

    LLFS_VLOG(1) << "[VolumeTrimmer] trim_volume_log";
    Status trim_result = trim_volume_log(this->trimmer_uuid_, this->slot_writer_,
                                         this->trimmer_grant_, std::move(trim_event_info),
                                         std::move(*trimmed_region_info), this->metadata_refresher_,
                                         this->drop_roots_, this->error_log_level_.load());

    this->trim_count_.fetch_add(1);

    BATT_REQUIRE_OK(trim_result) << this->error_log_level_.load();

    // Update the trim position and repeat the loop.
    //
    bytes_trimmed += (new_trim_target - trim_lower_bound);
    trim_lower_bound = new_trim_target;

    LLFS_VLOG(1) << "Trim(" << new_trim_target << ") is complete; awaiting new target ("
                 << BATT_INSPECT(least_upper_bound) << ")";
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> VolumeTrimmer::await_trim_target(slot_offset_type min_offset)
{
  StatusOr<slot_offset_type> trim_upper_bound =
      this->trim_control_.await_lower_bound(min_offset + this->trim_delay_);

  BATT_REQUIRE_OK(trim_upper_bound) << this->error_log_level_.load();

  const slot_offset_type new_trim_target = *trim_upper_bound - this->trim_delay_;

  BATT_CHECK(!slot_less_than(new_trim_target, min_offset));

  return new_trim_target;
}
//
// class VolumeTrimmer
//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<VolumeTrimEventInfo> write_trim_event(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                               batt::Grant& grant,
                                               const VolumeTrimmedRegionInfo& trimmed_region)
{
  VolumeTrimEvent trim_event;

  trim_event.old_trim_pos = trimmed_region.slot_range.lower_bound;
  trim_event.new_trim_pos = trimmed_region.slot_range.upper_bound;

  const u64 trim_slot_size = packed_sizeof_slot(trim_event);

  StatusOr<batt::Grant> event_grant = grant.spend(trim_slot_size, batt::WaitForResource::kFalse);
  BATT_REQUIRE_OK(event_grant) << BATT_INSPECT(trim_slot_size) << BATT_INSPECT(grant.size());

  const u64 event_grant_size_before = event_grant->size();
  //
  StatusOr<SlotRange> result = slot_writer.append(*event_grant, std::move(trim_event));
  BATT_REQUIRE_OK(result);
  //
  const u64 event_grant_size_after = event_grant->size();
  BATT_CHECK_EQ(event_grant_size_before - event_grant_size_after, trim_slot_size);

  return VolumeTrimEventInfo{
      .trim_event_slot = *result,
      .trimmed_region_slot_range = trimmed_region.slot_range,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status trim_volume_log(const boost::uuids::uuid& trimmer_uuid,
                       TypedSlotWriter<VolumeEventVariant>& slot_writer, batt::Grant& grant,
                       Optional<VolumeTrimEventInfo>&& trim_event,
                       VolumeTrimmedRegionInfo&& trimmed_region,
                       VolumeMetadataRefresher& metadata_refresher,
                       const VolumeDropRootsFn& drop_roots, batt::LogLevel error_log_level)
{
  LLFS_VLOG(1) << "trim_volume_log()" << BATT_INSPECT(trimmed_region.slot_range)
               << BATT_INSPECT(trim_event);

  auto on_scope_exit = batt::finally([&] {
    BATT_CHECK_LE(grant.size(), kTrimEventGrantSize);
  });

  // If we found some obsolete jobs in the newly trimmed log segment, then collect up all root set
  // ref counts to release.
  //
  if (!trimmed_region.obsolete_roots.empty()) {
    BATT_CHECK(trim_event);

    std::vector<PageId> roots_to_trim;
    std::swap(roots_to_trim, trimmed_region.obsolete_roots);
    BATT_REQUIRE_OK(
        drop_roots(trimmer_uuid, trim_event->trim_event_slot.lower_bound, as_slice(roots_to_trim)))
        << error_log_level;
  }
  // ** IMPORTANT ** It is only safe to trim the log after `commit(PageCacheJob, ...)` (which is
  // called inside `drop_roots`) returns; otherwise we will lose information about which root set
  // page references to remove!

  const usize in_use_size = slot_writer.in_use_size();

  StatusOr<batt::Grant> trimmed_space = [&slot_writer, &trimmed_region] {
    BATT_DEBUG_INFO("[VolumeTrimmer] SlotWriter::trim_and_reserve("
                    << trimmed_region.slot_range.upper_bound << ")");

    return slot_writer.trim_and_reserve(trimmed_region.slot_range.upper_bound);
  }();
  BATT_REQUIRE_OK(trimmed_space) << error_log_level << BATT_INSPECT(in_use_size)
                                 << BATT_INSPECT(trimmed_region.slot_range);

  // Take some of the trimmed space and retain it to cover the trim event that was just written.
  //
  if (trim_event && grant.size() < kTrimEventGrantSize) {
    StatusOr<batt::Grant> trim_event_grant =
        trimmed_space->spend(kTrimEventGrantSize - grant.size(), batt::WaitForResource::kFalse);

    BATT_REQUIRE_OK(trim_event_grant)
        << error_log_level << BATT_INSPECT(slot_writer.log_size())
        << BATT_INSPECT(slot_writer.log_capacity()) << BATT_INSPECT(slot_writer.pool_size())
        << BATT_INSPECT(grant.size());

    grant.subsume(std::move(*trim_event_grant));
  }

  if (grant.size() >= kTrimEventGrantSize) {
    // Use as much of the trimmed space as necessary to restore the metadata refresher's grant.
    //
    LLFS_VLOG(1) << "trim_volume_log(): updating metadata refresher grant;"
                 << BATT_INSPECT(metadata_refresher.grant_required())
                 << BATT_INSPECT(trimmed_space->size());

    BATT_REQUIRE_OK(metadata_refresher.update_grant_partial(*trimmed_space)) << error_log_level;
  }

  return batt::OkStatus();
}

}  // namespace llfs
