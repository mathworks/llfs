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
#include <llfs/volume_trimmer_recovery_visitor.hpp>

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
/*explicit*/ VolumeTrimmer::VolumeTrimmer(
    const boost::uuids::uuid& trimmer_uuid, std::string&& name, SlotLockManager& trim_control,
    TrimDelayByteCount trim_delay, std::unique_ptr<LogDevice::Reader>&& log_reader,
    TypedSlotWriter<VolumeEventVariant>& slot_writer, VolumeDropRootsFn&& drop_roots,
    const VolumeTrimmerRecoveryVisitor& recovery_visitor) noexcept
    : trimmer_uuid_{trimmer_uuid}
    , name_{std::move(name)}
    , trim_control_{trim_control}
    , trim_delay_{trim_delay}
    , log_reader_{std::move(log_reader)}
    , slot_reader_{*this->log_reader_}
    , slot_writer_{slot_writer}
    , drop_roots_{std::move(drop_roots)}
    , trimmer_grant_{BATT_OK_RESULT_OR_PANIC(
          this->slot_writer_.reserve(0, batt::WaitForResource::kFalse))}
    , pending_jobs_{recovery_visitor.get_pending_jobs()}
    , refresh_info_{recovery_visitor.get_refresh_info()}
    , latest_trim_event_{recovery_visitor.get_trim_event_info()}
    , reserve_from_next_trim_{recovery_visitor.get_reserve_from_pending_trim()}
    , pushed_grant_size_{trimmer_grant_.size()}
    , popped_grant_size_{0}
{
  StatusOr<batt::Grant> initial_grant = this->slot_writer_.reserve(
      recovery_visitor.get_trimmer_grant_size(), batt::WaitForResource::kFalse);

  BATT_CHECK_OK(initial_grant) << BATT_INSPECT(slot_writer.pool_size())
                               << BATT_INSPECT(recovery_visitor.get_trimmer_grant_size())
                               << BATT_INSPECT(recovery_visitor.get_reserve_from_pending_trim())
                               << BATT_INSPECT(this->latest_trim_event_)
                               << recovery_visitor.debug_info();

  this->trimmer_grant_.subsume(std::move(*initial_grant));
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
void VolumeTrimmer::push_grant(batt::Grant&& grant) noexcept
{
  LLFS_VLOG(2) << "trimmer grant: " << this->trimmer_grant_.size() << " -> "
               << (this->trimmer_grant_.size() + grant.size());

  this->pushed_grant_size_.fetch_add(grant.size());

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

    StatusOr<slot_offset_type> trim_upper_bound = this->await_trim_target(least_upper_bound);

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
    StatusOr<VolumeTrimmedRegionInfo> trimmed_region_info = read_trimmed_region(
        this->slot_reader_, *trim_upper_bound, this->pending_jobs_, this->popped_grant_size_);

    BATT_REQUIRE_OK(trimmed_region_info);

    LLFS_VLOG(1) << BATT_INSPECT(trimmed_region_info->grant_size_to_release)
                 << BATT_INSPECT(trimmed_region_info->grant_size_to_reserve)
                 << BATT_INSPECT(this->reserve_from_next_trim_);

    if (trimmed_region_info->slot_range.empty()) {
      LLFS_VLOG(1) << "Trimmed region too small; waiting for more to be trimmed,"
                   << BATT_INSPECT(least_upper_bound);
      continue;
    }
    trimmed_region_info->grant_size_to_reserve += this->reserve_from_next_trim_;
    this->reserve_from_next_trim_ = 0;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Make sure all Volume metadata has been refreshed.
    //
    BATT_REQUIRE_OK(refresh_volume_metadata(this->slot_writer_, this->trimmer_grant_,
                                            this->refresh_info_, *trimmed_region_info));

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Write a TrimEvent to the log if necessary.
    //
    if (trimmed_region_info->requires_trim_event_slot() && !this->latest_trim_event_) {
      BATT_ASSIGN_OK_RESULT(
          this->latest_trim_event_,
          write_trim_event(this->slot_writer_, this->trimmer_grant_, *trimmed_region_info));
      BATT_CHECK(this->latest_trim_event_);
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Trim the log.
    //
    const slot_offset_type new_trim_target = trimmed_region_info->slot_range.upper_bound;

    BATT_DEBUG_INFO("VolumeTrimmer -> trim_volume_log;" << BATT_INSPECT(this->name_));

    Status trim_result = trim_volume_log(
        this->slot_writer_, this->trimmer_grant_, std::move(this->latest_trim_event_),
        std::move(*trimmed_region_info), this->drop_roots_, this->pending_jobs_);

    this->trim_count_.fetch_add(1);
    this->latest_trim_event_ = batt::None;

    BATT_REQUIRE_OK(trim_result);

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

  BATT_REQUIRE_OK(trim_upper_bound);

  const slot_offset_type new_trim_target = *trim_upper_bound - this->trim_delay_;

  BATT_CHECK(!slot_less_than(new_trim_target, min_offset));

  return new_trim_target;
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace {

struct TrimmedRegionStats {
  usize prepare_job_count = 0;
  usize commit_job_count = 0;
  usize rollback_job_count = 0;
  usize volume_ids_count = 0;
  usize volume_attach_count = 0;
  usize volume_detach_count = 0;
  usize volume_trim_count = 0;
};

inline std::ostream& operator<<(std::ostream& out, const TrimmedRegionStats& t)
{
  return out << "TrimmedRegionStats"                                //
             << "{.prepare_job_count=" << t.prepare_job_count       //
             << ", .commit_job_count=" << t.commit_job_count        //
             << ", .rollback_job_count=" << t.rollback_job_count    //
             << ", .volume_ids_count=" << t.volume_ids_count        //
             << ", .volume_attach_count=" << t.volume_attach_count  //
             << ", .volume_detach_count=" << t.volume_detach_count  //
             << ", .volume_trim_count=" << t.volume_trim_count      //
             << ",}";
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// The main functions of VolumeTrimmer per se are:
//
//  1. To ensure that Volume metadata such as attachments to PageDevice instances and the UUID list
//     of the various components of the Volume itself (namely, the Volume "root log" LogDevice, the
//     VolumeTrimmer, and the PageRecycler) are preserved when the log is trimmed.
//
//  2. To participate in the lifetime management of reference-counted Pages by decrementing the ref
//     counts of any "root_page_ids" in the trimmed region (e.g., the trimmed region contains the
//     record of a new page being added, or an existing page being newly referenced; since these
//     references to said pages will no longer be accessible once the log is trimmed, the ref
//     counts should be reversed so as to prevent "leaks," broadly defined as allocated pages which
//     are unreachable through a transitive path beginning in a Volume root log -- according to this
//     definition, hopefully the rationale for calling the Volume's log the "root log" becomes
//     evident).
//
// Notice that both of these functions depend on explicit knowledge of the contents of a region to
// be trimmed; thus we must scan the interval between the current trim offset and the target offset
// to collect this knowledge and act upon it.  This is the purpose of `read_trimmed_region`.
//
// The first parameter `slot_reader` provides basic access to the data to be scanned.
//
// The second parameter `trim_upper_bound` defines the offset at which to stop scanning.
//
// Implementing function (2.) above means correctly interpreting the effect of PageCacheJob
// transactions recorded in the log as a series of PackedPrepareJob and PackedCommitJob (or
// PackedRollbackJob) event pairs.  In general, there is no guarantee that trimmed regions do not
// bisect such pairs of events in the log (nor, in fact, that a transactional event pair straddles
// only a single trim boundary): for example, the prepare event for a transaction may appear in one
// trimmed region and its corresponding commit may appear several trimmed regions later.  Further,
// both events are necessary in order for the VolumeTrimmer to take correct action wrt updating (or
// not) ref counts.
//
// The third parameter `prior_pending_jobs` tracks the set of jobs whose prepare event (which,
// importantly, contains the actual ref count update information) has been trimmed at some point in
// the past.
//
// The fourth and final paramater `job_slots_byte_count` is for diagnostic purposes, and will make
// sense in the context of the detailed explanation of what read_trimmed_region does below.
//
// Each time a trim is performed, a PackedVolumeTrimEvent is appended to the log.  In this event, of
// all the `root_page_ids` from before the trim point which appear in some PackedPrepareJob without
// a corresponding PackedCommitJob (yet) are saved so that when we do finally see the commits which
// resolve the jobs, we can release the appropriate page refs and possibly garbage collect dead
// pages.
//
// An important detail here is that a PackedVolumeTrimEvent must be appended _before_ the actual
// trim occurs, since once we trim, there is no way to recover the crucial information this event
// contains.  Therefore, even though we are about to reclaim a bunch of log space that can be used
// to write the trim event, we can't rely on reclaimed (trimmed) space to reserve the Grant needed
// for this append.  Instead, any Grant the PackedVolumeTrimEvent might need must be reserved
// beforehand.
//
// To solve this dilemma, Volume requires 2x the actual slot append size in order to append a
// transactional page job.  This is reflected in `Volume::calcaulte_grant_size(AppendableJob)` and
// `Volume::append(AppendableJob, ...)`.  Inside append, half of the passed Grant is used to do the
// actual slot writing, and half is sent off to the VolumeTrimmer, where it sits in a pool until we
// can be absolutely certain it is no longer needed.
//
// This same idea is used for the PackedVolumeTrimEvent itself; we "reserve" twice the amount of
// grant needed to append the trim event, once for the immediate append, and once to refresh the
// information it contains should the trim event itself be trimmed while jobs are still pending.
//
// The trim algorithm is:
//
//  1. Await a new trim target
//  2. Read up to the new trim target, gathering:
//     a. The number of pre-reserved grant bytes to be released after the trim is completed
//     b. The set of (root) page ids whose ref count to decrement
//  3. Refresh any attachments or volume uuids that would otherwise be lost in the trimmed region
//     (i.e., any such info that doesn't appear at some later offset in the log)
//  4. Write the trim event
//  5. Trim the log to reclaim Grant for the trimmed region
//  6. Release the portion of Grant that isn't needed for some future refresh
//
// Step (6.) requires an accounting of "necessary Grant" count; this is achieved via two fields of
// the VolumeTrimmedRegionInfo struct:
//  - grant_size_to_release
//  - grant_size_to_reserve
//
// The rules for correct update and usage of these are simple:
//
//  - When appending some event slot that may need to be refreshed on trim, always reserve 2x the
//    necessary grant (packed slot size); when such slots are first appended, we reserve directly
//    from the SlotWriter, using half and pushing the other half to the VolumeTrimmer; when we
//    refresh during a trim, we "reserve" by incrementing the `grant_size_to_reserve` field.
//  - When scanning the log before a trim (Step 2. above), we "release" the extra grant by adding
//    the slot size of certain record types (prepare, commit, trim, attachments, and uuids) to the
//    `grant_size_to_release` field.
//
// The process follows a principle similar to swinging from vine to vine, always making sure the
// next vine is in hand before releasing the previous one.  This way, we make sure we always have
// enough Grant, when we need it (Honor Necessity), aggressively releasing any extra as soon as we
// can without violating this principle (Honor Sufficiency).
//
StatusOr<VolumeTrimmedRegionInfo> read_trimmed_region(
    TypedSlotReader<VolumeEventVariant>& slot_reader, slot_offset_type trim_upper_bound,
    VolumePendingJobsUMap& prior_pending_jobs, std::atomic<u64>& popped_grant_size)
{
  StatusOr<VolumeTrimmedRegionInfo> result = VolumeTrimmedRegionInfo{};

  result->slot_range.lower_bound = slot_reader.next_slot_offset();
  result->slot_range.upper_bound = result->slot_range.lower_bound;

  LLFS_VLOG(2) << BATT_INSPECT(result->grant_size_to_release)
               << BATT_INSPECT(result->grant_size_to_reserve);

  TrimmedRegionStats stats;

  StatusOr<usize> read_status = slot_reader.run(
      batt::WaitForResource::kTrue,
      [trim_upper_bound, &result, &prior_pending_jobs, &stats, &popped_grant_size](
          const SlotParse& slot, const auto& payload) -> Status {
        const SlotRange& slot_range = slot.offset;
        const u64 slot_size = slot.size_in_bytes();

        auto& metrics = VolumeTrimmer::metrics();

        LLFS_VLOG(2) << "read slot: " << BATT_INSPECT(slot_range) << BATT_INSPECT(slot_size)
                     << BATT_INSPECT(!slot_less_than(slot_range.lower_bound, trim_upper_bound))
                     << BATT_INSPECT(slot_less_than(trim_upper_bound, slot_range.upper_bound));

        if (!slot_less_than(slot_range.lower_bound, trim_upper_bound) ||
            slot_less_than(trim_upper_bound, slot_range.upper_bound)) {
          return ::llfs::make_status(StatusCode::kBreakSlotReaderLoop);
        }

        LLFS_VLOG(2) << "visiting slot...";

        result->slot_range.upper_bound =
            slot_max(result->slot_range.upper_bound, slot_range.upper_bound);

        // Use batt::make_case_of_visitor because the alternative, batt::case_of, requires a
        // std::variant, which has already been unwrapped by the time we are inside the outer
        // lambda.
        //
        return batt::make_case_of_visitor(
            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const Ref<const PackedPrepareJob>& prepare) {
              ++stats.prepare_job_count;

              result->grant_size_to_release += slot_size;
              metrics.prepare_grant_released_byte_count.add(slot_size);
              popped_grant_size.fetch_add(slot_size);

              std::vector<PageId> root_page_ids =
                  as_seq(*prepare.get().root_page_ids) |
                  seq::map([](const PackedPageId& packed) -> PageId {
                    return packed.as_page_id();
                  }) |
                  seq::collect_vec();

              LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset)
                           << ", PrepareJob) root_page_ids=" << batt::dump_range(root_page_ids)
                           << BATT_INSPECT(slot_size);

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
            [&](const PackedCommitJob& commit) {
              ++stats.commit_job_count;

              result->grant_size_to_release += slot_size;
              metrics.commit_grant_released_byte_count.add(slot_size);
              popped_grant_size.fetch_add(slot_size);

              LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", CommitJob)"
                           << BATT_INSPECT(slot_size);

              //----- --- -- -  -  -   -
              // Define a local helper function to resolve pending jobs, because we don't know a
              // priori whether the prepare for a given commit/rollback will be found in the current
              // region (result->pending_jobs) or in a previous one (prior_pending_jobs).
              //
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
            [&](const PackedRollbackJob& rollback) {
              ++stats.rollback_job_count;

              result->grant_size_to_release += slot_size;
              popped_grant_size.fetch_add(slot_size);

              // The job has been resolved (rolled back); remove it from the maps.
              //
              LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", RollbackJob)"
                           << BATT_INSPECT(rollback.prepare_slot) << BATT_INSPECT(slot_size);

              if (prior_pending_jobs.erase(rollback.prepare_slot) == 1u) {
                result->resolved_jobs.emplace_back(rollback.prepare_slot);
              }
              result->pending_jobs.erase(rollback.prepare_slot);

              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const PackedVolumeIds& ids) {
              ++stats.volume_ids_count;

              metrics.ids_grant_released_byte_count.add(slot_size);
              result->grant_size_to_release += slot_size;
              popped_grant_size.fetch_add(slot_size);

              LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", PackedVolumeIds)"
                           << ids << BATT_INSPECT(slot_size);

              result->ids_to_refresh = ids;
              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const PackedVolumeAttachEvent& attach) {
              ++stats.volume_attach_count;

              result->grant_size_to_release += slot_size;
              metrics.attach_grant_released_byte_count.add(slot_size);
              popped_grant_size.fetch_add(slot_size);

              LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", AttachEvent)"
                           << BATT_INSPECT(slot_size);

              const auto& [iter, inserted] =
                  result->attachments_to_refresh.emplace(attach.base.id, attach);
              if (!inserted) {
                BATT_CHECK_EQ(iter->second.base.user_slot_offset, attach.base.user_slot_offset);
              }
              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const PackedVolumeDetachEvent& detach) {
              ++stats.volume_detach_count;

              result->attachments_to_refresh.erase(detach.id);
              return batt::OkStatus();
            },

            //+++++++++++-+-+--+----- --- -- -  -  -   -
            //
            [&](const VolumeTrimEvent& trim_event) {
              ++stats.volume_trim_count;

              metrics.trim_grant_released_byte_count.add(slot_size);
              result->grant_size_to_release += slot_size;
              //
              // Don't update popped_grant_size here because Grant for trim events isn't acquired
              // via VolumeTrimmer::push_grant.

              LLFS_VLOG(2) << "visit_slot(" << BATT_INSPECT(slot.offset) << ", TrimEvent)"
                           << BATT_INSPECT(slot_size);

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
            [](const auto& /*payload*/) {
              return batt::OkStatus();
            }
            //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
            )(payload);
      });

  const usize dropped_roots = result.ok() ? result->obsolete_roots.size() : usize{0};

  LLFS_VLOG(1) << "read_trimmed_region: done visiting slots," << BATT_INSPECT(read_status)
               << BATT_INSPECT(dropped_roots) << BATT_INSPECT(stats);

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

    const usize id_slot_size = packed_sizeof_slot(*trimmed_region.ids_to_refresh);
    trimmed_region.grant_size_to_reserve += id_slot_size * 2;

    LLFS_VLOG(1) << "(ids) grant_size_to_reserve: "
                 << (trimmed_region.grant_size_to_reserve - id_slot_size * 2) << " -> "
                 << trimmed_region.grant_size_to_reserve << " (+" << id_slot_size * 2 << ")";

    const slot_offset_type last_refresh_slot =
        refresh_info.most_recent_ids_slot.value_or(trimmed_region.slot_range.lower_bound);

    if (slot_less_than(last_refresh_slot, trimmed_region.slot_range.upper_bound)) {
      trimmed_region.ids_to_refresh->trim_slot_offset = trimmed_region.slot_range.lower_bound;

      StatusOr<SlotRange> id_refresh_slot =
          slot_writer.append(grant, *trimmed_region.ids_to_refresh);

      BATT_REQUIRE_OK(id_refresh_slot);

      VolumeTrimmer::metrics().ids_grant_reserved_byte_count.add(id_slot_size);

      refresh_info.most_recent_ids_slot = id_refresh_slot->lower_bound;
      clamp_min_slot(&sync_point, id_refresh_slot->upper_bound);
    }

    LLFS_VLOG(1) << " -- " << BATT_INSPECT(sync_point);
  }

  // Refresh any attachments that will be lost in this trim.
  //
  for (auto& [attach_id, event] : trimmed_region.attachments_to_refresh) {
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

    event.trim_slot_offset = trimmed_region.slot_range.lower_bound;

    const usize attach_slot_size = packed_sizeof_slot(event);
    trimmed_region.grant_size_to_reserve += attach_slot_size * 2;

    LLFS_VLOG(1) << "(attach) grant_size_to_reserve: "
                 << (trimmed_region.grant_size_to_reserve - attach_slot_size * 2) << " -> "
                 << trimmed_region.grant_size_to_reserve << " (+" << attach_slot_size * 2 << ")";

    LLFS_VLOG(1) << "Refreshing attachment " << attach_id;

    StatusOr<SlotRange> slot_range = slot_writer.append(grant, event);
    BATT_REQUIRE_OK(slot_range);

    VolumeTrimmer::metrics().attach_grant_reserved_byte_count.add(attach_slot_size);

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
                                               VolumeTrimmedRegionInfo& trimmed_region)
{
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

  // Retain enough recovered grant from the trim to cover the trim event we're about to append *and*
  // a future trim event where we pull forward all still-pending job info.
  //
  const u64 trim_slot_size = packed_sizeof_slot(event);
  trimmed_region.grant_size_to_reserve += trim_slot_size * 2;

  LLFS_VLOG(1) << "(trim) grant_size_to_reserve: "
               << (trimmed_region.grant_size_to_reserve - trim_slot_size * 2) << " -> "
               << trimmed_region.grant_size_to_reserve << " (+" << trim_slot_size * 2 << ")"
               << BATT_INSPECT(grant.size());

  StatusOr<batt::Grant> event_grant = grant.spend(trim_slot_size, batt::WaitForResource::kFalse);
  BATT_REQUIRE_OK(event_grant);

  const u64 event_grant_size_before = event_grant->size();
  //
  StatusOr<SlotRange> result = slot_writer.append(*event_grant, std::move(event));
  BATT_REQUIRE_OK(result);
  //
  const u64 event_grant_size_after = event_grant->size();
  BATT_CHECK_EQ(event_grant_size_before - event_grant_size_after, trim_slot_size);

  VolumeTrimmer::metrics().trim_grant_reserved_byte_count.add(trim_slot_size);

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
  const u64 grant_size_before = grant.size();

  BATT_CHECK_IMPLIES(!trim_event, trimmed_region.obsolete_roots.empty());

  // If we found some obsolete jobs in the newly trimmed log segment, then collect up all root set
  // ref counts to release.
  //
  if (!trimmed_region.obsolete_roots.empty()) {
    std::vector<PageId> roots_to_trim;
    std::swap(roots_to_trim, trimmed_region.obsolete_roots);

    LLFS_VLOG(1) << "trim_volume_log()" << BATT_INSPECT(trimmed_region.slot_range);

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

  trimmed_region.grant_size_to_release += trimmed_space->size();
  grant.subsume(std::move(*trimmed_space));

  // Balance the books.
  //
  BATT_CHECK_LE(trimmed_region.grant_size_to_reserve, trimmed_region.grant_size_to_release);
  if (trimmed_region.grant_size_to_reserve > trimmed_region.grant_size_to_release) {
    trimmed_region.grant_size_to_reserve -= trimmed_region.grant_size_to_release;
    trimmed_region.grant_size_to_release = 0;
  } else {
    trimmed_region.grant_size_to_release -= trimmed_region.grant_size_to_reserve;
    trimmed_region.grant_size_to_reserve = 0;

    if (trimmed_region.grant_size_to_release > 0) {
      usize release_size = 0;
      std::swap(release_size, trimmed_region.grant_size_to_release);
      StatusOr<batt::Grant> released = grant.spend(release_size, batt::WaitForResource::kFalse);
      BATT_REQUIRE_OK(released);
      LLFS_VLOG(1) << "released trimmer grant;" << BATT_INSPECT(release_size)
                   << BATT_INSPECT(grant_size_before);
    }
  }

  // Move pending jobs from the trimmed region to `prior_pending_jobs`
  //
  prior_pending_jobs.insert(std::make_move_iterator(trimmed_region.pending_jobs.begin()),
                            std::make_move_iterator(trimmed_region.pending_jobs.end()));

  return batt::OkStatus();
}

}  // namespace llfs
