//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume.hpp>
//

#include <llfs/pack_as_raw.hpp>
#include <llfs/volume_reader.hpp>
#include <llfs/volume_recovery_visitor.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const VolumeOptions& Volume::options() const
{
  return this->options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const boost::uuids::uuid& Volume::get_volume_uuid() const
{
  return this->volume_uuid_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const boost::uuids::uuid& Volume::get_recycler_uuid() const
{
  return this->recycler_->uuid();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const boost::uuids::uuid& Volume::get_trimmer_uuid() const
{
  return this->trimmer_.uuid();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 Volume::calculate_grant_size(const PackableRef& payload) const
{
  return packed_sizeof_slot(payload);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 Volume::calculate_grant_size(const std::string_view& payload) const
{
  // We must use `pack_as_raw` here so that when/if the payload is passed to a
  // slot visitor, it will not include a PackedBytes header; rather it should be
  // exactly the same bytes as `payload`.
  //
  return packed_sizeof_slot(pack_as_raw(payload));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 Volume::calculate_grant_size(const AppendableJob& appendable) const
{
  return (packed_sizeof_slot(prepare(appendable)) +
          packed_sizeof_slot(batt::StaticType<PackedCommitJob>{}))

         // We double the grant size to reserve log space to save the list of pages (and the record
         // of a job being committed) in TrimEvent slots, should prepare and commit be split across
         // two trimmed regions.
         //
         * 2;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto Volume::recover(VolumeRecoverParams&& params,
                                const VolumeReader::SlotVisitorFn& slot_visitor_fn)
    -> StatusOr<std::unique_ptr<Volume>>
{
  batt::TaskScheduler& scheduler = *params.scheduler;
  const VolumeOptions& volume_options = params.options;
  const auto recycler_options = PageRecyclerOptions{}.  //
                                set_max_refs_per_page(volume_options.max_refs_per_page);
  batt::SharedPtr<PageCache> cache = params.cache;
  LogDeviceFactory& root_log_factory = *params.root_log_factory;
  LogDeviceFactory& recycler_log_factory = *params.recycler_log_factory;

  if (!params.trim_control) {
    params.trim_control = std::make_shared<SlotLockManager>();
  }

  auto page_deleter = std::make_unique<PageCache::PageDeleterImpl>(*cache);

  BATT_ASSIGN_OK_RESULT(
      std::unique_ptr<PageRecycler> recycler,
      PageRecycler::recover(scheduler, volume_options.name + "_PageRecycler", recycler_options,
                            *page_deleter, recycler_log_factory));

  VolumePendingJobsMap pending_jobs;
  VolumeRecoveryVisitor visitor{batt::make_copy(slot_visitor_fn), pending_jobs};
  Optional<VolumeTrimmer::RecoveryVisitor> trimmer_visitor;

  // Open the log device and scan all slots.
  //
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<LogDevice> root_log,
                        root_log_factory.open_log_device(
                            [&](LogDevice::Reader& log_reader) -> StatusOr<slot_offset_type> {
                              trimmer_visitor.emplace(/*trim_pos=*/log_reader.slot_offset());

                              TypedSlotReader<VolumeEventVariant> slot_reader{log_reader};

                              StatusOr<usize> slots_read = slot_reader.run(
                                  batt::WaitForResource::kFalse,
                                  [&visitor, &trimmer_visitor](auto&&... args) -> Status {
                                    BATT_REQUIRE_OK(visitor(args...));
                                    BATT_REQUIRE_OK((*trimmer_visitor)(args...));

                                    return batt::OkStatus();
                                  });
                              BATT_UNTESTED_COND(!slots_read.ok());
                              BATT_REQUIRE_OK(slots_read);

                              return log_reader.slot_offset();
                            }));

  // The amount to allocate to the trimmer to refresh all metadata we append to the log below.
  //
  usize trimmer_grant_size = 0;

  // Put the main log in a clean state.  This means all configuration data must
  // be recorded, device attachments created, and pending jobs resolved.
  {
    TypedSlotWriter<VolumeEventVariant> slot_writer{*root_log};
    batt::Grant grant = BATT_OK_RESULT_OR_PANIC(
        slot_writer.reserve(slot_writer.pool_size(), batt::WaitForResource::kFalse));

    // If no uuids were found while opening the log, create them now.
    //
    if (!visitor.ids) {
      LLFS_VLOG(1) << "Initializing Volume uuids for the first time";

      visitor.ids.emplace(SlotWithPayload<PackedVolumeIds>{
          .slot_range = {0, 1},
          .payload =
              {
                  .main_uuid = volume_options.uuid.value_or(boost::uuids::random_generator{}()),
                  .recycler_uuid = recycler->uuid(),
                  .trimmer_uuid = boost::uuids::random_generator{}(),
              },
      });

      StatusOr<SlotRange> ids_slot = slot_writer.append(grant, visitor.ids->payload);

      BATT_UNTESTED_COND(!ids_slot.ok());
      BATT_REQUIRE_OK(ids_slot);

      Status flush_status =
          slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{ids_slot->upper_bound});

      BATT_UNTESTED_COND(!flush_status.ok());
      BATT_REQUIRE_OK(flush_status);
    }
    LLFS_VLOG(1) << BATT_INSPECT(visitor.ids->payload);

    // Attach the main uuid, recycler uuid, and trimmer uuid to each device in
    // the cache storage pool.
    //
    {
      // Loop through all combinations of uuid, device_id.
      //
      LLFS_VLOG(1) << "Recovered attachments: " << batt::dump_range(visitor.device_attachments);

      for (const auto& uuid : {
               visitor.ids->payload.main_uuid,
               visitor.ids->payload.recycler_uuid,
               visitor.ids->payload.trimmer_uuid,
           }) {
        for (const PageArena& arena : cache->all_arenas()) {
          Optional<PageAllocatorAttachmentStatus> attachment =
              arena.allocator().get_client_attachment_status(uuid);

          // Find the lowest available slot offset for the log associated with `uuid`.
          //
          const slot_offset_type next_available_slot_offset = [&] {
            if (uuid == visitor.ids->payload.recycler_uuid) {
              return recycler->slot_upper_bound(LogReadMode::kDurable);
            } else {
              // Both the volume (main) and trimmer share the same WAL (the main log).
              //
              return slot_writer.slot_offset();
            }
          }();

          auto attach_event = PackedVolumeAttachEvent{{
              .id =
                  VolumeAttachmentId{
                      .client = uuid,
                      .device = arena.device().get_id(),
                  },
              .user_slot_offset = next_available_slot_offset,
          }};

          trimmer_grant_size += packed_sizeof_slot(attach_event);

          // If already attached, then nothing to do; continue.
          //
          if (attachment || visitor.device_attachments.count(attach_event.id)) {
            continue;
          }

          LLFS_VLOG(1) << "[Volume::recover] attaching client " << uuid << " to device "
                       << arena.device().get_id() << BATT_INSPECT(next_available_slot_offset);

          StatusOr<slot_offset_type> sync_slot =
              arena.allocator().attach_user(uuid, /*user_slot=*/next_available_slot_offset);

          BATT_UNTESTED_COND(!sync_slot.ok());
          BATT_REQUIRE_OK(sync_slot);

          Status sync_status = arena.allocator().sync(*sync_slot);

          BATT_UNTESTED_COND(!sync_status.ok());
          BATT_REQUIRE_OK(sync_status);

          StatusOr<SlotRange> ids_slot = slot_writer.append(grant, attach_event);

          BATT_UNTESTED_COND(!ids_slot.ok());
          BATT_REQUIRE_OK(ids_slot);

          Status flush_status =
              slot_writer.sync(LogReadMode::kDurable, SlotUpperBoundAt{ids_slot->upper_bound});

          BATT_UNTESTED_COND(!flush_status.ok());
          BATT_REQUIRE_OK(flush_status);
        }
      }

      LLFS_VLOG(1) << "Page devices attached";
    }

    // Resolve any jobs with a PrepareJob slot but no CommitJob or RollbackJob.
    //
    LLFS_VLOG(1) << "Resolving pending jobs...";
    Status jobs_resolved = visitor.resolve_pending_jobs(
        *cache, *recycler, /*volume_uuid=*/visitor.ids->payload.main_uuid, slot_writer, grant);

    BATT_REQUIRE_OK(jobs_resolved);

    LLFS_VLOG(1) << "Pending jobs resolved";

    // Notify all PageAllocators that we are done with recovery.
    //
    for (const auto& uuid : {
             visitor.ids->payload.main_uuid,
             visitor.ids->payload.recycler_uuid,
             visitor.ids->payload.trimmer_uuid,
         }) {
      for (const PageArena& arena : cache->all_arenas()) {
        BATT_REQUIRE_OK(arena.allocator().notify_user_recovered(uuid));
      }
    }
  }

  // Create the Volume object.
  //
  std::unique_ptr<Volume> volume{
      new Volume{params.options, visitor.ids->payload.main_uuid, std::move(cache),
                 std::move(params.trim_control), std::move(page_deleter), std::move(root_log),
                 std::move(recycler), visitor.ids->payload.trimmer_uuid, *trimmer_visitor}};

  {
    batt::StatusOr<batt::Grant> trimmer_grant =
        volume->slot_writer_.reserve(trimmer_grant_size, batt::WaitForResource::kFalse);
    BATT_REQUIRE_OK(trimmer_grant);

    volume->trimmer_.push_grant(std::move(*trimmer_grant));
  }

  volume->start();

  return volume;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ Volume::Volume(const VolumeOptions& options, const boost::uuids::uuid& volume_uuid,
                            batt::SharedPtr<PageCache>&& page_cache,
                            std::shared_ptr<SlotLockManager>&& trim_control,
                            std::unique_ptr<PageCache::PageDeleterImpl>&& page_deleter,
                            std::unique_ptr<LogDevice>&& root_log,
                            std::unique_ptr<PageRecycler>&& recycler,
                            const boost::uuids::uuid& trimmer_uuid,
                            const VolumeTrimmer::RecoveryVisitor& trimmer_recovery_visitor) noexcept
    : options_{options}
    , volume_uuid_{volume_uuid}
    , cache_{std::move(page_cache)}
    , trim_control_{std::move(trim_control)}
    , page_deleter_{std::move(page_deleter)}
    , root_log_{std::move(root_log)}
    , trim_lock_{BATT_OK_RESULT_OR_PANIC(this->trim_control_->lock_slots(
          this->root_log_->slot_range(LogReadMode::kDurable), "Volume::(ctor)"))}
    , recycler_{std::move(recycler)}
    , slot_writer_{*this->root_log_}
    , trimmer_{
          trimmer_uuid,
          *this->trim_control_,
          this->root_log_->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable),
          this->slot_writer_,
          VolumeTrimmer::make_default_drop_roots_fn(this->cache(), *this->recycler_, trimmer_uuid),
          trimmer_recovery_visitor}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Volume::~Volume() noexcept
{
  this->root_log_->flush().IgnoreError();
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void Volume::start()
{
  if (this->recycler_) {
    this->recycler_->start();
  }

  if (!this->trimmer_task_) {
    this->trimmer_task_.emplace(
        /*executor=*/batt::Runtime::instance().schedule_task(),
        [this] {
          Status result = this->trimmer_.run();
          LLFS_VLOG(1) << "Volume::trimmer_task_ exited with status=" << result;
        },
        "Volume::trimmer_task_");
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void Volume::halt()
{
  this->slot_writer_.halt();
  this->trim_control_->halt();
  this->trimmer_.halt();
  this->root_log_->close().IgnoreError();
  if (this->recycler_) {
    this->recycler_->halt();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void Volume::join()
{
  if (this->trimmer_task_) {
    this->trimmer_task_->join();
    this->trimmer_task_ = None;
  }
  if (this->recycler_) {
    this->recycler_->join();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Volume::trim(slot_offset_type slot_lower_bound)
{
  return this->trim_lock_.with_lock([slot_lower_bound, this](SlotReadLock& trim_lock) -> Status {
    SlotRange target_range = trim_lock.slot_range();
    target_range.lower_bound = slot_max(target_range.lower_bound, slot_lower_bound);

    BATT_ASSIGN_OK_RESULT(trim_lock, this->trim_control_->update_lock(
                                         std::move(trim_lock), target_range, "Volume::trim"));

    return OkStatus();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> Volume::sync(LogReadMode mode, SlotUpperBoundAt event)
{
  Status status = this->root_log_->sync(mode, event);
  BATT_REQUIRE_OK(status);

  return this->root_log_->slot_range(mode);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotReadLock> Volume::lock_slots(const SlotRangeSpec& slot_range_spec, LogReadMode mode,
                                          Optional<const char*> lock_holder)
{
  const SlotRange slot_range = [&] {
    if (slot_range_spec.lower_bound == None || slot_range_spec.upper_bound == None) {
      const SlotRange default_range = this->root_log_->slot_range(mode);
      return SlotRange{slot_range_spec.lower_bound.value_or(default_range.lower_bound),
                       slot_range_spec.upper_bound.value_or(default_range.upper_bound)};
    } else {
      return SlotRange{*slot_range_spec.lower_bound, *slot_range_spec.upper_bound};
    }
  }();

  return this->trim_control_->lock_slots(slot_range, lock_holder.value_or("Volume::lock_slots"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotReadLock> Volume::lock_slots(const SlotRange& slot_range, LogReadMode mode,
                                          Optional<const char*> lock_holder)
{
  return this->lock_slots(SlotRangeSpec::from(slot_range), mode, lock_holder);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::Grant> Volume::reserve(u64 size, batt::WaitForResource wait_for_log_space)
{
  return this->slot_writer_.reserve(size, wait_for_log_space);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> Volume::append(const PackableRef& payload, batt::Grant& grant)
{
  return this->slot_writer_.append(grant, payload);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> Volume::append(const std::string_view& payload, batt::Grant& grant)
{
  // We must use `pack_as_raw` here so that when/if the payload is passed to a
  // slot visitor, it will not include a PackedBytes header; rather it should be
  // exactly the same bytes as `payload`.
  //
  return this->append(PackableRef{pack_as_raw(payload)}, grant);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> Volume::append(AppendableJob&& appendable, batt::Grant& grant,
                                   Optional<SlotSequencer>&& sequencer)
{
  const auto check_sequencer_is_resolved = batt::finally([&sequencer] {
    BATT_CHECK_IMPLIES(bool{sequencer}, sequencer->is_resolved())
        << "If a SlotSequencer is passed, it must be resolved even on failure "
           "paths.";
  });

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Phase 0: Wait for the previous slot in the sequence to be appended to the
  // log.
  //
  if (sequencer) {
    BATT_DEBUG_INFO("awaiting previous slot in sequence; "
                    << BATT_INSPECT(sequencer->has_prev()) << BATT_INSPECT(sequencer->poll_prev())
                    << BATT_INSPECT(sequencer->get_current()) << sequencer->debug_info());

    StatusOr<SlotRange> prev_slot = sequencer->await_prev();
    if (!prev_slot.ok()) {
      sequencer->set_error(prev_slot.status());
    }
    BATT_REQUIRE_OK(prev_slot);

    // We only need to do a speculative sync here, because flushing later slots
    // in the log implies that all earlier ones are flushed, and we are going to
    // do a durable sync (flush) for our prepare event below.
    //
    BATT_DEBUG_INFO("awaiting flush of previous slot: " << *prev_slot);

    Status sync_prev = this->slot_writer_.sync(LogReadMode::kSpeculative,
                                               SlotUpperBoundAt{prev_slot->upper_bound});
    if (!sync_prev.ok()) {
      sequencer->set_error(sync_prev);
    }
    BATT_REQUIRE_OK(sync_prev);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Phase 1: Write a prepare slot to the write-ahead log and flush it to
  // durable storage.
  //
  BATT_DEBUG_INFO("appending PrepareJob slot to the WAL");

  auto prepared_job = prepare(appendable);
  {
    BATT_ASSIGN_OK_RESULT(batt::Grant trim_refresh_grant,
                          grant.spend(packed_sizeof_slot(prepared_job)));
    this->trimmer_.push_grant(std::move(trim_refresh_grant));
  }

  StatusOr<SlotRange> prepare_slot =
      LLFS_COLLECT_LATENCY(this->metrics_.prepare_slot_append_latency,
                           this->slot_writer_.append(grant, std::move(prepared_job)));

  if (sequencer) {
    if (!prepare_slot.ok()) {
      BATT_CHECK(sequencer->set_error(prepare_slot.status()))
          << "each slot within a sequence may only be set once!";
    } else {
      BATT_CHECK(sequencer->set_current(*prepare_slot))
          << "each slot within a sequence may only be set once!";
    }
  }
  BATT_REQUIRE_OK(prepare_slot);

  BATT_DEBUG_INFO("flushing PrepareJob slot to storage");

  Status sync_prepare = LLFS_COLLECT_LATENCY(
      this->metrics_.prepare_slot_sync_latency,
      this->slot_writer_.sync(LogReadMode::kDurable, SlotUpperBoundAt{prepare_slot->upper_bound}));

  BATT_REQUIRE_OK(sync_prepare);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Phase 2a: Commit the job; this writes new pages, updates ref counts, and
  // deletes dropped pages.
  //
  BATT_DEBUG_INFO("committing PageCacheJob");

  const JobCommitParams params{
      .caller_uuid = &this->get_volume_uuid(),
      .caller_slot = prepare_slot->lower_bound,
      .recycler = as_ref(*this->recycler_),
      .recycle_grant = nullptr,
      .recycle_depth = -1,
  };

  Status commit_job_result = commit(std::move(appendable.job), params, Caller::Unknown);

  // BATT_UNTESTED_COND(!commit_job_result.ok());
  BATT_REQUIRE_OK(commit_job_result);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Phase 2b: Write the commit slot.
  //
  BATT_DEBUG_INFO("writing commit slot");

  StatusOr<SlotRange> commit_slot =
      this->slot_writer_.append(grant, PackedCommitJob{
                                           .reserved_ = {},
                                           .prepare_slot = prepare_slot->lower_bound,
                                       });

  BATT_REQUIRE_OK(commit_slot);

  return SlotRange{
      .lower_bound = prepare_slot->lower_bound,
      .upper_bound = commit_slot->upper_bound,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<VolumeReader> Volume::reader(const SlotRangeSpec& slot_range, LogReadMode mode)
{
  BATT_CHECK_NOT_NULLPTR(this->root_log_);

  SlotRange base_range = this->root_log_->slot_range(mode);

  // Clamp the effective slot range to the portion covered by the trim lock.
  //
  base_range.lower_bound = this->trim_lock_.with_lock([&](SlotReadLock& trim_lock) {
    if (trim_lock) {
      return slot_max(base_range.lower_bound, trim_lock.slot_range().lower_bound);
    }
    BATT_UNTESTED_LINE();
    return base_range.lower_bound;
  });

  // Acquire a lock on the
  //
  StatusOr<SlotReadLock> read_lock = this->trim_control_->lock_slots(
      SlotRange{
          .lower_bound = slot_range.lower_bound.value_or(base_range.lower_bound),
          .upper_bound = slot_range.upper_bound.value_or(base_range.upper_bound),
      },
      "Volume::read");

  BATT_UNTESTED_COND(!read_lock.ok());
  BATT_REQUIRE_OK(read_lock);

  return VolumeReader{*this, std::move(*read_lock), mode};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<PageCacheJob> Volume::new_job() const
{
  return this->cache().new_job();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PrepareJob_must_be_passed_to_Volume_append_by_move* Volume::append(const AppendableJob&,
                                                                          batt::Grant&,
                                                                          Optional<SlotSequencer>&&)
{
  return nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Volume::await_trim(slot_offset_type slot_lower_bound)
{
  return this->slot_writer_.await_trim(slot_lower_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache& Volume::cache() const
{
  return *this->cache_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotLockManager& Volume::trim_control()
{
  return *this->trim_control_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> Volume::sync(StatusOr<SlotRange> slot_range)
{
  BATT_REQUIRE_OK(slot_range);

  return this->sync(LogReadMode::kDurable, SlotUpperBoundAt{
                                               .offset = slot_range->upper_bound,
                                           });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 Volume::root_log_space() const
{
  return this->root_log_->space();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 Volume::root_log_size() const
{
  return this->root_log_->size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 Volume::root_log_capacity() const
{
  return this->root_log_->capacity();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotRange Volume::root_log_slot_range(LogReadMode mode) const
{
  return this->root_log_->slot_range(mode);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageRecycler::Metrics& Volume::page_recycler_metrics() const
{
  return this->recycler_->metrics();
}

}  // namespace llfs
