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
#include <llfs/volume_job_recovery_visitor.hpp>
#include <llfs/volume_metadata_recovery_visitor.hpp>
#include <llfs/volume_reader.hpp>
#include <llfs/volume_trimmer_recovery_visitor.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ bool Volume::write_new_pages_asap()
{
  static const bool value = [] {
    bool b = (batt::getenv_as<int>("LLFS_WRITE_NEW_PAGES_ASAP").value_or(1) == 1);
    LLFS_LOG_INFO() << "LLFS_WRITE_NEW_PAGES_ASAP=" << b;
    return b;
  }();

  return value;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const VolumeOptions& Volume::options() const
{
  return this->options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::string& Volume::name() const noexcept
{
  return this->options().name;
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
  return this->trimmer_->uuid();
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
  return appendable.calculate_grant_size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto Volume::recover(VolumeRecoverParams&& params,
                                const VolumeReader::SlotVisitorFn& slot_visitor_fn)
    -> StatusOr<std::unique_ptr<Volume>>
{
  BATT_CHECK_NOT_NULLPTR(params.scheduler);

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

  LLFS_VLOG(1) << "Recovering PageRecycler";

  BATT_ASSIGN_OK_RESULT(
      std::unique_ptr<PageRecycler> recycler,
      PageRecycler::recover(scheduler, volume_options.name + "_PageRecycler", recycler_options,
                            *page_deleter, recycler_log_factory));

  LLFS_VLOG(1) << "PageRecycler recovered";

  VolumeMetadata metadata;

  VolumeJobRecoveryVisitor job_visitor;
  VolumeMetadataRecoveryVisitor metadata_visitor{metadata};
  VolumeSlotDemuxer<Status> user_fn_visitor{batt::make_copy(slot_visitor_fn)};

  // Open the log device and scan all slots.
  //
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<LogDevice> root_log,
                        root_log_factory.open_log_device(
                            [&](LogDevice::Reader& log_reader) -> StatusOr<slot_offset_type> {
                              TypedSlotReader<VolumeEventVariant> slot_reader{log_reader};

                              StatusOr<usize> slots_read = slot_reader.run(
                                  batt::WaitForResource::kFalse, [&](auto&&... args) -> Status {
                                    BATT_REQUIRE_OK(job_visitor(args...));
                                    BATT_REQUIRE_OK(metadata_visitor(args...));
                                    BATT_REQUIRE_OK(user_fn_visitor(args...));
                                    return batt::OkStatus();
                                  });

                              BATT_REQUIRE_OK(slots_read);

                              return log_reader.slot_offset();
                            }));

  // Create a slot writer to complete recovery.
  //
  auto slot_writer = std::make_unique<TypedSlotWriter<VolumeEventVariant>>(*root_log);

  // If no uuids were found while opening the log, create them now.
  //
  if (!metadata.ids) {
    LLFS_VLOG(1) << "Initializing Volume uuids for the first time";

    metadata.ids.emplace(PackedVolumeIds{
        .main_uuid = volume_options.uuid.value_or(boost::uuids::random_generator{}()),
        .recycler_uuid = recycler->uuid(),
        .trimmer_uuid = boost::uuids::random_generator{}(),
    });
  }

  // Make sure all Volume metadata is up-to-date.
  //
  auto metadata_refresher =
      std::make_unique<VolumeMetadataRefresher>(*slot_writer, batt::make_copy(metadata));
  {
    for (const boost::uuids::uuid& uuid : {
             metadata.ids->main_uuid,
             metadata.ids->recycler_uuid,
             metadata.ids->trimmer_uuid,
         }) {
      for (PageDeviceEntry* entry : cache->all_devices()) {
        BATT_CHECK_NOT_NULLPTR(entry);
        const PageArena& arena = entry->arena;
        if (!arena.has_allocator()) {
          continue;
        }
        Optional<PageAllocatorAttachmentStatus> attachment =
            arena.allocator().get_client_attachment_status(uuid);

        // If already attached, then nothing to do; continue.
        //
        if (attachment) {
          continue;
        }

        // Find the lowest available slot offset for the log associated with `uuid`.
        //
        const slot_offset_type next_available_slot_offset = [&] {
          if (uuid == metadata.ids->recycler_uuid) {
            return recycler->slot_upper_bound(LogReadMode::kDurable);
          } else {
            // Both the volume (main) and trimmer share the same WAL (the main log).
            //
            return slot_writer->slot_offset();
          }
        }();

        //----- --- -- -  -  -   -
        // Attach to this arena's PageAllocator.
        {
          LLFS_VLOG(1) << "[Volume::recover] attaching client " << uuid << " to device "
                       << arena.device().get_id() << BATT_INSPECT(next_available_slot_offset);

          StatusOr<slot_offset_type> sync_slot =
              arena.allocator().attach_user(uuid, /*user_slot=*/next_available_slot_offset);

          BATT_REQUIRE_OK(sync_slot);

          Status sync_status = arena.allocator().sync(*sync_slot);

          BATT_UNTESTED_COND(!sync_status.ok());
        }
        //----- --- -- -  -  -   -

        // Add the attachment to the metadata refresher; we will append a new slot when we call
        // flush below.
        //
        BATT_ASSIGN_OK_RESULT(batt::Grant attach_grant,
                              slot_writer->reserve(VolumeMetadataRefresher::kAttachmentGrantSize,
                                                   batt::WaitForResource::kFalse));

        BATT_REQUIRE_OK(metadata_refresher->add_attachment(
            VolumeAttachmentId{
                .client = uuid,
                .device = arena.device().get_id(),
            },
            next_available_slot_offset, attach_grant));
      }
    }

    //----- --- -- -  -  -   -
    // Flush any non-durable metadata to the log.
    //
    if (metadata_refresher->needs_flush()) {
      batt::Grant initial_metadata_flush_grant = BATT_OK_RESULT_OR_PANIC(slot_writer->reserve(
          metadata_refresher->flush_grant_size(), batt::WaitForResource::kFalse));

      BATT_REQUIRE_OK(metadata_refresher->update_grant_partial(initial_metadata_flush_grant));
      BATT_REQUIRE_OK(metadata_refresher->flush());
    }

    {
      const usize reclaimable_size = metadata_visitor.grant_byte_size_reclaimable_on_trim();

      const usize initial_refresh_grant_size = [&]() -> usize {
        if (metadata_refresher->grant_required() < reclaimable_size) {
          return 0;
        }
        return metadata_refresher->grant_required() - reclaimable_size;
      }();

      LLFS_VLOG(1) << "Reserving grant for metadata refresher;"
                   << BATT_INSPECT(initial_refresh_grant_size)
                   << BATT_INSPECT(metadata_refresher->grant_target())
                   << BATT_INSPECT(metadata_refresher->grant_size())
                   << BATT_INSPECT(metadata_refresher->grant_required())
                   << BATT_INSPECT(VolumeMetadata::kVolumeIdsGrantSize)
                   << BATT_INSPECT(VolumeMetadata::kAttachmentGrantSize)
                   << BATT_INSPECT(reclaimable_size);

      batt::Grant initial_metadata_refresh_grant = BATT_OK_RESULT_OR_PANIC(
          slot_writer->reserve(initial_refresh_grant_size, batt::WaitForResource::kFalse));

      BATT_REQUIRE_OK(metadata_refresher->update_grant_partial(initial_metadata_refresh_grant));
    }
  }

  {
    // Resolve any jobs with a PrepareJob slot but no CommitJob or RollbackJob.
    //
    LLFS_VLOG(1) << "Resolving pending jobs...";
    Status jobs_resolved = job_visitor.resolve_pending_jobs(
        *cache, *recycler, /*volume_uuid=*/metadata.ids->main_uuid, *slot_writer);

    BATT_REQUIRE_OK(jobs_resolved);

    LLFS_VLOG(1) << "Pending jobs resolved";

    // Notify all PageAllocators that we are done with recovery.
    //
    for (const boost::uuids::uuid& uuid : {
             metadata.ids->main_uuid,
             metadata.ids->recycler_uuid,
             metadata.ids->trimmer_uuid,
         }) {
      for (PageDeviceEntry* entry : cache->all_devices()) {
        BATT_CHECK_NOT_NULLPTR(entry);
        if (entry->arena.has_allocator()) {
          BATT_REQUIRE_OK(entry->arena.allocator().notify_user_recovered(uuid));
        }
      }
    }
  }

  // The recycler must be started before the trimmer is recovered, in case the trimmer needs to
  // recycle some pages and the recycler log is full.
  //
  BATT_CHECK_NOT_NULLPTR(recycler);
  recycler->start();

  // Recover the VolumeTrimmer state, resolving any pending log trim.
  //
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<VolumeTrimmer> trimmer,
                        VolumeTrimmer::recover(                                            //
                            metadata.ids->trimmer_uuid,                                    //
                            batt::to_string(params.options.name, "_Trimmer"),              //
                            params.options.trim_delay_byte_count,                          //
                            *root_log,                                                     //
                            *slot_writer,                                                  //
                            VolumeTrimmer::make_default_drop_roots_fn(*cache, *recycler),  //
                            *params.trim_control,                                          //
                            *metadata_refresher                                            //
                            ));

  // Create the Volume object.
  //
  std::unique_ptr<Volume> volume{new Volume{
      scheduler,
      params.options,
      metadata.ids->main_uuid,
      std::move(cache),
      std::move(params.trim_control),
      std::move(page_deleter),
      std::move(root_log),
      std::move(recycler),
      std::move(slot_writer),
      std::move(metadata_refresher),
      std::move(trimmer),
  }};

  volume->start();

  return volume;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ Volume::Volume(batt::TaskScheduler& task_scheduler,                                 //
                            const VolumeOptions& options,                                        //
                            const boost::uuids::uuid& volume_uuid,                               //
                            batt::SharedPtr<PageCache>&& page_cache,                             //
                            std::shared_ptr<SlotLockManager>&& trim_control,                     //
                            std::unique_ptr<PageCache::PageDeleterImpl>&& page_deleter,          //
                            std::unique_ptr<LogDevice>&& root_log,                               //
                            std::unique_ptr<PageRecycler>&& recycler,                            //
                            std::unique_ptr<TypedSlotWriter<VolumeEventVariant>>&& slot_writer,  //
                            std::unique_ptr<VolumeMetadataRefresher>&& metadata_refresher,       //
                            std::unique_ptr<VolumeTrimmer>&& trimmer) noexcept
    : task_scheduler_{task_scheduler}
    , options_{options}
    , volume_uuid_{volume_uuid}
    , cache_{std::move(page_cache)}
    , trim_control_{std::move(trim_control)}
    , page_deleter_{std::move(page_deleter)}
    , root_log_{std::move(root_log)}
    , trim_lock_{BATT_OK_RESULT_OR_PANIC(this->trim_control_->lock_slots(
          this->root_log_->slot_range(LogReadMode::kDurable), "Volume::(ctor)"))}
    , recycler_{std::move(recycler)}
    , slot_writer_{std::move(slot_writer)}
    , metadata_refresher_{std::move(metadata_refresher)}
    , trimmer_{std::move(trimmer)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Volume::~Volume() noexcept
{
  this->pre_halt();
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
        /*executor=*/this->task_scheduler_.schedule_task(),
        [this] {
          Status result = this->trimmer_->run();
          if (this->pre_halt_.load()) {
            LLFS_VLOG(1) << "Volume::trimmer_task_ exited with status=" << result;
          } else {
            LLFS_LOG_ERROR() << "Volume::trimmer_task_ exited with status=" << result;
          }
        },
        /*task_name=*/batt::to_string(this->name(), "_Volume.trimmer_task"));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void Volume::pre_halt()
{
  const bool previously_pre_halted = this->pre_halt_.exchange(true);
  if (!previously_pre_halted) {
    if (this->trimmer_) {
      this->trimmer_->pre_halt();
    }
    if (this->recycler_) {
      this->recycler_->pre_halt();
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void Volume::halt()
{
  this->pre_halt();

  this->slot_writer_->halt();
  this->trim_control_->halt();
  this->trimmer_->halt();
  this->root_log_->halt();
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
  this->root_log_->join();
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
  return this->slot_writer_->reserve(size, wait_for_log_space);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> Volume::append(const std::string_view& payload, batt::Grant& grant)
{
  // We must use `pack_as_raw` here so that when/if the payload is passed to a
  // slot visitor, it will not include a PackedBytes header; rather it should be
  // exactly the same bytes as `payload`.
  //
  return this->append(pack_as_raw(payload), grant);  // PackableRef
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> Volume::append(AppendableJob&& appendable, batt::Grant& grant,
                                   Optional<SlotSequencer>&& sequencer)
{
  if (write_new_pages_asap()) {
    BATT_REQUIRE_OK(appendable.job.start_writing_new_pages());
  }

  StatusOr<SlotRange> result;

  const auto check_sequencer_is_resolved = batt::finally([&sequencer, &result] {
    BATT_CHECK_IMPLIES(bool{sequencer}, sequencer->is_resolved())
        << "If a SlotSequencer is passed, it must be resolved even on failure "
           "paths."
        << BATT_INSPECT(result);
  });

  result = [&]() -> StatusOr<SlotRange> {
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

      Status sync_prev = this->slot_writer_->sync(LogReadMode::kSpeculative,
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

    PrepareJob prepared_job = prepare(appendable);
    const u64 prepared_job_size = packed_sizeof_slot(prepared_job);
    LLFS_VLOG(1) << BATT_INSPECT(prepared_job_size);

    Optional<slot_offset_type> prev_user_slot;

    const auto grant_size_before_prepare = grant.size();

    // Acquired in the post-commit-fn of the prepare slot; prevents the trimmer from having to
    // deal with unresolved jobs.
    //
    Optional<SlotReadLock> trim_lock;

    // Append the prepare!
    //
    StatusOr<SlotParseWithPayload<const PackedPrepareJob*>> prepare_slot = LLFS_COLLECT_LATENCY(
        this->metrics_.prepare_slot_append_latency,
        this->slot_writer_->typed_append(
            grant, std::move(prepared_job),
            [this, &prev_user_slot, &trim_lock](StatusOr<SlotRange> slot_range) {
              if (slot_range.ok()) {
                // Set the prev_user_slot.
                //
                trim_lock.emplace(BATT_OK_RESULT_OR_PANIC(
                    this->trim_control_->lock_slots(*slot_range, "Volume::append(job)")));

                // Acquire a read lock to prevent premature trimming.
                //
                prev_user_slot = this->latest_user_slot_.exchange(slot_range->lower_bound);
              }
              return slot_range;
            }));

    const auto grant_size_after_prepare = grant.size();

    if (prepare_slot.ok()) {
      BATT_CHECK_EQ(prepared_job_size, grant_size_before_prepare - grant_size_after_prepare)
          << BATT_INSPECT(prepare_slot.status()) << BATT_INSPECT(grant_size_before_prepare);
    }

    if (sequencer) {
      if (!prepare_slot.ok()) {
        BATT_CHECK(sequencer->set_error(prepare_slot.status()))
            << "each slot within a sequence may only be set once!";
      } else {
        BATT_CHECK(sequencer->set_current(prepare_slot->slot.offset))
            << "each slot within a sequence may only be set once!";
      }
    }

    BATT_REQUIRE_OK(prepare_slot);

    const slot_offset_type prepare_slot_offset = prepare_slot->slot.offset.lower_bound;

    BATT_CHECK(trim_lock);
    BATT_CHECK(prev_user_slot);
    BATT_CHECK(slot_at_most(*prev_user_slot, prepare_slot_offset))
        << BATT_INSPECT(prev_user_slot) << BATT_INSPECT(prepare_slot);

    BATT_DEBUG_INFO("flushing PrepareJob slot to storage");

    Status sync_prepare = LLFS_COLLECT_LATENCY(
        this->metrics_.prepare_slot_sync_latency,
        this->slot_writer_->sync(LogReadMode::kDurable,
                                 SlotUpperBoundAt{prepare_slot->slot.offset.upper_bound}));

    BATT_REQUIRE_OK(sync_prepare);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Phase 2a: Commit the job; this writes new pages, updates ref counts, and
    // deletes dropped pages.
    //
    BATT_DEBUG_INFO("committing PageCacheJob");

    const JobCommitParams params{
        .caller_uuid = &this->get_volume_uuid(),
        .caller_slot = prepare_slot_offset,
        .recycler = as_ref(*this->recycler_),
        .recycle_grant = nullptr,
        .recycle_depth = -1,
    };

    Status commit_job_result = commit(std::move(appendable.job), params, Caller::Unknown,
                                      *prev_user_slot, &this->durable_user_slot_);

    BATT_REQUIRE_OK(commit_job_result);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Phase 2b: Write the commit slot.
    //
    BATT_DEBUG_INFO("writing commit slot");

    CommitJob commit_job{
        .prepare_slot_offset = prepare_slot_offset,
        .packed_prepare = prepare_slot->payload,
    };
    LLFS_VLOG(1) << BATT_INSPECT(packed_sizeof_slot(commit_job));

    StatusOr<SlotRange> commit_slot = this->slot_writer_->append(grant, std::move(commit_job));

    BATT_REQUIRE_OK(commit_slot);

    return SlotRange{
        .lower_bound = prepare_slot->slot.offset.lower_bound,
        .upper_bound = commit_slot->upper_bound,
    };
  }();

  return result;
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
  return this->slot_writer_->await_trim(slot_lower_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type Volume::get_trim_pos() const noexcept
{
  return this->slot_writer_->get_trim_pos();
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ConstBuffer> Volume::get_root_log_data(const SlotReadLock& read_lock,
                                                Optional<SlotRange> slot_range) const
{
  // The lock must be in good standing and sponsored by us.
  //
  if (!read_lock || read_lock.get_sponsor() != this->trim_control_.get()) {
    return {batt::StatusCode::kInvalidArgument};
  }

  // If slot_range is not specified, use the range of the lock.
  //
  if (!slot_range) {
    slot_range = read_lock.slot_range();
  }

  // Check for negative-size slot range.
  //
  if (slot_less_than(slot_range->upper_bound, slot_range->lower_bound)) {
    return {batt::StatusCode::kInvalidArgument};
  }

  // The captured range may be below the lock's lower_bound, but not by more than the trim delay.
  //
  if (slot_less_than(slot_range->lower_bound,
                     read_lock.slot_range().lower_bound - this->options_.trim_delay_byte_count)) {
    return {batt::StatusCode::kOutOfRange};
  }

  // Edge case: request for empty slot range.
  //
  if (slot_range->lower_bound == slot_range->upper_bound) {
    return batt::ConstBuffer{nullptr, 0};
  }

  // Create a log reader to access the data.
  //
  std::unique_ptr<llfs::LogDevice::Reader> log_reader =
      this->root_log_->new_reader(slot_range->lower_bound, llfs::LogReadMode::kSpeculative);

  BATT_CHECK_NOT_NULLPTR(log_reader);
  BATT_CHECK_EQ(log_reader->slot_offset(), slot_range->lower_bound);

  // Verify that we can capture the requested range.
  //
  const usize slot_range_size = BATT_CHECKED_CAST(usize, slot_range->size());
  batt::ConstBuffer available = log_reader->data();
  if (available.size() < slot_range_size) {
    return {batt::StatusCode::kOutOfRange};
  }

  return batt::ConstBuffer{available.data(), slot_range_size};
}

}  // namespace llfs
