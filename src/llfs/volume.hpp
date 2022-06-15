//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_HPP
#define LLFS_VOLUME_HPP

#include <llfs/finalized_page_cache_job.hpp>
#include <llfs/log_device.hpp>
#include <llfs/packable_ref.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/page_recycler.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_lock_manager.hpp>
#include <llfs/slot_sequencer.hpp>
#include <llfs/volume_events.hpp>
#include <llfs/volume_metrics.hpp>
#include <llfs/volume_options.hpp>
#include <llfs/volume_reader.hpp>
#include <llfs/volume_trimmer.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/shared_ptr.hpp>

#include <memory>
#include <type_traits>

namespace llfs {

struct VolumeRecoverParams {
  batt::TaskScheduler* scheduler;
  VolumeOptions options;
  batt::SharedPtr<PageCache> cache;
  LogDeviceFactory* root_log_factory;
  LogDeviceFactory* recycler_log_factory;
  std::unique_ptr<SlotLockManager> trim_control;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class Volume
{
 public:
  friend class VolumeReader;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Read the Volume state from the passed log devices and resolve any pending jobs by committing or
  // rolling back so the Volume is in a clean state.
  //
  static StatusOr<std::unique_ptr<Volume>> recover(
      VolumeRecoverParams&& params, const VolumeReader::SlotVisitorFn& slot_visitor_fn);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  ~Volume() noexcept;

  // Initiates an asynchronous shutdown of the Volume and all associated background Tasks.
  //
  void halt();

  // Waits for all activity to be stopped on this Volume.  `halt()` must be called, or `join()` will
  // never return.
  //
  void join();

  // The VolumeOptions used to create/recover this volume.
  //
  const VolumeOptions& options() const
  {
    return this->options_;
  }

  // Returns the UUID for this volume.
  //
  const boost::uuids::uuid& get_volume_uuid() const
  {
    return this->volume_uuid_;
  }

  // Returns the UUID for this volume's recycler log.
  //
  const boost::uuids::uuid& get_recycler_uuid() const
  {
    return this->recycler_->uuid();
  }

  // Returns the UUID for this volume's trimmer task.
  //
  const boost::uuids::uuid& get_trimmer_uuid() const
  {
    return this->trimmer_.uuid();
  }

  // Reserve `size` bytes in the log for future appends.
  //
  StatusOr<batt::Grant> reserve(u64 size, batt::WaitForResource wait_for_log_space);

  // Returns the number of bytes needed to append `payload`.
  //
  u64 calculate_grant_size(const PackableRef& payload) const
  {
    return packed_sizeof_slot(payload);
  }

  // Returns the number of bytes needed to append `prepare_job`.
  //
  u64 calculate_grant_size(const AppendableJob& appendable) const
  {
    return packed_sizeof_slot(prepare(appendable)) +
           packed_sizeof_slot(batt::StaticType<PackedCommitJob>{});
  }

  // Atomically append a new slot containing `payload` to the end of the root log.
  //
  StatusOr<SlotRange> append(const PackableRef& payload, batt::Grant& grant);

  // Create a new PageCacheJob for writing new pages via the WAL of this Volume.
  //
  std::unique_ptr<PageCacheJob> new_job() const
  {
    return this->cache().new_job();
  }

  // Atomically append a new slot containing `payload` to the end of the root log, committing the
  // job contained in `prepare_job`.
  //
  StatusOr<SlotRange> append(AppendableJob&& appendable_job, batt::Grant& grant,
                             Optional<SlotSequencer>&& prepare_slot_sequencer = None);

  // Overload to prevent confusion.
  //
  struct PrepareJob_must_be_passed_to_Volume_append_by_move* append(
      const AppendableJob&, batt::Grant&, Optional<SlotSequencer>&& = None)
  {
    return nullptr;
  }

  // Returns a new VolumeReader for the given slot range and durability level.  This can be used to
  // read raw user-level slot data; if you want to read typed user slots, use `typed_reader`
  // instead.
  //
  StatusOr<VolumeReader> reader(const SlotRangeSpec& slot_range, LogReadMode mode);

  // Returns a new TypedVolumeReader for reading user-level packed slot data from the root log.  `T`
  // should be a PackedVariant type.
  //
  template <typename T>
  StatusOr<TypedVolumeReader<T>> typed_reader(const SlotRangeSpec& slot_range, LogReadMode mode,
                                              batt::StaticType<T> = {});

  // Blocks until the Volume is closed/failed, or the root log trim position has reached the
  // specified lower bound.
  //
  Status await_trim(slot_offset_type slot_lower_bound)
  {
    return this->slot_writer_.await_trim(slot_lower_bound);
  }

  // Returns the PageCache associated with this Volume.
  //
  PageCache& cache() const
  {
    return *this->cache_;
  }

  // Set the Volume's minimum trim position.  The log will be trimmed when all slot read locks for a
  // given SlotRange have been released; this method only releases one such lock.
  //
  Status trim(slot_offset_type slot_lower_bound);

  // Returns a reference to the root log lock manager, to allow users to obtain read locks on
  // arbitrary SlotRanges of the root log without the overhead of creating a full VolumeReader.
  //
  SlotLockManager& trim_control()
  {
    return *this->trim_control_;
  }

  // Blocks until the root log has reached the given durability level at the given slot offset.
  //
  StatusOr<SlotRange> sync(LogReadMode mode, SlotUpperBoundAt event);

  // Convenience.
  //
  StatusOr<SlotRange> sync(StatusOr<SlotRange> slot_range)
  {
    BATT_REQUIRE_OK(slot_range);

    return this->sync(LogReadMode::kDurable, SlotUpperBoundAt{
                                                 .offset = slot_range->upper_bound,
                                             });
  }

  // Acquires a read lock on a the given slot offset range within the log, to prevent premature
  // trimming while that segment is being read.
  //
  StatusOr<SlotReadLock> lock_slots(const SlotRangeSpec& slot_range, LogReadMode mode);

  // Convenience.
  //
  StatusOr<SlotReadLock> lock_slots(const SlotRange& slot_range, LogReadMode mode)
  {
    return this->lock_slots(SlotRangeSpec::from(slot_range), mode);
  }

  auto root_log_space() const
  {
    return this->root_log_->space();
  }

  auto root_log_size() const
  {
    return this->root_log_->size();
  }

  auto root_log_capacity() const
  {
    return this->root_log_->capacity();
  }

  auto root_log_slot_range(LogReadMode mode) const
  {
    return this->root_log_->slot_range(mode);
  }

  const PageRecycler::Metrics& page_recycler_metrics() const
  {
    return this->recycler_->metrics();
  }

 private:
  explicit Volume(const VolumeOptions& options, const boost::uuids::uuid& volume_uuid,
                  batt::SharedPtr<PageCache>&& page_cache,
                  std::unique_ptr<SlotLockManager>&& trim_control,
                  std::unique_ptr<PageCache::PageDeleterImpl>&& page_deleter,
                  std::unique_ptr<LogDevice>&& root_log, std::unique_ptr<PageRecycler>&& recycler,
                  const boost::uuids::uuid& trimmer_uuid) noexcept;

  // Launch background tasks associated with this Volume.
  //
  void start();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Configuration options for this volume.
  //
  VolumeOptions options_;

  // The unique identifier for this volume.
  //
  const boost::uuids::uuid volume_uuid_;

  // Run-time health and performance metrics for this volume.
  //
  VolumeMetrics metrics_;

  // The cache backing this volume.
  //
  batt::SharedPtr<PageCache> cache_;

  // Tracks all readers so that we don't prematurely trim the log.
  //
  std::unique_ptr<SlotLockManager> trim_control_;

  // Links to `cache_` on behalf of `recycler_`.
  //
  std::unique_ptr<PageCache::PageDeleterImpl> page_deleter_;

  // The root WAL for the volume.
  //
  std::unique_ptr<LogDevice> root_log_;

  // Controls log trimming.  Volume::trim() updates this object.
  //
  batt::Mutex<SlotReadLock> trim_lock_;

  // Recycles pages that are dereferenced by this volume.
  //
  std::unique_ptr<PageRecycler> recycler_;

  // Appends new slots to the root log.
  //
  TypedSlotWriter<VolumeEventVariant> slot_writer_;

  // Refreshes volume config slots and trims the root log.
  //
  VolumeTrimmer trimmer_;

  // Task that runs `trimmer_` continuously in the background.
  //
  Optional<batt::Task> trimmer_task_;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_HPP

#include <llfs/volume.ipp>
