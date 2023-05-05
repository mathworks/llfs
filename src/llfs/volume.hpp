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
#include <llfs/volume_reader.ipp>
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
  std::shared_ptr<SlotLockManager> trim_control;
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
  const VolumeOptions& options() const;

  // Returns the UUID for this volume.
  //
  const boost::uuids::uuid& get_volume_uuid() const;

  // Returns the UUID for this volume's recycler log.
  //
  const boost::uuids::uuid& get_recycler_uuid() const;

  // Returns the UUID for this volume's trimmer task.
  //
  const boost::uuids::uuid& get_trimmer_uuid() const;

  // Reserve `size` bytes in the log for future appends.
  //
  StatusOr<batt::Grant> reserve(u64 size, batt::WaitForResource wait_for_log_space);

  // Returns the number of bytes needed to append `payload`.
  //
  u64 calculate_grant_size(const PackableRef& payload) const;

  // Returns the number of bytes needed to append `payload`.
  //
  u64 calculate_grant_size(const std::string_view& payload) const;

  // Returns the number of bytes needed to append `prepare_job`.
  //
  u64 calculate_grant_size(const AppendableJob& appendable) const;

  // Atomically append a new slot containing `payload` to the end of the root log.
  //
  StatusOr<SlotRange> append(const PackableRef& payload, batt::Grant& grant);

  // Atomically append a new slot containing `payload` to the end of the root log.
  //
  StatusOr<SlotRange> append(const std::string_view& payload, batt::Grant& grant);

  // Create a new PageCacheJob for writing new pages via the WAL of this Volume.
  //
  std::unique_ptr<PageCacheJob> new_job() const;

  // Atomically append a new slot containing `payload` to the end of the root log, committing the
  // job contained in `prepare_job`.
  //
  StatusOr<SlotRange> append(AppendableJob&& appendable_job, batt::Grant& grant,
                             Optional<SlotSequencer>&& prepare_slot_sequencer = None,
                             u64* total_spent = nullptr);

  // Overload to prevent confusion.
  //
  struct PrepareJob_must_be_passed_to_Volume_append_by_move* append(
      const AppendableJob&, batt::Grant&, Optional<SlotSequencer>&& = None);

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
  Status await_trim(slot_offset_type slot_lower_bound);

  /** \brief Causes trim to be delayed by the specified number of bytes.  The default is 0.
   */
  void set_trim_delay(u64 byte_count);

  // Returns the PageCache associated with this Volume.
  //
  PageCache& cache() const;

  // Set the Volume's minimum trim position.  The log will be trimmed when all slot read locks for a
  // given SlotRange have been released; this method only releases one such lock.
  //
  Status trim(slot_offset_type slot_lower_bound);

  // Returns a reference to the root log lock manager, to allow users to obtain read locks on
  // arbitrary SlotRanges of the root log without the overhead of creating a full VolumeReader.
  //
  SlotLockManager& trim_control();

  // Blocks until the root log has reached the given durability level at the given slot offset.
  //
  StatusOr<SlotRange> sync(LogReadMode mode, SlotUpperBoundAt event);

  // Convenience.
  //
  StatusOr<SlotRange> sync(StatusOr<SlotRange> slot_range);

  // Acquires a read lock on a the given slot offset range within the log, to prevent premature
  // trimming while that segment is being read.
  //
  StatusOr<SlotReadLock> lock_slots(const SlotRangeSpec& slot_range, LogReadMode mode,
                                    Optional<const char*> lock_holder);

  // Convenience.
  //
  StatusOr<SlotReadLock> lock_slots(const SlotRange& slot_range, LogReadMode mode,
                                    Optional<const char*> lock_holder);

  // Returns the number of free bytes in the root log.
  //
  u64 root_log_space() const;

  // Returns the number of valid (committed) bytes in the root log.
  //
  u64 root_log_size() const;

  // Returns the total byte size of the root log; capacity == space + size.
  //
  u64 root_log_capacity() const;

  // Returns the current valid slot offset range for the root log at the specified durability level.
  //
  SlotRange root_log_slot_range(LogReadMode mode) const;

  // Returns diagnostic metrics for this Volume's PageRecycler.
  //
  const PageRecycler::Metrics& page_recycler_metrics() const;

  /** \brief Returns the root log data corresponding to the given slot read lock.
   *
   * The returned buffer is valid only as long as the lock is held.
   */
  StatusOr<ConstBuffer> get_root_log_data(const SlotReadLock& read_lock) const;

  //----- --- -- -  -  -   -
  // FOR TESTING ONLY
  //
  LogDevice& root_log() const noexcept
  {
    return *this->root_log_;
  }
  //----- --- -- -  -  -   -

 private:
  explicit Volume(batt::TaskScheduler& task_scheduler, const VolumeOptions& options,
                  const boost::uuids::uuid& volume_uuid, batt::SharedPtr<PageCache>&& page_cache,
                  std::shared_ptr<SlotLockManager>&& trim_control,
                  std::unique_ptr<PageCache::PageDeleterImpl>&& page_deleter,
                  std::unique_ptr<LogDevice>&& root_log, std::unique_ptr<PageRecycler>&& recycler,
                  const boost::uuids::uuid& trimmer_uuid,
                  const VolumeTrimmer::RecoveryVisitor& trimmer_recovery_visitor) noexcept;

  // Launch background tasks associated with this Volume.
  //
  void start();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // TaskScheduler used to launch all background tasks that are created after recovery.
  //
  batt::TaskScheduler& task_scheduler_;

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
  std::shared_ptr<SlotLockManager> trim_control_;

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

  // Tracks the latest job appended.
  //
  std::atomic<slot_offset_type> latest_user_slot_{0};

  // Tracks the latest job that is durable (will-commit).
  //
  batt::Watch<slot_offset_type> durable_user_slot_{0};
};

}  // namespace llfs

#endif  // LLFS_VOLUME_HPP

#include <llfs/volume.ipp>
