//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_TRIMMER_HPP
#define LLFS_VOLUME_TRIMMER_HPP

#include <llfs/log_device.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/slot_lock_manager.hpp>
#include <llfs/slot_writer.hpp>
#include <llfs/status.hpp>
#include <llfs/volume_events.hpp>

#include <atomic>
#include <unordered_map>
#include <unordered_set>

namespace llfs {

class VolumeTrimmer
{
 public:
  // A log trim operation.
  //
  class TrimJob;

  // Listener for dropped root page refs.
  //
  using DropRootsFn = std::function<batt::Status(slot_offset_type, Slice<const PageId>)>;

  using PendingJobsMap = std::unordered_map<slot_offset_type /*prepare_slot*/, std::vector<PageId>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static DropRootsFn make_default_drop_roots_fn(PageCache& cache, PageRecycler& recycler,
                                                const boost::uuids::uuid& trimmer_uuid);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit VolumeTrimmer(const boost::uuids::uuid& trimmer_uuid, SlotLockManager& trim_control,
                         std::unique_ptr<LogDevice::Reader>&& log_reader,
                         TypedSlotWriter<VolumeEventVariant>& slot_writer,
                         DropRootsFn&& drop_roots) noexcept;

  VolumeTrimmer(const VolumeTrimmer&) = delete;
  VolumeTrimmer& operator=(const VolumeTrimmer&) = delete;

  const boost::uuids::uuid& uuid() const
  {
    return this->trimmer_uuid_;
  }

  /** \brief Adds the given grant to the trim event grant held by this object, which is used to
   * append the log.
   */
  void push_grant(batt::Grant&& grant) noexcept;

  void halt();

  Status run();

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  boost::uuids::uuid trimmer_uuid_;
  SlotLockManager& trim_control_;
  std::unique_ptr<LogDevice::Reader> log_reader_;
  TypedSlotReader<VolumeEventVariant> slot_reader_;
  TypedSlotWriter<VolumeEventVariant>& slot_writer_;
  DropRootsFn drop_roots_;
  batt::Grant trimmer_grant_;
  std::atomic<bool> halt_requested_{false};
  PendingJobsMap pending_jobs_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeTrimmer::TrimJob
{
 public:
  explicit TrimJob(slot_offset_type old_trim_pos, slot_offset_type new_trim_pos,
                   VolumeTrimmer::PendingJobsMap& pending_jobs) noexcept;

  /** \brief Scan log slots up to the new trim slot offset, collecting page refs to release.
   *
   *  \return batt::OkStatus() if successful; error status code otherwise.
   */
  Status scan_trimmed_region(TypedSlotReader<VolumeEventVariant>& slot_reader) noexcept;

  /** \brief If there are ids to refresh, then append slots to the log to refresh them.
   */
  Status refresh_ids(batt::Grant& trimmer_grant, TypedSlotWriter<VolumeEventVariant>& slot_writer);

  /** \brief If there are attachments to refresh, then append slots to the log to refresh them.
   */
  Status refresh_attachment_events(batt::Grant& trimmer_grant,
                                   TypedSlotWriter<VolumeEventVariant>& slot_writer);

  /** \brief Write the trim event to the volume WAL; this must come before all other steps.
   *
   *  \return The slot upper bound of the written trim event.
   */
  StatusOr<SlotRange> write_trim_event(batt::Grant& trimmer_grant,
                                       TypedSlotWriter<VolumeEventVariant>& slot_writer) noexcept;

  /** \brief Writes page ref updates for prepare/commits in the trimmed region.
   */
  Status drop_obsolete_roots(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                             const SlotRange& trim_event_slot,
                             const DropRootsFn& drop_roots) noexcept;

  /** \brief Do the log trim and retain the precalculated amount of grant.
   */
  Status trim_log(batt::Grant& trimmer_grant, TypedSlotWriter<VolumeEventVariant>& slot_writer);

  /** \brief The log offset that will be the new lower bound for the log when this job completes.
   */
  slot_offset_type new_trim_pos() const
  {
    return this->new_trim_pos_;
  }

 private:
  Status visit_slot(const SlotParse& slot, const Ref<const PackedRawData>& raw);
  Status visit_slot(const SlotParse& slot, const Ref<const PackedPrepareJob>& prepare);
  Status visit_slot(const SlotParse& slot, const PackedCommitJob& commit);
  Status visit_slot(const SlotParse& slot, const PackedRollbackJob& rollback);
  Status visit_slot(const SlotParse& slot, const PackedVolumeIds& ids);
  Status visit_slot(const SlotParse& slot, const PackedVolumeAttachEvent& attach);
  Status visit_slot(const SlotParse& slot, const PackedVolumeDetachEvent& detach);
  Status visit_slot(const SlotParse& slot, const PackedVolumeFormatUpgrade& upgrade);
  Status visit_slot(const SlotParse& slot, const PackedVolumeRecovered& recovered);
  Status visit_slot(const SlotParse& slot, const VolumeTrimEvent& trim);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  slot_offset_type old_trim_pos_;
  slot_offset_type new_trim_pos_;

  // PageIds from pending jobs before the current trimmed region.
  //
  VolumeTrimmer::PendingJobsMap& prior_pending_jobs_;

  // PageIds discovered in trimmed jobs.
  //
  VolumeTrimmer::PendingJobsMap trimmed_pending_jobs_;

  // PageIds that are confirmed to have been written.
  //
  std::vector<PageId> obsolete_roots_;

  // PageDevice attachments found in the last GC'ed log segment that must be refreshed before
  // trimming.
  //
  std::unordered_map<boost::uuids::uuid, PackedVolumeAttachEvent, boost::hash<boost::uuids::uuid>>
      attachments_to_refresh_;

  // PageDevice detachments found in the last GC'ed log segment that must be refreshed before
  // trimming.
  //
  std::unordered_map<boost::uuids::uuid, PackedVolumeDetachEvent, boost::hash<boost::uuids::uuid>>
      detachments_to_refresh_;

  // Volume metadata found in the last GC'ed log segment that must be refreshed before trimming.
  //
  Optional<PackedVolumeIds> ids_to_refresh_;

  // The amount of grant to retain after trimming.
  //
  usize grant_size_to_reserve_;

  // The amount of grant to release after trimming.
  //
  usize grant_size_to_release_;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_TRIMMER_HPP

#include <llfs/volume_trimmer.ipp>
