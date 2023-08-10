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
#include <llfs/volume_event_visitor.hpp>
#include <llfs/volume_events.hpp>

#include <atomic>
#include <unordered_map>
#include <unordered_set>

namespace llfs {

using VolumePendingJobsUMap =
    std::unordered_map<slot_offset_type /*prepare_slot*/, std::vector<PageId>>;

struct VolumeTrimmedRegionInfo {
  SlotRange slot_range;
  std::vector<slot_offset_type> resolved_jobs;
  VolumePendingJobsUMap pending_jobs;
  std::vector<PageId> obsolete_roots;
  Optional<PackedVolumeIds> ids_to_refresh;
  std::unordered_map<VolumeAttachmentId, PackedVolumeAttachEvent, VolumeAttachmentId::Hash>
      attachments_to_refresh;
  usize grant_size_to_release = 0;
  usize grant_size_to_reserve = 0;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool requires_trim_event_slot() const
  {
    return !this->resolved_jobs.empty() || !this->obsolete_roots.empty() ||
           !this->pending_jobs.empty();
  }
};

/** \brief Consumer of dropped root page refs.
 */
using VolumeDropRootsFn = std::function<batt::Status(slot_offset_type, Slice<const PageId>)>;

/** \brief Reads slots from the passed reader, up to the given slot upper bound, collecting the
 * information needed to trim the log.
 */
StatusOr<VolumeTrimmedRegionInfo> read_trimmed_region(
    TypedSlotReader<VolumeEventVariant>& slot_reader, slot_offset_type upper_bound,
    VolumePendingJobsUMap& prior_pending_jobs, std::atomic<u64>& job_slots_byte_count);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Tracks when the Volume metadata was last refreshed.
 */
struct VolumeMetadataRefreshInfo {
  using AttachSlotMap =
      std::unordered_map<VolumeAttachmentId, slot_offset_type, VolumeAttachmentId::Hash>;

  Optional<slot_offset_type> most_recent_ids_slot;
  AttachSlotMap most_recent_attach_slot;
};

inline bool operator==(const VolumeMetadataRefreshInfo& l, const VolumeMetadataRefreshInfo& r)
{
  return l.most_recent_ids_slot == r.most_recent_ids_slot  //
         && l.most_recent_attach_slot == r.most_recent_attach_slot;
}

inline bool operator!=(const VolumeMetadataRefreshInfo& l, const VolumeMetadataRefreshInfo& r)
{
  return !(l == r);
}

inline std::ostream& operator<<(std::ostream& out, const VolumeMetadataRefreshInfo& t)
{
  return out << "{.most_recent_ids_slot=" << t.most_recent_ids_slot
             << ",  .most_recent_attach_slot=" << batt::dump_range(t.most_recent_attach_slot)
             << ",}";
}

/** \brief Appends any Volume metadata that will be lost when trimmed_region is trimmed to the end
 * of the log using the passed grant.
 */
Status refresh_volume_metadata(TypedSlotWriter<VolumeEventVariant>& slot_writer, batt::Grant& grant,
                               VolumeMetadataRefreshInfo& refresh_info,
                               VolumeTrimmedRegionInfo& trimmed_region);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Information about a durably committed PackedVolumeTrimEvent.
 */
struct VolumeTrimEventInfo {
  SlotRange trim_event_slot;
  SlotRange trimmed_region_slot_range;
};

inline std::ostream& operator<<(std::ostream& out, const VolumeTrimEventInfo& t)
{
  return out << "{.trim_event_slot=" << t.trim_event_slot
             << ", .trimmed_region_slot_range=" << t.trimmed_region_slot_range << ",}";
}

/** \brief Writes a trim event slot to the volume log.
 */
StatusOr<VolumeTrimEventInfo> write_trim_event(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                               batt::Grant& grant,
                                               VolumeTrimmedRegionInfo& trimmed_region);

/** \brief Decrement ref counts of obsolete roots in the given trimmed region and trim the log.
 */
Status trim_volume_log(TypedSlotWriter<VolumeEventVariant>& slot_writer, batt::Grant& grant,
                       Optional<VolumeTrimEventInfo>&& trim_event,
                       VolumeTrimmedRegionInfo&& trimmed_region,
                       const VolumeDropRootsFn& drop_roots,
                       VolumePendingJobsUMap& prior_pending_jobs);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Runs in the background, trimming a single Volume's main log as needed.
 */
class VolumeTrimmer
{
 public:
  /** \brief Reconstructs trimmer state during crash recovery.
   */
  class RecoveryVisitor;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static VolumeDropRootsFn make_default_drop_roots_fn(PageCache& cache, PageRecycler& recycler,
                                                      const boost::uuids::uuid& trimmer_uuid);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit VolumeTrimmer(const boost::uuids::uuid& trimmer_uuid, std::string&& name,
                         SlotLockManager& trim_control, TrimDelayByteCount trim_delay,
                         std::unique_ptr<LogDevice::Reader>&& log_reader,
                         TypedSlotWriter<VolumeEventVariant>& slot_writer,
                         VolumeDropRootsFn&& drop_roots,
                         const RecoveryVisitor& recovery_visitor) noexcept;

  VolumeTrimmer(const VolumeTrimmer&) = delete;
  VolumeTrimmer& operator=(const VolumeTrimmer&) = delete;

  ~VolumeTrimmer() noexcept;

  const boost::uuids::uuid& uuid() const noexcept
  {
    return this->trimmer_uuid_;
  }

  std::string_view name() const noexcept
  {
    return this->name_;
  }

  /** \brief Adds the given grant to the trim event grant held by this object, which is used to
   * append the log.
   */
  void push_grant(batt::Grant&& grant) noexcept;

  void halt();

  Status run();

  u64 grant_pool_size() const noexcept
  {
    return this->trimmer_grant_.size();
  }

  u64 trim_count() const noexcept
  {
    return this->trim_count_.load();
  }

  u64 pushed_grant_size() const noexcept
  {
    return this->pushed_grant_size_.load();
  }

  u64 popped_grant_size() const noexcept
  {
    return this->popped_grant_size_.load();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  /** \brief Blocks the caller until it is safe to trim the log up to the specified offset (or some
   * higher offset).
   *
   * This function takes the trim delay into account; the trim control SlotLockManager must indicate
   * that there are no locks less than `min_offset` + `trim_delay_`.
   *
   * \return the new candidate trim pos on success; error status code otherwise
   */
  StatusOr<slot_offset_type> await_trim_target(slot_offset_type min_offset);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The unique identifier for this trimmer; used to prevent page refcount
   * double-updates.
   */
  const boost::uuids::uuid trimmer_uuid_;

  /** \brief Human-readable name for this object, for diagnostics.
   */
  std::string name_;

  /** \brief The lock manager that determines when it is safe to trim part of the log.
   */
  SlotLockManager& trim_control_;

  /** \brief The number of bytes by which to delay trimming.
   */
  const TrimDelayByteCount trim_delay_;

  /** \brief Used to scan the log as it is trimmed.
   */
  std::unique_ptr<LogDevice::Reader> log_reader_;

  /** \brief Created from this->log_reader_.
   */
  TypedSlotReader<VolumeEventVariant> slot_reader_;

  /** \brief The shared slot writer for the Volume's main log.
   */
  TypedSlotWriter<VolumeEventVariant>& slot_writer_;

  /** \brief Passed in at creation time; responsible for durably releasing PageId roots as the log
   * is trimmed.
   */
  VolumeDropRootsFn drop_roots_;

  /** \brief Used for all log appends performed by this task.
   */
  batt::Grant trimmer_grant_;

  /** \brief Set to true by VolumeTrimmer::halt(); indicates whether the trimmer is in "shutdown"
   * mode.
   */
  std::atomic<bool> halt_requested_{false};

  /** \brief Contains all PrepareJobs that haven't been resolved yet by a corresponding commit or
   * rollback.
   */
  VolumePendingJobsUMap pending_jobs_;

  /** \brief Contains the last known slot where Volume metadata (ids and attachments) was refreshed.
   */
  VolumeMetadataRefreshInfo refresh_info_;

  /** \brief When present, contains information read from the log region currently being trimmed.
   */
  Optional<VolumeTrimmedRegionInfo> trimmed_region_info_;

  /** \brief When present, contains information about the most recent durable TrimEvent slot.
   */
  Optional<VolumeTrimEventInfo> latest_trim_event_;

  /** \brief The number of trim operations completed.
   */
  std::atomic<u64> trim_count_{0};

  /** \brief The number of bytes of grant obtained via push_grant.
   */
  std::atomic<u64> pushed_grant_size_{0};

  /** \brief The number of push_grant-obtained grant bytes that have been released via a trim.
   */
  std::atomic<u64> popped_grant_size_{0};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeTrimmer::RecoveryVisitor : public VolumeEventVisitor<Status>
{
 public:
  explicit RecoveryVisitor(slot_offset_type trim_pos) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // VolumeEventVisitor methods.
  //
  Status on_raw_data(const SlotParse&, const Ref<const PackedRawData>&) override;

  Status on_prepare_job(const SlotParse&, const Ref<const PackedPrepareJob>&) override;

  Status on_commit_job(const SlotParse&, const PackedCommitJob&) override;

  Status on_rollback_job(const SlotParse&, const PackedRollbackJob&) override;

  Status on_volume_attach(const SlotParse& slot, const PackedVolumeAttachEvent& attach) override;

  Status on_volume_detach(const SlotParse& slot, const PackedVolumeDetachEvent& detach) override;

  Status on_volume_ids(const SlotParse& slot, const PackedVolumeIds&) override;

  Status on_volume_recovered(const SlotParse&, const PackedVolumeRecovered&) override;

  Status on_volume_format_upgrade(const SlotParse&, const PackedVolumeFormatUpgrade&) override;

  Status on_volume_trim(const SlotParse&, const VolumeTrimEvent&) override;
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the current last-known refresh information for all Volume metadata.
   */
  const VolumeMetadataRefreshInfo& get_refresh_info() const noexcept
  {
    return this->refresh_info_;
  }

  /** \brief Returns information about the most recent trim event slot.
   */
  const Optional<VolumeTrimEventInfo>& get_trim_event_info() const noexcept
  {
    return this->trim_event_info_;
  }

  /** \brief Returns the page transaction jobs that have been trimmed but not yet resolved, indexed
   * by their prepare slot.
   */
  const VolumePendingJobsUMap& get_pending_jobs() const noexcept
  {
    return this->pending_jobs_;
  }

  /** \brief Returns the size of the grant required by the VolumeTrimmer task.
   */
  usize get_trimmer_grant_size() const noexcept
  {
    return this->trimmer_grant_size_;
  }

 private:
  slot_offset_type log_trim_pos_;
  VolumeMetadataRefreshInfo refresh_info_;
  Optional<VolumeTrimEventInfo> trim_event_info_;
  VolumePendingJobsUMap pending_jobs_;
  usize trimmer_grant_size_ = 0;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_TRIMMER_HPP

#include <llfs/volume_trimmer.ipp>
