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

#include <llfs/config.hpp>
//
#include <llfs/log_device.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/slot_lock_manager.hpp>
#include <llfs/slot_writer.hpp>
#include <llfs/status.hpp>
#include <llfs/volume_cancelled_job_tracker.hpp>
#include <llfs/volume_event_visitor.hpp>
#include <llfs/volume_events.hpp>
#include <llfs/volume_metadata_refresher.hpp>
#include <llfs/volume_trimmed_region_info.hpp>
#include <llfs/volume_trimmer_metrics.hpp>

#include <atomic>
#include <unordered_map>
#include <unordered_set>

namespace llfs {

/** \brief Consumer of dropped root page refs.
 */
using VolumeDropRootsFn = std::function<batt::Status(const boost::uuids::uuid& trimmer_uuid,
                                                     slot_offset_type trim_event_slot_offset,
                                                     Slice<const PageId> root_ref_page_ids)>;

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
             << ", .trimmed_region_slot_range=" << t.trimmed_region_slot_range
             << "(size=" << t.trimmed_region_slot_range.size() << "),}";
}

/** \brief Writes a trim event slot to the volume log.
 */
StatusOr<VolumeTrimEventInfo> write_trim_event(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                               batt::Grant& grant,
                                               const VolumeTrimmedRegionInfo& trimmed_region);

/** \brief Decrement ref counts of obsolete roots in the given trimmed region and trim the log.
 */
Status trim_volume_log(const boost::uuids::uuid& trimmer_uuid,
                       TypedSlotWriter<VolumeEventVariant>& slot_writer, batt::Grant& grant,
                       Optional<VolumeTrimEventInfo>&& trim_event,
                       VolumeTrimmedRegionInfo&& trimmed_region,
                       VolumeMetadataRefresher& metadata_refresher,
                       const VolumeDropRootsFn& drop_roots);

// Forward-declaration.
//
class VolumeTrimmerRecoveryVisitor;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Runs in the background, trimming a single Volume's main log as needed.
 */
class VolumeTrimmer
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using Metrics = VolumeTrimmerMetrics;

  static Metrics& metrics() noexcept
  {
    static Metrics metrics_;
    return metrics_;
  }

  /** \brief Creates and returns a function to drop trimmed page root refs.
   */
  static VolumeDropRootsFn make_default_drop_roots_fn(PageCache& cache, PageRecycler& recycler);

  static StatusOr<std::unique_ptr<VolumeTrimmer>> recover(
      const boost::uuids::uuid& trimmer_uuid,            //
      std::string&& name,                                //
      TrimDelayByteCount trim_delay,                     //
      LogDevice& volume_root_log,                        //
      TypedSlotWriter<VolumeEventVariant>& slot_writer,  //
      VolumeDropRootsFn&& drop_roots,                    //
      SlotLockManager& trim_control,                     //
      VolumeMetadataRefresher& metadata_refresher);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit VolumeTrimmer(const boost::uuids::uuid& trimmer_uuid,            //
                         std::string&& name,                                //
                         SlotLockManager& trim_control,                     //
                         TrimDelayByteCount trim_delay,                     //
                         std::unique_ptr<LogDevice::Reader>&& log_reader,   //
                         TypedSlotWriter<VolumeEventVariant>& slot_writer,  //
                         VolumeDropRootsFn&& drop_roots_fn,                 //
                         VolumeMetadataRefresher& metadata_refresher) noexcept;

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

  /** \brief Contains the last known slot(s) where Volume metadata (ids and attachments) was
   * refreshed, and takes care of refreshing this information on trim.
   */
  VolumeMetadataRefresher& metadata_refresher_;

  /** \brief The number of trim operations completed.
   */
  std::atomic<u64> trim_count_{0};
};

}  // namespace llfs

#endif  // LLFS_VOLUME_TRIMMER_HPP

#include <llfs/volume_trimmer.ipp>
