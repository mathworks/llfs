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
  explicit VolumeTrimmer(PageCache& cache, SlotLockManager& trim_control, PageRecycler& recycler,
                         const boost::uuids::uuid& trimmer_uuid,
                         std::unique_ptr<LogDevice::Reader>&& log_reader,
                         TypedSlotWriter<VolumeEventVariant>& slot_writer) noexcept;

  VolumeTrimmer(const VolumeTrimmer&) = delete;
  VolumeTrimmer& operator=(const VolumeTrimmer&) = delete;

  const boost::uuids::uuid& uuid() const
  {
    return this->trimmer_uuid_;
  }

  void halt();

  Status run();

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCache& cache_;
  PageRecycler& recycler_;
  boost::uuids::uuid trimmer_uuid_;
  SlotLockManager& trim_control_;
  std::unique_ptr<LogDevice::Reader> log_reader_;
  TypedSlotReader<VolumeEventVariant> slot_reader_;
  TypedSlotWriter<VolumeEventVariant>& slot_writer_;
  std::unordered_map<slot_offset_type /*prepare_slot*/, std::vector<PageId>> roots_per_pending_job_;
  std::vector<PageId> obsolete_roots_;

  // Volume metadata found in the last GC'ed log segment that must be refreshed before trimming.
  //
  Optional<PackedVolumeIds> ids_to_refresh_;

  // PageDevice attachments found in the last GC'ed log segment that must be refreshed before
  // trimming.
  //
  std::unordered_set<PackedVolumeAttachEvent, PackedVolumeAttachEvent::Hash>
      attachments_to_refresh_;

  std::unordered_set<PackedVolumeDetachEvent, PackedVolumeDetachEvent::Hash>
      detachments_to_refresh_;

  batt::Grant id_refresh_grant_;
  std::atomic<bool> halt_requested_{false};
};

}  // namespace llfs

#endif  // LLFS_VOLUME_TRIMMER_HPP

#include <llfs/volume_trimmer.ipp>
