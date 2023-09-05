//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_JOB_RECOVERY_VISITOR_HPP
#define LLFS_VOLUME_JOB_RECOVERY_VISITOR_HPP

#include <llfs/config.hpp>
//
#include <llfs/optional.hpp>
#include <llfs/status.hpp>
#include <llfs/volume_pending_jobs_map.hpp>
#include <llfs/volume_reader.hpp>
#include <llfs/volume_slot_demuxer.hpp>

#include <map>
#include <unordered_set>

namespace llfs {

class VolumeJobRecoveryVisitor : public VolumeEventVisitor<Status>::NullImpl
{
 public:
  explicit VolumeJobRecoveryVisitor() noexcept;

  //----- --- -- -  -  -   -

  void reset_pending_jobs()
  {
    this->pending_jobs_.clear();
  }

  const VolumePendingJobsMap& get_pending_jobs() const noexcept
  {
    return this->pending_jobs_;
  }

  Status resolve_pending_jobs(PageCache& cache, PageRecycler& recycler,
                              const boost::uuids::uuid& volume_uuid,
                              TypedSlotWriter<VolumeEventVariant>& slot_writer);

  //----- --- -- -  -  -   -

  Status on_prepare_job(const SlotParse&, const Ref<const PackedPrepareJob>&) override;

  Status on_commit_job(const SlotParse&, const Ref<const PackedCommitJob>&) override;

  Status on_rollback_job(const SlotParse&, const PackedRollbackJob&) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  // The pending jobs for this volume.
  //
  VolumePendingJobsMap pending_jobs_;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_JOB_RECOVERY_VISITOR_HPP

#include <llfs/volume_job_recovery_visitor.ipp>
