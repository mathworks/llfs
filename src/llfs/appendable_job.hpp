//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// see https://www.apache.org/licenses/LICENSE-2.0 for license information.
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_APPENDABLE_JOB_HPP
#define LLFS_APPENDABLE_JOB_HPP

#include <llfs/committable_page_cache_job.hpp>
#include <llfs/packable_ref.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/status.hpp>

#include <memory>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Forward-declarations.
//
struct PrepareJob;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// A PageCacheJob with opaque user_data; can be appended to the root log of a Volume.
//
struct AppendableJob {
  CommittablePageCacheJob job;
  PackableRef user_data;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the total grant size needed to append both the PrepareJob and CommitJob events
   * for this job.
   */
  u64 calculate_grant_size() const noexcept;
};

// Construct an AppendableJob.
//
StatusOr<AppendableJob> make_appendable_job(std::unique_ptr<PageCacheJob>&& job,
                                            PackableRef&& user_data);

// Create a prepare event from an AppendableJob to commit it to the log via 2pc.
//
PrepareJob prepare(const AppendableJob& appendable);

/** \brief Used to compute grant required for a hypothetical job.
 */
struct JobSizeSpec {
  using Self = JobSizeSpec;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self from_job(const AppendableJob& job) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize user_data_size;
  usize root_page_ref_count;
  usize new_page_count;
  usize deleted_page_count;
  usize page_device_count;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize calculate_grant_size() const noexcept;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief For testing only; allows a job to be committed to page devices without having a Volume.
 */
Status unsafe_commit_job(std::unique_ptr<PageCacheJob>&& job) noexcept;

}  // namespace llfs

#endif  // LLFS_APPENDABLE_JOB_HPP
