//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// see https://www.apache.org/licenses/LICENSE-2.0 for license information.
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_APPENDABLE_JOB_HPP
#define LLFS_APPENDABLE_JOB_HPP

#include <llfs/finalized_page_cache_job.hpp>
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
};

// Construct an AppendableJob.
//
StatusOr<AppendableJob> make_appendable_job(std::unique_ptr<PageCacheJob>&& job,
                                            PackableRef&& user_data);

// Create a prepare event from an AppendableJob to commit it to the log via 2pc.
//
PrepareJob prepare(const AppendableJob& appendable);

}  // namespace llfs

#endif  // LLFS_APPENDABLE_JOB_HPP
