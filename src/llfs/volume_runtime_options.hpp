//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_RUNTIME_OPTIONS_HPP
#define LLFS_VOLUME_RUNTIME_OPTIONS_HPP

#include <llfs/ioring_log_driver_options.hpp>
#include <llfs/volume_reader.hpp>

#include <batteries/async/task_scheduler.hpp>

namespace llfs {

// Options used to recover a Volume from a StorageContext.
//
struct VolumeRuntimeOptions {
  // The scheduler used to launch all background tasks needed by the Volume.
  //
  batt::TaskScheduler* scheduler;

  // A slot visitor that will be invoked to rebuild the application-specific Volume state by
  // replaying the events found in the root log.
  //
  VolumeReader::SlotVisitorFn slot_visitor_fn;

  // Runtime options used to tune the behavior of the root log driver.
  //
  IoRingLogDriverOptions root_log_options;

  // Runtime options used to tune the behavior of the recycler log driver.
  //
  IoRingLogDriverOptions recycler_log_options;

  // (Optional) The SlotLockManager to use for trimming the recovered Volume's root log.  If
  // `nullptr`, a new SlotLockManager will be created.
  //
  std::unique_ptr<SlotLockManager> trim_control;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_RUNTIME_OPTIONS_HPP
