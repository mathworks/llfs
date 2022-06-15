//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FILE_LOG_DRIVER_FLUSH_TASK_MAIN_HPP
#define LLFS_FILE_LOG_DRIVER_FLUSH_TASK_MAIN_HPP

namespace llfs {

// Stateful task function that loops flushing committed data to segment files.
//
class FileLogDriver::FlushTaskMain
{
 public:
  explicit FlushTaskMain(const RingBuffer& buffer, ConcurrentSharedState& shared_state,
                         ActiveFile&& active_file) noexcept;

  // The flush task main loop entry point.
  //
  void operator()();

 private:  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Read-only access to the ring buffer contents.
  //
  const RingBuffer& buffer_;

  // Thread-safe shared state used to communicate with committers and trimmers.
  //
  ConcurrentSharedState& shared_state_;

  // The current active segment file.
  //
  ActiveFile active_file_;
};

}  // namespace llfs

#endif  // LLFS_FILE_LOG_DRIVER_FLUSH_TASK_MAIN_HPP
