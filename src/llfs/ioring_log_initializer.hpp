//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_INITIALIZER_HPP
#define LLFS_IORING_LOG_INITIALIZER_HPP

#include <llfs/ioring_log_device.hpp>

#include <batteries/async/handler.hpp>

#include <atomic>
#include <vector>

namespace llfs {

class IoRingLogInitializer
{
 public:
  struct Subtask {
    IoRingLogDriver::PackedPageHeaderBuffer buffer;
    IoRingLogInitializer* that = nullptr;
    i64 file_offset = 0;
    i32 block_progress = sizeof(this->buffer);
    batt::HandlerMemory<64> handler_memory;
    batt::Status final_status;
    bool done = false;

    void start_write();

    void handle_write(const batt::StatusOr<i32>& n_written);

    void finish(const batt::Status& status);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingLogInitializer(usize n_tasks, IoRing::File& file,
                                const IoRingLogDriver::Config& config) noexcept;

  batt::Status run();

 private:
  IoRing::File& file_;
  IoRingLogDriver::Config config_;
  std::atomic<usize> next_block_i_{0};
  batt::Watch<usize> finished_count_{0};
  std::vector<Subtask> subtasks_;
};

}  // namespace llfs

#endif  // LLFS_IORING_LOG_INITIALIZER_HPP
