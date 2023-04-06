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

#include <llfs/config.hpp>
//
#ifndef LLFS_DISABLE_IO_URING

#include <llfs/confirm.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring.hpp>
#include <llfs/ioring_log_config.hpp>
#include <llfs/packed_log_page_buffer.hpp>
#include <llfs/raw_block_file.hpp>

#include <batteries/async/handler.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/status.hpp>

#include <atomic>
#include <vector>

namespace llfs {

// Write an empty log device to the given fd.
//
Status initialize_ioring_log_device(RawBlockFile& file, const IoRingLogConfig& config,
                                    ConfirmThisWillEraseAllMyData confirm);

template <typename IoRingImpl>
class BasicIoRingLogInitializer;

using IoRingLogInitializer = BasicIoRingLogInitializer<IoRing>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename IoRingImpl>
class BasicIoRingLogInitializer
{
 public:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Subtask {
    // Must be first (to guarantee that each Subtask state is cpu-cache-line-isolated); the buffer
    // to hold log page header information.
    //
    PackedLogPageBuffer buffer;

    // The BasicIoRingLogInitializer object to which this Subtask belongs.
    //
    BasicIoRingLogInitializer* that = nullptr;

    // The current destination file offset to which this Subtask is writing information.
    //
    i64 file_offset = 0;

    // The number of bytes that have been written in the current block.
    //
    i32 block_progress = sizeof(this->buffer);

    // Embedded memory for handler-related allocation.
    //
    batt::HandlerMemory<128> handler_memory;

    // Used to aggregate the success/error disposition of the initializer.  Should be set only once
    // per Subtask.
    //
    batt::Status final_status;

    // Set to `true` once the Subtask finishes.
    //
    bool done = false;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    //
    //
    void start_write();

    void handle_write(const batt::StatusOr<i32>& n_written);

    void finish(const batt::Status& status);

    usize self_index() const;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BasicIoRingLogInitializer(usize n_tasks, typename IoRingImpl::File& file,
                                     const IoRingLogConfig& config, u64 n_blocks_to_init) noexcept;

  batt::Status run();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  // The file to which this initializer is writing log information.
  //
  typename IoRingImpl::File& file_;

  // The configuration of the log which this object is initializing.
  //
  IoRingLogConfig config_;

  // The next log block index which hasn't been assigned to a Subtask; used by the Subtasks to
  // coordinate the distribution of work.
  //
  std::atomic<usize> next_block_i_{0};

  // Subtasks are responsible to increment this variable by 1 when they are finished; this is how
  // the initializer knows when we are done.
  //
  batt::Watch<usize> finished_count_{0};

  // The Subtask states.
  //
  std::vector<Subtask> subtasks_;

  // The maximum number of block headers to initialize.
  //
  u64 n_blocks_to_init_;
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_IORING_LOG_INITIALIZER_HPP
