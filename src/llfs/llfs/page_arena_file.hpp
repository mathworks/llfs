#pragma once
#ifndef LLFS_PAGE_ARENA_FILE_HPP
#define LLFS_PAGE_ARENA_FILE_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/confirm.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/file_segment_ref.hpp>
#include <llfs/ioring.hpp>
#include <llfs/ioring_log_device.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_arena.hpp>
#include <llfs/page_arena_config.hpp>
#include <llfs/read_only.hpp>

#include <batteries/async/task_scheduler.hpp>

namespace llfs {

struct PageArenaConfigInFile {
  FileSegmentRef file_ref;
  PackedPageArenaConfig config;
};

// Open the passed file and read all PageArena configs it contains.
//
StatusOr<std::vector<std::unique_ptr<PageArenaConfigInFile>>> read_arena_configs_from_file(
    const std::string& filename);

// We must make this struct large enough to use with O_DIRECT.
//

struct PageArenaFileRuntimeOptions {
  std::string name = "anonymous";
  ReadOnly read_only = ReadOnly::kFalse;
  usize log_queue_depth = 16;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> page_arena_from_file(batt::TaskScheduler& scheduler, IoRing& ioring,
                                         const FileSegmentRef& file_ref,
                                         const PageArenaFileRuntimeOptions& options,
                                         PackedPageArenaConfig* config_out = nullptr);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> initialize_page_arena_file(batt::TaskScheduler& scheduler, IoRing& ioring,
                                               const FileSegmentRef& file_ref,
                                               const PageArenaFileRuntimeOptions& options,
                                               const PackedPageArenaConfig& config,
                                               ConfirmThisWillEraseAllMyData confirm);

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_PAGE_ARENA_FILE_HPP
