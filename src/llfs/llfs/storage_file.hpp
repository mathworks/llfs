#pragma once
#ifndef LLFS_STORAGE_FILE_HPP
#define LLFS_STORAGE_FILE_HPP

#include <llfs/status.hpp>
#include <llfs/storage_file_config_block.hpp>

#include <memory>
#include <vector>

namespace llfs {

StatusOr<std::vector<std::unique_ptr<StorageFileConfigBlock>>>
read_storage_file_config_blocks_from_fd(int fd, i64 start_offset);

// Status write_storage_file_

}  // namespace llfs

#endif  // LLFS_STORAGE_FILE_HPP
