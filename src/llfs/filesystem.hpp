//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

// Utilities for dealing with the OS filesystem.
//
#pragma once
#ifndef LLFS_FILESYSTEM_HPP
#define LLFS_FILESYSTEM_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/status.hpp>

#include <filesystem>
#include <string_view>

namespace llfs {

namespace fs = std::filesystem;

BATT_STRONG_TYPEDEF(bool, OpenForAppend);

StatusOr<int> open_file_read_only(std::string_view file_name);

StatusOr<int> open_file_read_write(std::string_view file_name,
                                   OpenForAppend open_for_append = OpenForAppend{true});

StatusOr<int> create_file_read_write(std::string_view file_name,
                                     OpenForAppend open_for_append = OpenForAppend{true});

Status truncate_file(std::string_view file_name, u64 size);

Status truncate_fd(int fd, u64 size);

StatusOr<ConstBuffer> read_file(std::string_view file_name, MutableBuffer buffer, u64 offset = 0);

StatusOr<ConstBuffer> read_fd(int fd, MutableBuffer buffer, u64 offset);

Status write_fd(int fd, ConstBuffer buffer, u64 offset);

Status delete_file(std::string_view file_name);

StatusOr<i64> sizeof_file(std::string_view file_name);

StatusOr<i64> sizeof_fd(int fd);

BATT_STRONG_TYPEDEF(i32, EnableFileFlags);
BATT_STRONG_TYPEDEF(i32, DisableFileFlags);

StatusOr<i32> get_fd_flags(int fd);

Status set_fd_flags(int fd, i32 flags);

Status update_fd_flags(int fd, EnableFileFlags enable_flags, DisableFileFlags disable_flags);

StatusOr<i32> get_file_status_flags(int fd);

Status set_file_status_flags(int fd, i32 flags);

Status update_file_status_flags(int fd, EnableFileFlags enable_flags,
                                DisableFileFlags disable_flags);

Status enable_raw_io_fd(int fd, bool enabled = true);

}  // namespace llfs

#endif  // LLFS_FILESYSTEM_HPP
