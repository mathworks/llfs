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

}  // namespace llfs

#endif  // LLFS_FILESYSTEM_HPP
