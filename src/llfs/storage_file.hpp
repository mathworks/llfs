//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_FILE_HPP
#define LLFS_STORAGE_FILE_HPP

#include <llfs/raw_block_device.hpp>
#include <llfs/status.hpp>
#include <llfs/storage_file_config_block.hpp>

#include <memory>
#include <vector>

namespace llfs {

StatusOr<std::vector<std::unique_ptr<StorageFileConfigBlock>>> read_storage_file(
    RawBlockDevice& file, i64 start_offset);

// Status write_storage_file_

}  // namespace llfs

#endif  // LLFS_STORAGE_FILE_HPP
