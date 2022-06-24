//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_FILE_CONFIG_BLOCK_HPP
#define LLFS_STORAGE_FILE_CONFIG_BLOCK_HPP

#include <llfs/file_offset_ptr.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/raw_block_file.hpp>

#include <cstring>
#include <type_traits>

namespace llfs {

class StorageFileConfigBlock
{
 public:
  static StatusOr<std::unique_ptr<StorageFileConfigBlock>> read_from_fd(int fd, i64 offset);

  static StatusOr<std::unique_ptr<StorageFileConfigBlock>> read_from_raw_block_file(
      RawBlockFile& file, i64 offset);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit StorageFileConfigBlock(i64 file_offset) noexcept;

  StorageFileConfigBlock(const StorageFileConfigBlock&) = delete;
  StorageFileConfigBlock& operator=(const StorageFileConfigBlock&) = delete;

  bool is_dirty() const
  {
    return this->dirty_;
  }

  i64 file_offset() const
  {
    return this->get_const_ptr().file_offset;
  }

  PackedConfigBlock& get_mutable()
  {
    this->dirty_ = true;
    return *(reinterpret_cast<PackedConfigBlock*>(&this->block_));
  }

  const PackedConfigBlock& get_const() const
  {
    return *(reinterpret_cast<const PackedConfigBlock*>(&this->block_));
  }

  FileOffsetPtr<PackedConfigBlock&> get_mutable_ptr() const
  {
    return this->ptr_;
  }

  const FileOffsetPtr<PackedConfigBlock&>& get_const_ptr() const
  {
    return this->ptr_;
  }

 private:
  // If true, the current value of `block` needs to be written back to the file.
  //
  bool dirty_{true};

  std::aligned_storage_t<sizeof(PackedConfigBlock), alignof(PackedConfigBlock)> block_;

  FileOffsetPtr<PackedConfigBlock&> ptr_;
};

}  // namespace llfs

#endif  // LLFS_STORAGE_FILE_CONFIG_BLOCK_HPP
