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
#include <llfs/seq.hpp>
#include <llfs/status.hpp>
#include <llfs/storage_file_config_block.hpp>

#include <batteries/shared_ptr.hpp>

#include <memory>
#include <string>
#include <vector>

namespace llfs {

StatusOr<std::vector<std::unique_ptr<StorageFileConfigBlock>>> read_storage_file(
    RawBlockDevice& file, i64 start_offset);

class StorageFile : public batt::RefCounted<StorageFile>
{
 public:
  explicit StorageFile(
      std::string&& file_name,
      std::vector<std::unique_ptr<StorageFileConfigBlock>>&& config_blocks) noexcept;

  const std::string& file_name() const
  {
    return this->file_name_;
  }

  template <typename PackedConfigT>
  BoxedSeq<FileOffsetPtr<const PackedConfigT&>> find_objects_by_type(
      batt::StaticType<PackedConfigT> = {});

 private:
  std::string file_name_;
  std::vector<std::unique_ptr<StorageFileConfigBlock>> config_blocks_;
};

}  // namespace llfs

#include <llfs/storage_file.ipp>

#endif  // LLFS_STORAGE_FILE_HPP
