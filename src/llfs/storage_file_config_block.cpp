//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_file_config_block.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<StorageFileConfigBlock>> StorageFileConfigBlock::read_from_fd(
    int fd, i64 offset)
{
  auto config_block = std::make_unique<StorageFileConfigBlock>(offset);

  Status read_status = config_block->ptr_.read_from_fd(fd);
  BATT_REQUIRE_OK(read_status);

  config_block->dirty_ = false;

  return config_block;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ StorageFileConfigBlock::StorageFileConfigBlock(i64 file_offset) noexcept
    : ptr_{this->get_mutable(), file_offset}
{
  std::memset(&this->block_, 0, sizeof(this->block_));
}

}  // namespace llfs
