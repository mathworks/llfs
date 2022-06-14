#include <llfs/storage_file.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::vector<std::unique_ptr<StorageFileConfigBlock>>>
read_storage_file_config_blocks_from_fd(int fd, i64 start_offset)
{
  std::vector<std::unique_ptr<StorageFileConfigBlock>> blocks;

  i64 offset = start_offset;
  for (;;) {
    StatusOr<std::unique_ptr<StorageFileConfigBlock>> next_block =
        StorageFileConfigBlock::read_from_fd(fd, offset);
    BATT_REQUIRE_OK(next_block);

    const PackedConfigBlock& block = next_block->get()->get_const();
    if (block.magic != PackedConfigBlock::kMagic) {
      LOG(WARNING) << "The magic number is wrong!";
      return {batt::StatusCode::kDataLoss};
    }
    if (block.crc64 != block.true_crc64()) {
      LOG(WARNING) << "The crc64 doesn't match!";
      return {batt::StatusCode::kDataLoss};
    }

    blocks.emplace_back(std::move(*next_block));
    const i64 next_offset = blocks.back()->get_const().next_offset;
    if (next_offset == PackedConfigBlock::kNullFileOffset || next_offset == 0) {
      break;
    }

    offset += next_offset;
  }

  return blocks;
}

}  // namespace llfs
