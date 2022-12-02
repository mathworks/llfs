//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_file.hpp>
//

#include <llfs/status_code.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::vector<std::unique_ptr<StorageFileConfigBlock>>> read_storage_file(RawBlockFile& file,
                                                                                 i64 start_offset)
{
  std::vector<std::unique_ptr<StorageFileConfigBlock>> blocks;

  const i64 aligned_start = batt::round_up_bits(PackedConfigBlock::kSizeLog2, start_offset);
  LLFS_LOG_WARNING_IF(aligned_start != start_offset)
      << "Specified `start_offset` is not aligned to " << PackedConfigBlock::kSize
      << "-byte boundary; rounding up to " << aligned_start;

  i64 offset = aligned_start;
  for (;;) {
    StatusOr<std::unique_ptr<StorageFileConfigBlock>> next_block =
        StorageFileConfigBlock::read_from_raw_block_file(file, offset);
    BATT_REQUIRE_OK(next_block);

    const PackedConfigBlock& block = next_block->get()->get_const();
    if (block.magic != PackedConfigBlock::kMagic) {
      LLFS_LOG_WARNING() << "The magic number is wrong!";
      return make_status(StatusCode::kStorageFileBadConfigBlockMagic);
    }
    if (block.crc64 != block.true_crc64()) {
      LLFS_LOG_WARNING() << "The crc64 doesn't match!";
      return make_status(StatusCode::kStorageFileBadConfigBlockCrc);
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ StorageFile::StorageFile(
    std::string&& file_name,
    std::vector<std::unique_ptr<StorageFileConfigBlock>>&& config_blocks) noexcept
    : file_name_{std::move(file_name)}
    , config_blocks_{std::move(config_blocks)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<FileOffsetPtr<const PackedConfigSlot&>> StorageFile::find_all_objects()
{
  return as_seq(this->config_blocks_)  //
         | seq::map([](const std::unique_ptr<StorageFileConfigBlock>& p_config_block) {
             return as_seq(boost::irange<usize>(0, p_config_block->get_const().slots.size()))  //
                    | seq::map([p_config_block = p_config_block.get()](
                                   usize slot_index) -> FileOffsetPtr<const PackedConfigSlot&> {
                        return p_config_block->get_const_ptr().get_slot(slot_index);
                      });
           })              //
         | seq::flatten()  //
         | seq::boxed();
}

}  // namespace llfs
