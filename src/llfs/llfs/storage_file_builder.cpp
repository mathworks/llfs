#include <llfs/storage_file_builder.hpp>
//

#include <llfs/crc.hpp>
#include <llfs/ioring_log_device.hpp>
#include <llfs/page_allocator.hpp>
#include <llfs/page_recycler.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StorageFileBuilder::StorageFileBuilder(i64 base_offset) noexcept
    : base_offset_{base_offset}
    , next_offset_{base_offset}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StorageFileBuilder::Transaction::Transaction(StorageFileBuilder& builder) noexcept
    : builder_{builder}
    , p_config_block_{[&]() -> FileOffsetPtr<PackedConfigBlock&> {
      if (!builder.unused_payload_) {
        return builder.allocate_config_block();
      }
      BATT_CHECK(!builder.config_blocks_.empty());
      return builder.config_blocks_.back()->get_mutable_ptr();
    }()}
    , config_block_{this->p_config_block_.object}
    , packer_{*builder.unused_payload_}
    , slots_pending_{0}
    , active_{true}
    , block_is_full_{false}
    , next_offset_{builder.next_offset_}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StorageFileBuilder::Transaction::~Transaction() noexcept
{
  if (this->active_) {
    this->abort();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageFileBuilder::Transaction::abort()
{
  if (!this->active_) {
    return;
  }
  this->active_ = false;

  if (this->block_is_full_) {
    this->builder_.finalize_config_block();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageFileBuilder::Transaction::commit()
{
  if (!this->active_) {
    return;
  }
  this->active_ = false;

  BATT_CHECK(!this->block_is_full_);
  BATT_CHECK_EQ(&this->config_block_,
                this->builder_.config_blocks_.back()->get_mutable_ptr().get());

  this->config_block_.slots.item_count += this->slots_pending_;
  this->builder_.next_offset_ = this->next_offset_;
  this->builder_.unused_payload_ = this->packer_.avail_buffer();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FileOffsetPtr<PackedConfigBlock&> StorageFileBuilder::allocate_config_block()
{
  this->next_offset_ = round_up_to_page_size_multiple(this->next_offset_);

  this->config_blocks_.emplace_back(std::make_unique<StorageFileConfigBlock>(this->next_offset_));

  this->next_offset_ += sizeof(PackedConfigBlock);

  StorageFileConfigBlock& rec = *this->config_blocks_.back();
  FileOffsetPtr<PackedConfigBlock&> p_block = rec.get_mutable_ptr();

  p_block->magic = PackedConfigBlock::kMagic;
  p_block->version = PackedConfigBlock::kVersion;
  if (this->config_blocks_.size() > 1) {
    p_block.absolute_prev_offset(
        this->config_blocks_[this->config_blocks_.size() - 2]->file_offset());
  } else {
    p_block->prev_offset = PackedConfigBlock::kNullFileOffset;
  }
  p_block->next_offset = PackedConfigBlock::kNullFileOffset;

  this->unused_payload_.emplace(
      MutableBuffer{p_block->payload, PackedConfigBlock::kPayloadCapacity});

  return p_block;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageFileBuilder::finalize_config_block()
{
  if (!this->unused_payload_) {
    return;
  }
  BATT_CHECK(!this->config_blocks_.empty());
  {
    PackedConfigBlock& p_block = this->config_blocks_.back()->get_mutable();
    p_block.crc64 = p_block.true_crc64();
  }
  this->unused_payload_ = None;
}

}  // namespace llfs
