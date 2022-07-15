//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_file_builder.hpp>
//

#include <llfs/crc.hpp>
#include <llfs/ioring_log_device.hpp>
#include <llfs/page_allocator.hpp>
#include <llfs/page_recycler.hpp>

#include <batteries/math.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class StorageFileBuilder
//+++++++++++-+-+--+----- --- -- -  -  -   -

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StorageFileBuilder::StorageFileBuilder(RawBlockFile& file, i64 base_offset) noexcept
    : file_{file}
    , config_blocks_{}
    , unused_payload_{None}
    , base_offset_{base_offset}
    , next_offset_{base_offset}
    , next_available_device_id_{0}
    , pre_flush_actions_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status StorageFileBuilder::flush_all()
{
  // First run pre-flush actions to write any data referred to or required by the committed config
  // blocks.
  //
  for (auto action : this->pre_flush_actions_) {
    Status result = action(this->file_);
    BATT_REQUIRE_OK(result);
  }

  // Write the config blocks in reverse order so that we never see an incomplete chain on recovery.
  //
  while (!this->config_blocks_.empty()) {
    Status block_flush_status = this->flush_block(this->config_blocks_.back()->get_const_ptr());
    BATT_REQUIRE_OK(block_flush_status);

    this->config_blocks_.pop_back();
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status StorageFileBuilder::flush_block(const FileOffsetPtr<PackedConfigBlock&>& p_config_block)
{
  return write_all(this->file_, p_config_block.file_offset,
                   batt::buffer_from_struct(p_config_block.object));
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

  // NOTE: StorageFileConfigBlock's ctor will zero-initialize the PackedConfigBlock buffer.

  // Initialize constant fields.
  //
  p_block->magic = PackedConfigBlock::kMagic;
  p_block->version = PackedConfigBlock::kVersion;

  // Link this block into the chain of config blocks for this file.
  //
  if (this->config_blocks_.size() >= 2) {
    p_block.absolute_prev_offset(
        this->config_blocks_[this->config_blocks_.size() - 2]->file_offset());
  } else {
    p_block->prev_offset = PackedConfigBlock::kNullFileOffset;
  }
  p_block->next_offset = PackedConfigBlock::kNullFileOffset;

  // Set the payload section.
  //
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
  this->unused_payload_ = None;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class StorageFileBuilder::Transaction
//+++++++++++-+-+--+----- --- -- -  -  -   -

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
    , n_slots_added_{0}
    , active_{true}
    , payload_overflow_{false}
    , next_offset_{builder.next_offset_}
    , next_available_device_id_{builder.next_available_device_id_}
    , pre_flush_actions_{}
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
batt::Interval<i64> StorageFileBuilder::Transaction::reserve_aligned(i32 align_bits, i64 amount)
{
  i64 aligned_begin = batt::round_up_bits(align_bits, this->next_offset_);
  i64 aligned_end = batt::round_up_bits(align_bits, aligned_begin + amount);

  this->next_offset_ = aligned_end;

  return batt::Interval<i64>{
      .lower_bound = aligned_begin,
      .upper_bound = aligned_end,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageFileBuilder::Transaction::require_pre_flush_action(PreFlushAction&& action)
{
  this->pre_flush_actions_.emplace_back(std::move(action));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 StorageFileBuilder::Transaction::reserve_device_id()
{
  const u64 reserved_id = this->next_available_device_id_;
  this->next_available_device_id_ += 1;
  return reserved_id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DataPacker& StorageFileBuilder::Transaction::packer()
{
  return this->packer_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageFileBuilder::Transaction::abort()
{
  if (!this->active_) {
    return;
  }
  this->active_ = false;

  // Revert the packed slot count.
  //
  this->config_block_.slots.item_count -= this->n_slots_added_;

  if (this->payload_overflow_) {
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

  BATT_CHECK(!this->payload_overflow_);
  BATT_CHECK_EQ(&this->config_block_,
                this->builder_.config_blocks_.back()->get_mutable_ptr().get());

  this->config_block_.crc64 = this->config_block_.true_crc64();

  // Write state variables back to the builder object.
  //
  this->builder_.unused_payload_ = this->packer_.avail_buffer();
  this->builder_.next_offset_ = this->next_offset_;
  this->builder_.next_available_device_id_ = this->next_available_device_id_;

  this->builder_.pre_flush_actions_.insert(
      this->builder_.pre_flush_actions_.end(),
      std::make_move_iterator(this->pre_flush_actions_.begin()),
      std::make_move_iterator(this->pre_flush_actions_.end()));

  this->pre_flush_actions_.clear();
}

}  // namespace llfs
