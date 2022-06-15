//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FILE_LAYOUT_BUILDER_IPP
#define LLFS_FILE_LAYOUT_BUILDER_IPP

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
StatusOr<FileOffsetPtr<T&>> FileLayoutBuilder::Transaction::add_slot(
    batt::StaticType<T> static_type)
{
  BATT_CHECK(this->active_);

  BATT_STATIC_ASSERT_EQ(sizeof(T), sizeof(PackedConfigSlot));

  T* slot = this->packer_.pack_record(static_type);
  if (!slot) {
    this->block_is_full_ = true;
    return None;
  }
  slot->tag = PackedConfigTagFor<T>::value;

  FileOffsetPtr<PackedConfigSlot&> p_slot = this->p_config_block_.mutable_slot(
      config_block.object.slots.item_count + this->slots_pending_);

  this->slots_pending_ += 1;

  return FileOffsetPtr<T&>{
      .object = *reinterpret_cast(&p_slot.object),
      .file_offset = p_slot.offset,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R>
StatusOr<R> FileLayoutBuilder::apply_transaction(const std::function<StatusOr<R>(Transaction&)>& fn)
{
  for (usize attempts = 0; attempts < 2; ++attempts) {
    Transaction t{*this};

    StatusOr<R> result = fn(t);

    if (result.ok()) {
      t.commit();
    } else {
      t.abort();
      if (attempts == 0 && result.status() == batt::StatusCode::kResourceExhausted) {
        continue;
      }
    }

    return result;
  }
}

}  // namespace llfs

#endif  // LLFS_FILE_LAYOUT_BUILDER_IPP
