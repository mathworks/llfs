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
template <typename ConfigOptionsT, typename PackedConfigT>
StatusOr<FileOffsetPtr<const PackedConfigT&>> StorageFileBuilder::add_object(
    const ConfigOptionsT& options)
{
  return this->transact<FileOffsetPtr<const PackedConfigT&>>([&options](Transaction& txn) {
    return txn.add_object(options);
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R>
StatusOr<R> StorageFileBuilder::transact(const std::function<StatusOr<R>(Transaction&)>& fn)
{
  // Allow one retry so that `fn` gets at least one shot at succeeding with a completely fresh
  // config block.
  //
  for (usize attempts = 0; attempts < 2; ++attempts) {
    Transaction txn{*this};

    StatusOr<R> result = fn(txn);

    if (result.ok()) {
      txn.commit();
    } else {
      txn.abort();
      if (attempts == 0 && result.status() == batt::StatusCode::kResourceExhausted) {
        continue;
      }
    }

    return result;
  }

  BATT_UNREACHABLE();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ConfigOptionsT, typename PackedConfigT>
StatusOr<FileOffsetPtr<const PackedConfigT&>> StorageFileBuilder::Transaction::add_object(
    const ConfigOptionsT& options)
{
  StatusOr<FileOffsetPtr<PackedConfigT&>> p_packed_config = this->add_config_slot(options);
  BATT_REQUIRE_OK(p_packed_config);

  Status config_status = configure_storage_object(*this, *p_packed_config, options);
  BATT_REQUIRE_OK(config_status);

  return p_packed_config;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ConfigOptionsT, typename PackedConfigT>
StatusOr<FileOffsetPtr<PackedConfigT&>> StorageFileBuilder::Transaction::add_config_slot(
    const ConfigOptionsT& options)
{
  verify_config_slot_type_requirements<PackedConfigT>();

  BATT_CHECK(this->active_);

  PackedConfigT* packed_config = this->packer_.pack_record(batt::StaticType<PackedConfigT>{});
  if (!packed_config) {
    this->payload_overflow_ = true;
    return {batt::StatusCode::kResourceExhausted};
  }
  //
  // The PackedConfigBlock was zero-initialized by the ctor of StorageFileConfigBlock, so no need to
  // clear the packed_config.

  packed_config->tag = PackedConfigTagFor<PackedConfigT>::value;
  packed_config->slot_i = 0;
  packed_config->n_slots = sizeof(PackedConfigT) / sizeof(PackedConfigSlot);

  this->config_block_.slots.item_count += packed_config->n_slots;

  FileOffsetPtr<PackedConfigSlot&> p_slot = this->p_config_block_.mutable_slot(
      this->config_block_.slots.item_count - packed_config->n_slots);

  this->n_slots_added_ += packed_config->n_slots;

  return FileOffsetPtr<PackedConfigT&>{
      .object = *config_slot_cast<PackedConfigT>(&p_slot.object),
      .file_offset = p_slot.file_offset,
  };
}

}  // namespace llfs

#endif  // LLFS_FILE_LAYOUT_BUILDER_IPP
