//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_FILE_BUILDER_HPP
#define LLFS_STORAGE_FILE_BUILDER_HPP

#include <llfs/data_packer.hpp>
#include <llfs/file_offset_ptr.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/raw_block_file.hpp>
#include <llfs/storage_file_config_block.hpp>

#include <batteries/type_traits.hpp>

#include <vector>

namespace llfs {

class StorageFileBuilder
{
 public:
  using PreFlushAction = std::function<Status(RawBlockFile&)>;

  class Transaction;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit StorageFileBuilder(RawBlockFile& file, i64 base_offset,
                              page_device_id_int next_available_device_id = 0) noexcept;

  // Add a storage object to the file.
  //
  template <typename ConfigOptionsT,
            typename PackedConfigT = typename ConfigOptionsT::PackedConfigType>
  StatusOr<FileOffsetPtr<const PackedConfigT&>> add_object(const ConfigOptionsT& options);

  // Flush all storage object configs to the underlying storage file.
  //
  Status flush_all();

 private:
  // Atomically apply the given transaction.  `fn` must be safe to re-run (i.e. side-effects must be
  // idempotent).
  //
  template <typename R>
  StatusOr<R> transact(const std::function<StatusOr<R>(Transaction&)>& fn);

  // Allocate a new config block placed at the end of the storage file (`this->next_offset_`).
  //
  FileOffsetPtr<PackedConfigBlock&> allocate_config_block();

  // "Closes" the current config block for updates.  Does not automatically allocate a new block
  // (this is done lazily when the next Transaction is created).
  //
  void finalize_config_block();

  // Writes the given config block to the file.
  //
  Status flush_block(const FileOffsetPtr<PackedConfigBlock&>& p_config_block);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  RawBlockFile& file_;
  std::vector<std::unique_ptr<StorageFileConfigBlock>> config_blocks_;
  Optional<MutableBuffer> unused_payload_;
  i64 base_offset_;
  i64 next_offset_;
  page_device_id_int next_available_device_id_;
  std::vector<PreFlushAction> pre_flush_actions_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class StorageFileBuilder::Transaction
{
 public:
  explicit Transaction(StorageFileBuilder& builder) noexcept;

  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  ~Transaction() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Add a storage object to this transaction.
  //
  template <typename ConfigOptionsT,
            typename PackedConfigT = typename ConfigOptionsT::PackedConfigType>
  StatusOr<FileOffsetPtr<const PackedConfigT&>> add_object(const ConfigOptionsT& options);

  // Add a config slot to the current block.  This method allocates the next available slot in the
  // file's current non-full config block.
  //
  // Returns a mutable reference to the packed config slot; any changes made to this slot prior to
  // committing this transaction will be included in the written config block (see also
  // `Transaction::packer()`).
  //
  template <typename ConfigOptionsT,
            typename PackedConfigT = typename ConfigOptionsT::PackedConfigType>
  StatusOr<FileOffsetPtr<PackedConfigT&>> add_config_slot(const ConfigOptionsT& options);

  // Reserves the specified amount, aligned to (2^align_bits), returning the half-closed interval
  // of the reserved region in byte offset from the beginning of the file (absolute file offset).
  //
  batt::Interval<i64> reserve_aligned(i32 align_bits, i64 amount);

  // Add an I/O action to be performed before the transaction is committed by writing out the
  // config blocks.
  //
  void require_pre_flush_action(PreFlushAction&& action);

  // Return the next available monotonic device id number.
  //
  page_device_id_int reserve_device_id();

  // Returns a mutable reference to the DataPacker for the current config block.  This allows the
  // use of extra space at the end of the block for variable-sized data fields referenced from
  // config slots.
  //
  DataPacker& packer();

  // Cancel this transaction and roll back any side-effects.
  //
  void abort();

  // Add this transaction to the list of changes to be written to the storage file.
  //
  void commit();

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The builder object that owns this transaction.
  //
  StorageFileBuilder& builder_;

  // The current config block in which the transaction takes place; a transaction may only affect
  // a single config block.
  //
  const FileOffsetPtr<PackedConfigBlock&> p_config_block_;

  // (convenience) The `PackedConfigBlock` referenced by `this->p_config_block_`.
  //
  PackedConfigBlock& config_block_;

  // Used to pack data into the payload field of the config block.
  //
  DataPacker packer_;

  // The number of slots added to the config block in this transaction.
  //
  usize n_slots_added_;

  // True iff this transaction has not been committed or aborted yet.
  //
  bool active_;

  // Set to true if the data packer fails because it has run out of space.
  //
  bool payload_overflow_;

  // The next available offset in the storage file; used by `reserve_aligned`.
  //
  i64 next_offset_;

  // The next available device number; used by `reserve_device_id`.
  //
  page_device_id_int next_available_device_id_;

  // I/O actions that must succeed before flushing this config block; used to initialize data in
  // the storage file that is referenced by config slots.
  //
  std::vector<PreFlushAction> pre_flush_actions_;
};

}  // namespace llfs

#include <llfs/storage_file_builder.ipp>

#endif  // LLFS_STORAGE_FILE_BUILDER_HPP
