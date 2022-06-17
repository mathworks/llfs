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
#include <llfs/raw_block_device.hpp>
#include <llfs/storage_file_config_block.hpp>

#include <batteries/type_traits.hpp>

#include <vector>

namespace llfs {

class StorageFileBuilder
{
 public:
  class Transaction
  {
   public:
    using PreCommitAction = std::function<Status(RawBlockDevice*)>;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit Transaction(StorageFileBuilder& builder) noexcept;

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    ~Transaction() noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    // Add a storage object to the file.  This method allocates the next available slot in the
    // file's current non-full config block.
    //
    // Returns a mutable reference to the packed config slot; any changes made to this slot prior to
    // committing this transaction will be included in the written config block (see also
    // `Transaction::packer()`).
    //
    template <typename ConfigOptionsT>
    StatusOr<FileOffsetPtr<typename ConfigOptionsT::PackedConfigType&>> add_object(
        const ConfigOptionsT& options);

    // Reserves the specified amount, aligned to (2^align_bits), returning the half-closed interval
    // of the reserved region in byte offset from the beginning of the file (absolute file offset).
    //
    batt::Interval<i64> reserve_aligned(u16 align_bits, i64 amount);

    // Add an I/O action to be performed before the transaction is committed by writing out the
    // config blocks.
    //
    void require_pre_commit_action(PreCommitAction&& action);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    void abort();

    void commit();

    u64 reserve_device_id();

    // Returns a mutable reference to the DataPacker for the current config block.  This allows the
    // use of extra space at the end of the block for variable-sized data fields referenced from
    // config slots.
    //
    DataPacker& packer();

   private:
    //+++++++++++-+-+--+----- --- -- -  -  -   -

    StorageFileBuilder& builder_;
    const FileOffsetPtr<PackedConfigBlock&> p_config_block_;
    PackedConfigBlock& config_block_;
    DataPacker packer_;
    usize slots_pending_;
    bool active_;
    bool block_is_full_;
    i64 next_offset_;
    u64 next_available_device_id_;
    std::vector<PreCommitAction> pre_commit_actions_;
  };

  explicit StorageFileBuilder(i64 base_offset) noexcept;

  template <typename ConfigOptionsT>
  StatusOr<FileOffsetPtr<typename ConfigOptionsT::PackedConfigType&>> add_object(
      const ConfigOptionsT& options);

 private:
  template <typename R>
  StatusOr<R> apply_transaction(const std::function<StatusOr<R>(Transaction&)>& fn);

  FileOffsetPtr<PackedConfigBlock&> allocate_config_block();

  void finalize_config_block();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::vector<std::unique_ptr<StorageFileConfigBlock>> config_blocks_;
  Optional<MutableBuffer> unused_payload_;
  i64 base_offset_;
  i64 next_offset_;
  u64 next_available_device_id_ = 0;
};

}  // namespace llfs

#endif  // LLFS_STORAGE_FILE_BUILDER_HPP
