//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_CONTEXT_HPP
#define LLFS_STORAGE_CONTEXT_HPP

#include <llfs/ioring.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/storage_file.hpp>
#include <llfs/storage_object_info.hpp>

#include <batteries/async/task_scheduler.hpp>
#include <batteries/shared_ptr.hpp>

#include <boost/functional/hash.hpp>

#include <string>
#include <unordered_map>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class StorageContext : public batt::RefCounted<StorageContext>
{
 public:
  explicit StorageContext(batt::TaskScheduler& scheduler, IoRing& io) noexcept;

  StorageContext(const StorageContext&) = delete;
  StorageContext& operator=(const StorageContext&) = delete;

  StatusOr<batt::SharedPtr<PageCache>> get_page_cache();

  batt::SharedPtr<StorageObjectInfo> find_object_by_uuid(const boost::uuids::uuid& uuid);

  Status add_named_file(std::string&& file_name, i64 start_offset = 0);

  Status add_file(const batt::SharedPtr<StorageFile>& file);

  template <typename PackedConfigT, typename... ExtraConfigOptions,
            typename R = decltype(recover_storage_object(
                std::declval<batt::SharedPtr<StorageContext>>(), std::declval<const std::string&>(),
                std::declval<FileOffsetPtr<const PackedConfigT&>>(),
                std::declval<ExtraConfigOptions>()...))  //
            >
  R recover_object(batt::StaticType<PackedConfigT>, const boost::uuids::uuid& uuid,
                   ExtraConfigOptions&&... extra_options)
  {
    batt::SharedPtr<StorageObjectInfo> info = this->find_object_by_uuid(uuid);
    if (!info) {
      return {batt::StatusCode::kNotFound};
    }
    return recover_storage_object(batt::shared_ptr_from(this), info->storage_file->file_name(),
                                  FileOffsetPtr<const PackedConfigT&>{
                                      config_slot_cast<PackedConfigT>(info->p_config_slot.object),
                                      info->p_config_slot.file_offset},
                                  BATT_FORWARD(extra_options)...);
  }

 private:
  batt::TaskScheduler& scheduler_;

  IoRing& io_;

  std::unordered_map<boost::uuids::uuid, batt::SharedPtr<StorageObjectInfo>,
                     boost::hash<boost::uuids::uuid>>
      index_;

  batt::SharedPtr<PageCache> page_cache_;
};

}  // namespace llfs

#endif  // LLFS_STORAGE_CONTEXT_HPP
