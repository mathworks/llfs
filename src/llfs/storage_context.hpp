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

#include <llfs/config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring.hpp>
#include <llfs/log_device_runtime_options.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_cache_options.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/storage_file.hpp>
#include <llfs/storage_file_builder.hpp>
#include <llfs/storage_object_info.hpp>

#include <batteries/async/task_scheduler.hpp>
#include <batteries/shared_ptr.hpp>

#include <boost/functional/hash.hpp>

#include <functional>
#include <string>
#include <unordered_map>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// A collection of storage files containing one or more storage objects, used to create a PageCache
// and recover LLFS Volumes.
//
class StorageContext : public batt::RefCounted<StorageContext>
{
 public:
  static boost::intrusive_ptr<StorageContext> make_shared(batt::TaskScheduler& scheduler,
                                                          const IoRing& io) noexcept
  {
    return boost::intrusive_ptr<StorageContext>{new StorageContext{scheduler, io}};
  }

 private:
  // Construct a new StorageContext that will use the given TaskScheduler and IoRing for background
  // tasks and asynchronous file I/O.
  //
  explicit StorageContext(batt::TaskScheduler& scheduler, const IoRing& io) noexcept;

 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // noncopyable
  //
  StorageContext(const StorageContext&) = delete;
  StorageContext& operator=(const StorageContext&) = delete;
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::TaskScheduler& get_scheduler() const
  {
    return this->scheduler_;
  }

  const IoRing& get_io_ring() const
  {
    return *this->io_ring_;
  }

  boost::intrusive_ptr<StorageContext> shared_from_this()
  {
    BATT_CHECK_NE(this->use_count(), 0);
    return boost::intrusive_ptr<StorageContext>{this};
  }

  /** \brief Set runtime options for PageCache.
   */
  void set_page_cache_options(const PageCacheOptions& options);

  // Returns a PageCache object that can be used to access all PageDevices in the StorageContext.
  // The PageCache is created the first time this function is called, and cached to be returned on
  // subsequent calls.
  //
  StatusOr<batt::SharedPtr<PageCache>> get_page_cache();

  // If one of the files managed by this context contains an object with the given uuid, returns
  // metadata about that object.  Otherwise returns nullptr.
  //
  batt::SharedPtr<StorageObjectInfo> find_object_by_uuid(const boost::uuids::uuid& uuid);

  // Returns a sequence of StorageObjectInfo for all objects with the type named by the passed
  // `tag`.  See `PackedConfigSlotBase::Tag`.
  //
  batt::BoxedSeq<batt::SharedPtr<StorageObjectInfo>> find_objects_by_tag(u16 tag);

  // Adds an already existing file to this context.
  //
  Status add_existing_named_file(std::string&& file_name, i64 start_offset = 0);

  // Creates a new file with the given name by invoking the passed `initializer` function to build
  // storage objects.
  //
  // If the initializer returns an error Status, then the file is not created.
  //
  Status add_new_file(const std::string& file_name,
                      const std::function<Status(StorageFileBuilder&)>& initializer);

  // Adds an already existing file to this context; the file must have been scanned for metadata by
  // `read_storage_file`.
  //
  Status add_existing_file(const batt::SharedPtr<StorageFile>& file);

  // Attempts to recover an object of a given type from this context by uuid.
  //
  // The type of the first argument determines the return type, the tag of the packed config, and
  // the type of the extra_options parameter pack.
  //
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
    if (PackedConfigTagFor<PackedConfigT>::value != info->p_config_slot->tag) {
      return ::llfs::make_status(::llfs::StatusCode::kStorageObjectTypeError);
    }
    return recover_storage_object(batt::shared_ptr_from(this), info->storage_file->file_name(),
                                  FileOffsetPtr<const PackedConfigT&>{
                                      config_slot_cast<PackedConfigT>(info->p_config_slot.object),
                                      info->p_config_slot.file_offset},
                                  BATT_FORWARD(extra_options)...);
  }

  /** \brief Special case for LogDevice recovery; handles both IoRingLogDevice and IoRingLogDevice2.
   */
  StatusOr<std::unique_ptr<LogDeviceFactory>> recover_log_device(
      const boost::uuids::uuid& uuid, const LogDeviceRuntimeOptions& log_runtime_options);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  // Passed in at creation time; used to schedule all background tasks needed by recovered objects
  // and the PageCache.
  //
  batt::TaskScheduler& scheduler_;

  // Passed in at creation time; this is the default IoRing used to perform I/O.
  //
  const IoRing* io_ring_;

  // An index of all storage objects by uuid.
  //
  std::unordered_map<boost::uuids::uuid, batt::SharedPtr<StorageObjectInfo>,
                     boost::hash<boost::uuids::uuid>>
      index_;

  // Options that will be used to instantiate `this->page_cache_`.
  //
  PageCacheOptions page_cache_options_ = PageCacheOptions::with_default_values();

  // The PageCache for this context; this is lazily created the first time
  // `StorageContext::get_page_cache()` is called.
  //
  batt::SharedPtr<PageCache> page_cache_;

  // TODO [tastolfi 2022-07-15]  IMPORTANT!!! BUG : we must track the device_ids to make sure there
  // are no conflicts.
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_STORAGE_CONTEXT_HPP
