//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_context.hpp>
//

#include <llfs/page_arena_config.hpp>
#include <llfs/raw_block_file_impl.hpp>
#include <llfs/status_code.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StorageContext::StorageContext(batt::TaskScheduler& scheduler, const IoRing& io_ring) noexcept
    : scheduler_{scheduler}
    , io_ring_{&io_ring}
{
  initialize_status_codes();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::SharedPtr<StorageObjectInfo> StorageContext::find_object_by_uuid(
    const boost::uuids::uuid& uuid) /*override*/
{
  auto iter = this->index_.find(uuid);
  if (iter == this->index_.end()) {
    return nullptr;
  }
  return iter->second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status StorageContext::add_existing_named_file(std::string&& file_name, i64 start_offset)
{
  StatusOr<int> fd = open_file_read_write(file_name, OpenForAppend{false}, OpenRawIO{true});
  BATT_REQUIRE_OK(fd);

  IoRingRawBlockFile file{IoRing::File{*this->io_ring_, *fd}};
  StatusOr<std::vector<std::unique_ptr<StorageFileConfigBlock>>> config_blocks =
      read_storage_file(file, start_offset);
  BATT_REQUIRE_OK(config_blocks);

  return this->add_existing_file(
      batt::make_shared<StorageFile>(std::move(file_name), std::move(*config_blocks)));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status StorageContext::add_new_file(const std::string& file_name,
                                    const std::function<Status(StorageFileBuilder&)>& initializer)
{
  {
    BATT_ASSIGN_OK_RESULT(
        std::unique_ptr<IoRingRawBlockFile> file,
        IoRingRawBlockFile::open(*this->io_ring_, file_name.c_str(),
                                 /*flags=*/O_RDWR | O_CREAT | O_EXCL | O_DIRECT | O_SYNC,
                                 /*mode=*/S_IRUSR | S_IWUSR));

    StorageFileBuilder builder{*file, /*base_offset=*/0};

    Status init_status = initializer(builder);
    if (!init_status.ok()) {
      file->close().IgnoreError();
      delete_file(file_name).IgnoreError();
      return init_status;
    }

    Status flush_status = builder.flush_all();
    BATT_REQUIRE_OK(flush_status);
  }
  return this->add_existing_named_file(batt::make_copy(file_name));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status StorageContext::add_existing_file(const batt::SharedPtr<StorageFile>& file)
{
  file->find_all_objects()  //
      | seq::for_each([&](const FileOffsetPtr<const PackedConfigSlot&>& slot) {
          LLFS_VLOG(1) << "Adding " << *slot << " to storage context";

          this->index_.emplace(slot->uuid,
                               batt::make_shared<StorageObjectInfo>(batt::make_copy(file), slot));
        });

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<LogDeviceFactory>> StorageContext::recover_log_device(
    const boost::uuids::uuid& uuid, const LogDeviceRuntimeOptions& log_runtime_options)
{
  batt::SharedPtr<StorageObjectInfo> info = this->find_object_by_uuid(uuid);
  if (!info) {
    return {batt::StatusCode::kNotFound};
  }

  switch (info->p_config_slot->tag) {
      //----- --- -- -  -  -   -
    case PackedConfigSlotBase::Tag::kLogDevice:
      return recover_storage_object(
          batt::shared_ptr_from(this), info->storage_file->file_name(),
          FileOffsetPtr<const PackedLogDeviceConfig&>{
              config_slot_cast<PackedLogDeviceConfig>(info->p_config_slot.object),
              info->p_config_slot.file_offset},
          log_runtime_options);

      //----- --- -- -  -  -   -
    case PackedConfigSlotBase::Tag::kLogDevice2:
      return recover_storage_object(
          batt::shared_ptr_from(this), info->storage_file->file_name(),
          FileOffsetPtr<const PackedLogDeviceConfig2&>{
              config_slot_cast<PackedLogDeviceConfig2>(info->p_config_slot.object),
              info->p_config_slot.file_offset},
          log_runtime_options);

      //----- --- -- -  -  -   -
    default:
      return ::llfs::make_status(::llfs::StatusCode::kStorageObjectTypeError);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void StorageContext::set_page_cache_options(const PageCacheOptions& options)
{
  this->page_cache_options_ = options;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::SharedPtr<PageCache>> StorageContext::get_page_cache()
{
  if (this->page_cache_) {
    return this->page_cache_;
  }

  std::vector<PageArena> storage_pool;

  for (const auto& [uuid, p_object_info] : this->index_) {
    if (p_object_info->p_config_slot->tag == PackedConfigSlotBase::Tag::kPageArena) {
      const auto& packed_arena_config =
          config_slot_cast<PackedPageArenaConfig>(p_object_info->p_config_slot.object);

      const std::string base_name =
          batt::to_string("PageDevice_", packed_arena_config.page_device_uuid);

      StatusOr<PageArena> arena = this->recover_object(
          batt::StaticType<PackedPageArenaConfig>{}, uuid,
          PageAllocatorRuntimeOptions{
              .scheduler = this->scheduler_,
              .name = batt::to_string(base_name, "_Allocator"),
          },
          [&] {
            IoRingLogDriverOptions options;
            options.name = batt::to_string(base_name, "_AllocatorLog");
            return options;
          }(),
          IoRingFileRuntimeOptions{
              .io_ring = *this->io_ring_,
              .use_raw_io = true,
              .allow_read = true,
              .allow_write = true,
          });

      BATT_REQUIRE_OK(arena);

      storage_pool.emplace_back(std::move(*arena));
    }
  }

  StatusOr<batt::SharedPtr<PageCache>> page_cache =
      PageCache::make_shared(std::move(storage_pool), this->page_cache_options_);

  BATT_REQUIRE_OK(page_cache);

  this->page_cache_ = *page_cache;

  return page_cache;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::BoxedSeq<batt::SharedPtr<StorageObjectInfo>> StorageContext::find_objects_by_tag(u16 tag)
{
  return as_seq(this->index_.begin(), this->index_.end())  //
         | seq::filter_map(
               [tag](const auto& kv_pair) -> Optional<batt::SharedPtr<StorageObjectInfo>> {
                 if (kv_pair.second->p_config_slot->tag == tag) {
                   return kv_pair.second;
                 } else {
                   return None;
                 }
               })  //
         | seq::boxed();
}

}  // namespace llfs
