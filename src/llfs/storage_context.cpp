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
    llfs::page_device_id_int initial_device_id =
        (this->page_cache_) ? this->page_cache_->get_num_page_devices() : 0;
    StorageFileBuilder builder{*file, /*base_offset=*/0, initial_device_id};
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

// TODO: [Gabe Bornstein 6/4/24] Could encapsulate some of these params in a new llfs object
//
Status StorageContext::increase_storage_capacity(
    const std::filesystem::path& dir_path, u64 increase_capacity, PageSize leaf_size,
    PageSizeLog2 leaf_size_log2, PageSize node_size, PageSizeLog2 node_size_log2,
    const char* const kPageFileName, unsigned int max_tree_height, unsigned int max_attachments)
{
  // TODO: [Gabe Bornstein 6/3/24] A lot of this code is copy-pasted and could be de-duped. This
  // code could potentially be moved into turtle_db. It basically already exists there in
  // DB::create.
  //
  // Calculate the page counts from the total capacity and TreeOptions.
  //
  const auto max_in_refs_size_per_leaf = 64 * max_tree_height;

  const auto leaf_page_count =
      llfs::PageCount{increase_capacity / (leaf_size + max_in_refs_size_per_leaf)};

  const auto total_leaf_pages_size = leaf_page_count * leaf_size;
  const auto total_node_pages_size = increase_capacity - total_leaf_pages_size;

  const auto node_page_count = llfs::PageCount{total_node_pages_size / node_size};

  VLOG(1) << BATT_INSPECT(increase_capacity) << BATT_INSPECT(node_page_count)
          << BATT_INSPECT(leaf_page_count);

  LOG(INFO) << "PAGE COUNT add_existing_file: " << node_page_count;
  // Create the page file.
  //
  Status page_file_status = this->add_new_file(
      (dir_path / kPageFileName).string(),
      [&](llfs::StorageFileBuilder& builder) -> Status  //
      {
        // Add an arena for node pages.
        //
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> node_pool_config =
            builder.add_object(
                llfs::PageArenaConfigOptions{
                    .uuid = None,
                    .page_allocator =
                        llfs::CreateNewPageAllocator{
                            .options =
                                llfs::PageAllocatorConfigOptions{
                                    .uuid = llfs::None,
                                    .max_attachments = max_attachments,
                                    .page_count = node_page_count,
                                    .log_device =
                                        llfs::CreateNewLogDevice2WithDefaultSize{
                                            .uuid = llfs::None,
                                            .device_page_size_log2=None,
                                            .data_alignment_log2=None,
                                        },
                                    .page_size_log2 = node_size_log2,
                                    .page_device = llfs::LinkToNewPageDevice{},
                                },
                        },
                    .page_device =
                        llfs::CreateNewPageDevice{
                            .options =
                                llfs::PageDeviceConfigOptions{
                                    .uuid = llfs::None,
                                    .device_id = llfs::None,
                                    .page_count = node_page_count,
                                    .page_size_log2 = node_size_log2,
                                },
                        },
                });

        BATT_REQUIRE_OK(node_pool_config);

        // Add an arena for leaf pages.
        //
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> leaf_pool_config =
            builder.add_object(
                llfs::PageArenaConfigOptions{
                    .uuid = None,
                    .page_allocator =
                        llfs::CreateNewPageAllocator{
                            .options =
                                llfs::PageAllocatorConfigOptions{
                                    .uuid = llfs::None,
                                    .max_attachments = max_attachments,
                                    .page_count = leaf_page_count,
                                    .log_device =
                                        llfs::CreateNewLogDevice2WithDefaultSize{
                                            .uuid = llfs::None,
                                            .device_page_size_log2=None,
                                            .data_alignment_log2=None,
                                        },
                                    .page_size_log2 = leaf_size_log2,
                                    .page_device = llfs::LinkToNewPageDevice{},
                                },
                        },
                    .page_device =
                        llfs::CreateNewPageDevice{
                            .options =
                                llfs::PageDeviceConfigOptions{
                                    .uuid = llfs::None,
                                    .device_id = llfs::None,
                                    .page_count = leaf_page_count,
                                    .page_size_log2 = leaf_size_log2,
                                },
                        },
                });
        BATT_REQUIRE_OK(leaf_pool_config);
        return OkStatus();
      });

  BATT_REQUIRE_OK(page_file_status);
  std::vector<PageArena> arenas;
  BATT_CHECK_OK(this->recover_arenas(arenas));
  BATT_CHECK_NE(this->page_cache_, nullptr);
  BATT_CHECK_OK(this->page_cache_->add_page_devices(arenas));
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
      return {::llfs::make_status(::llfs::StatusCode::kLogDeviceV1Deprecated)};

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

// TODO: [Gabe Bornstein 6/7/24] This could probably be a private function.
//
Status StorageContext::recover_arena(std::vector<PageArena>& arenas, boost::uuids::uuid uuid,
                                     batt::SharedPtr<StorageObjectInfo> p_object_info)
{
  if (p_object_info->p_config_slot->tag == PackedConfigSlotBase::Tag::kPageArena) {
    const auto& packed_arena_config =
        config_slot_cast<PackedPageArenaConfig>(p_object_info->p_config_slot.object);

    // If we have already recovered a device with the same uuid before, return immediately. If we
    // recover the same device twice, would could have asynchronous write conflicts in the future.
    //
    if (this->recovered_uuids_.find(uuid) != this->recovered_uuids_.end()) {
      return OkStatus();
    }

    const std::string base_name =
        batt::to_string("PageDevice_", packed_arena_config.page_device_uuid);
    StatusOr<PageArena> arena = this->recover_object(
        batt::StaticType<PackedPageArenaConfig>{}, uuid,
        PageAllocatorRuntimeOptions{
            .scheduler = this->get_scheduler(),
            .name = batt::to_string(base_name, "_Allocator"),
        },
        [&] {
          LogDeviceRuntimeOptions options;
          options.name = batt::to_string(base_name, "_AllocatorLog");
          return options;
        }(),
        IoRingFileRuntimeOptions{
            .io_ring = this->get_io_ring(),
            .use_raw_io = true,
            .allow_read = true,
            .allow_write = true,
        });

    // TODO: [Gabe Bornstein 6/11/24] Consider, should I insert before calling recover_object?
    //
    this->recovered_uuids_.insert(uuid);

    BATT_REQUIRE_OK(arena);
    arenas.emplace_back(std::move(*arena));
  }
  return OkStatus();
}

// TODO: [Gabe Bornstein 6/7/24] This could probably be a private function.
//
Status StorageContext::recover_arenas(std::vector<PageArena>& arenas)
{
  // Add Arenas to PageCache.
  //
  for (const auto& [uuid, p_object_info] : this->index_) {
    BATT_CHECK_OK(this->recover_arena(arenas, uuid, p_object_info));
  }
  return OkStatus();
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
        BATT_CHECK_OK(this->recover_arena(storage_pool, uuid, p_object_info));
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
