//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_arena_config.hpp>
//

#include <llfs/crc.hpp>
#include <llfs/ioring_log_device.hpp>
#include <llfs/page_allocator.hpp>
#include <llfs/uuid.hpp>

#include <batteries/checked_cast.hpp>

namespace llfs {

BATT_PRINT_OBJECT_IMPL(PackedPageArenaConfig,
                       (tag)                  //
                       (page_device_uuid)     //
                       (page_allocator_uuid)  //
)

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedPageArenaConfig&> p_config,
                                const PageArenaConfigOptions& options)
{
  if (options.page_device.device_id &&
      options.page_allocator.page_device_id != *options.page_device.device_id) {
    LOG(WARNING) << "The device numbers for page device and page allocator must match when "
                    "configuring a page arena.";
    return batt::StatusCode::kInvalidArgument;
  }

  PageDeviceConfigOptions page_device_options = options.page_device;

  // If no PageDevice UUID is specified, then create a new PageDevice.
  //
  if (!options.page_device.uuid) {
    StatusOr<FileOffsetPtr<const PackedPageDeviceConfig&>> p_page_device_config =
        txn.add_object(page_device_options);
    BATT_REQUIRE_OK(p_page_device_config);

    page_device_options.uuid = (*p_page_device_config)->uuid;
    page_device_options.device_id = (*p_page_device_config)->device_id;
  }

  PageAllocatorConfigOptions page_allocator_options = options.page_allocator;

  // If no PageAllocator UUID is specified, then create a new one.
  //
  if (!options.page_allocator.uuid) {
    StatusOr<FileOffsetPtr<const PackedPageAllocatorConfig&>> p_page_allocator_config =
        txn.add_object(page_allocator_options);
    BATT_REQUIRE_OK(p_page_allocator_config);

    page_allocator_options.uuid = (*p_page_allocator_config)->uuid;
  }

  p_config->tag = PackedConfigSlotBase::Tag::kPageArena;
  p_config->uuid = options.uuid.value_or(random_uuid());
  p_config->page_device_uuid = *page_device_options.uuid;
  p_config->page_allocator_uuid = *page_allocator_options.uuid;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context,       //
    const std::string& file_name,                                 //
    const FileOffsetPtr<const PackedPageArenaConfig&>& p_config,  //
    const PageAllocatorRuntimeOptions& allocator_options,         //
    const IoRingLogDriverOptions& allocator_log_options,          //
    const IoRingFileRuntimeOptions& page_device_file_options)
{
  StatusOr<std::unique_ptr<PageAllocator>> page_allocator = storage_context->recover_object(
      batt::StaticType<PackedPageAllocatorConfig>{}, p_config->page_allocator_uuid,
      allocator_options, allocator_log_options);
  BATT_REQUIRE_OK(page_allocator);

  StatusOr<std::unique_ptr<PageDevice>> page_device =
      storage_context->recover_object(batt::StaticType<PackedPageDeviceConfig>{},
                                      p_config->page_device_uuid, page_device_file_options);
  BATT_REQUIRE_OK(page_device);

  return PageArena{
      std::move(*page_device),
      std::move(*page_allocator),
  };
}

}  // namespace llfs
