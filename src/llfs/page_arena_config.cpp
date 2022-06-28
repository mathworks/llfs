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

#include <batteries/checked_cast.hpp>

#include <boost/uuid/random_generator.hpp>

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
  auto page_device_options = PageDeviceConfigOptions{
      .page_size_log2 = PageSizeLog2{options.page_size_bits},
      .page_count = options.page_count,
      .device_id = options.device_id,
      .uuid = options.device_uuid,
  };

  // If no PageDevice UUID is specified, then create a new one.
  //
  if (!options.device_uuid) {
    StatusOr<FileOffsetPtr<const PackedPageDeviceConfig&>> p_page_device_config =
        txn.add_object(page_device_options);
    BATT_REQUIRE_OK(p_page_device_config);

    page_device_options.device_id = (*p_page_device_config)->device_id;
    page_device_options.uuid = (*p_page_device_config)->uuid;
  }

  const u64 log_block_size = 1ull << options.log_block_size_bits.value_or(12);
  const u64 log_block_pages = log_block_size / kLogPageSize;
  BATT_CHECK_EQ(log_block_size % kLogPageSize, 0u)
      << "The PageAllocator log block size must be a multiple of the kLogPageSize (" << kLogPageSize
      << "); specified value = " << log_block_size;

  const u64 log_block_pages_log2 = batt::log2_ceil(log_block_pages);
  BATT_CHECK_EQ(1ull << log_block_pages_log2, log_block_pages);

  auto page_allocator_options = PageAllocatorConfigOptions{
      .page_count = options.page_count,
      .max_attachments = options.max_attachments,
      .uuid = options.allocator_uuid,
      .log_device_uuid = options.log_uuid,
      .log_device_pages_per_block_log2 = log_block_pages_log2,
      .page_device_id = BATT_CHECKED_CAST(u32, *page_device_options.device_id),
  };

  // If no PageAllocator UUID is specified, then create a new one.
  //
  if (!options.allocator_uuid) {
    StatusOr<FileOffsetPtr<const PackedPageAllocatorConfig&>> p_page_allocator_config =
        txn.add_object(page_allocator_options);
    BATT_REQUIRE_OK(p_page_allocator_config);

    page_allocator_options.uuid = (*p_page_allocator_config)->uuid;
  }

  p_config->tag = PackedConfigSlotBase::Tag::kPageArena;
  p_config->uuid = options.arena_uuid.value_or(boost::uuids::random_generator{}());
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
