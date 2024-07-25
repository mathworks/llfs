//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator_config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/page_allocator.hpp>
#include <llfs/uuid.hpp>

#include <batteries/case_of.hpp>
#include <batteries/stream_util.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ PageAllocatorConfigOptions PageAllocatorConfigOptions::with_default_values() noexcept
{
  return PageAllocatorConfigOptions{
      .uuid = None,
      .max_attachments = 64,
      .page_count = PageCount{0},
      .log_device = CreateNewLogDeviceWithDefaultSize{},
      .page_size_log2 = PageSizeLog2{0},
      .page_device = LinkToNewPageDevice{},
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_PRINT_OBJECT_IMPL(PackedPageAllocatorConfig,  //
                       (tag)                       //
                       (max_attachments)           //
                       (uuid)                      //
)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedPageAllocatorConfig&> p_allocator_config,
                                const PageAllocatorConfigOptions& options)
{
  p_allocator_config->uuid = options.uuid.value_or(random_uuid());
  p_allocator_config->max_attachments = options.max_attachments;
  p_allocator_config->page_count = options.page_count;
  p_allocator_config->page_size_log2 = options.page_size_log2;

  const auto minimum_log_size =
      PageAllocator::calculate_log_size(options.page_count, options.max_attachments);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Create or link to log device.
  //
  BATT_ASSIGN_OK_RESULT(
      p_allocator_config->log_device_uuid,
      batt::case_of(
          options.log_device,

          //----- --- -- -  -  -   -
          [&](const CreateNewLogDevice& create_new) -> StatusOr<boost::uuids::uuid> {
            if (create_new.options.log_size < minimum_log_size) {
              // TODO [tastolfi 2022-07-27] custom code
              return {batt::StatusCode::kInvalidArgument};
            }

            BATT_ASSIGN_OK_RESULT(
                const FileOffsetPtr<const PackedLogDeviceConfig&> p_log_device_config,
                txn.add_object(create_new.options));

            return p_log_device_config->uuid;
          },

          //----- --- -- -  -  -   -
          [&](const CreateNewLogDeviceWithDefaultSize& log_options)
              -> StatusOr<boost::uuids::uuid> {
            BATT_ASSIGN_OK_RESULT(
                const FileOffsetPtr<const PackedLogDeviceConfig&> p_log_device_config,
                txn.add_object(LogDeviceConfigOptions{
                    .uuid = log_options.uuid,
                    .pages_per_block_log2 = log_options.pages_per_block_log2,
                    .log_size = minimum_log_size,
                }));

            return p_log_device_config->uuid;
          },

          //----- --- -- -  -  -   -
          [&](const LinkToExistingLogDevice& link_to_existing) -> StatusOr<boost::uuids::uuid> {
            return link_to_existing.uuid;
          },

          //----- --- -- -  -  -   -
          [&](const CreateNewLogDevice2& create_new) -> StatusOr<boost::uuids::uuid> {
            if (create_new.options.log_size < minimum_log_size) {
              return {batt::StatusCode::kInvalidArgument};
            }

            BATT_ASSIGN_OK_RESULT(
                const FileOffsetPtr<const PackedLogDeviceConfig2&> p_log_device_config,
                txn.add_object(create_new.options));

            return p_log_device_config->uuid;
          },

          //----- --- -- -  -  -   -
          [&](const CreateNewLogDevice2WithDefaultSize& log_options)
              -> StatusOr<boost::uuids::uuid> {
            BATT_ASSIGN_OK_RESULT(
                const FileOffsetPtr<const PackedLogDeviceConfig2&> p_log_device_config,
                txn.add_object(LogDeviceConfigOptions2{
                    .uuid = log_options.uuid,
                    .log_size = minimum_log_size,
                    .device_page_size_log2 = LogDeviceConfigOptions2::kDefaultDevicePageSizeLog2,
                    .data_alignment_log2 = LogDeviceConfigOptions2::kDefaultDataAlignmentLog2,
                }));

            return p_log_device_config->uuid;
          }));

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Create or link to page device.
  //
  BATT_ASSIGN_OK_RESULT(
      p_allocator_config->page_device_id,
      batt::case_of(
          options.page_device,

          //----- --- -- -  -  -   -
          [&](const CreateNewPageDevice& create_new) -> StatusOr<page_device_id_int> {
            if (create_new.options.page_count != options.page_count) {
              // TODO [tastolfi 2022-07-27] custom code
              return {batt::StatusCode::kInvalidArgument};
            }

            if (create_new.options.page_size_log2 != options.page_size_log2) {
              // TODO [tastolfi 2022-07-27] custom code
              return {batt::StatusCode::kInvalidArgument};
            }

            BATT_ASSIGN_OK_RESULT(
                const FileOffsetPtr<const PackedPageDeviceConfig&> p_page_device_config,
                txn.add_object(create_new.options));

            return page_device_id_int{p_page_device_config->device_id};
          },

          //----- --- -- -  -  -   -
          [&](const LinkToExistingPageDevice& link_to_existing) -> StatusOr<page_device_id_int> {
            return link_to_existing.device_id;
          },

          //----- --- -- -  -  -   -
          [](const LinkToNewPageDevice&) -> StatusOr<page_device_id_int> {
            // TODO [tastolfi 2022-07-27] custom code
            return {batt::StatusCode::kInvalidArgument};
          }));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<PageAllocator>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context,                     //
    const std::string& /*file_name*/,                                           //
    const FileOffsetPtr<const PackedPageAllocatorConfig&>& p_allocator_config,  //
    const PageAllocatorRuntimeOptions& allocator_options,                       //
    const IoRingLogDriverOptions& log_options)
{
  StatusOr<std::unique_ptr<LogDeviceFactory>> log_factory =
      storage_context->recover_log_device(p_allocator_config->log_device_uuid, log_options);

  BATT_REQUIRE_OK(log_factory);

  const auto page_ids = PageIdFactory{
      PageCount{BATT_CHECKED_CAST(PageCount::value_type, p_allocator_config->page_count.value())},
      p_allocator_config->page_device_id,
  };

  return PageAllocator::recover(allocator_options, page_ids, **log_factory);
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
