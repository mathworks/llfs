//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator_config.hpp>
//

#include <llfs/page_allocator.hpp>

#include <batteries/stream_util.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

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
                                FileOffsetPtr<PackedPageAllocatorConfig&> p_config,
                                const PageAllocatorConfigOptions& options)
{
  const auto resolved_device_id = [&]() -> page_device_id_int {
    if (!options.page_device_id) {
      return txn.reserve_device_id();
    } else {
      return *options.page_device_id;
    }
  }();

  p_config->max_attachments = options.max_attachments;
  p_config->uuid = options.uuid.value_or(boost::uuids::random_generator{}());
  p_config->page_device_id = resolved_device_id;
  p_config->page_count = options.page_count;

  // If `options` specifies a uuid for the log device, then use that; otherwise, create a log device
  // and add it to the transaction.
  //
  auto resolve_log_device_uuid = [&]() -> StatusOr<boost::uuids::uuid> {
    if (options.log_device.uuid) {
      return *options.log_device.uuid;
    }

    const LogDeviceConfigOptions log_device_config_options{
        .uuid = None,
        .pages_per_block_log2 = options.log_device.pages_per_block_log2,
        .log_size =
            std::max(PageAllocator::calculate_log_size(options.page_count, options.max_attachments),
                     options.log_device.log_size),
    };

    BATT_ASSIGN_OK_RESULT(const FileOffsetPtr<const PackedLogDeviceConfig&> p_log_device,
                          txn.add_object(log_device_config_options));

    return p_log_device->uuid;
  };

  BATT_ASSIGN_OK_RESULT(p_config->log_device_uuid, resolve_log_device_uuid());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<PageAllocator>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context,           //
    const std::string& /*file_name*/,                                 //
    const FileOffsetPtr<const PackedPageAllocatorConfig&>& p_config,  //
    const PageAllocatorRuntimeOptions& options,                       //
    const IoRingLogDriverOptions& log_options)
{
  StatusOr<std::unique_ptr<LogDeviceFactory>> log_factory = storage_context->recover_object(
      batt::StaticType<PackedLogDeviceConfig>{}, p_config->log_device_uuid, log_options);

  BATT_REQUIRE_OK(log_factory);

  const auto page_ids = PageIdFactory{
      PageCount{BATT_CHECKED_CAST(PageCount::value_type, p_config->page_count.value())},
      p_config->page_device_id,
  };

  return PageAllocator::recover(options, page_ids, **log_factory);
}

}  // namespace llfs
