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
  p_config->max_attachments = options.max_attachments;
  p_config->uuid = options.uuid.value_or(boost::uuids::random_generator{}());

  // If `options` specifies a uuid for the log device, then use that; otherwise, create a log device
  // and add it to the transaction.
  //
  auto resolve_log_device_uuid = [&]() -> StatusOr<boost::uuids::uuid> {
    if (options.log_device_uuid) {
      return *options.log_device_uuid;
    }

    const LogDeviceConfigOptions log_device_config_options{
        .log_size = PageAllocator::calculate_log_size(options.page_count, options.max_attachments),
        .uuid = options.log_device_uuid,
        .pages_per_block_log2 = options.log_device_pages_per_block_log2,
    };

    BATT_ASSIGN_OK_RESULT(const FileOffsetPtr<const PackedLogDeviceConfig&> p_log_device,
                          txn.add_object(log_device_config_options));

    return p_log_device->uuid;
  };

  BATT_ASSIGN_OK_RESULT(p_config->log_device_uuid, resolve_log_device_uuid());

  return OkStatus();
}

}  // namespace llfs
