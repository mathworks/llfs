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

  const LogDeviceConfigOptions log_device_config_options{
      .log_size = PageAllocator::calculate_log_size(options.page_count, options.max_attachments),
      .uuid = options.log_device_uuid,
      .pages_per_block_log2 = options.log_device_pages_per_block_log2,
  };

  BATT_ASSIGN_OK_RESULT(FileOffsetPtr<PackedLogDeviceConfig&> p_log_device,
                        txn.add_config_slot(log_device_config_options));

  p_config->log_device.reset(p_log_device.get(), &txn.packer());

  return OkStatus();
}

}  // namespace llfs
