//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_device_config.hpp>
//

#include <batteries/stream_util.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_PRINT_OBJECT_IMPL(PackedPageDeviceConfig,  //
                       (page_0_offset)          //
                       (device_id)              //
                       (page_count)             //
                       (page_size_log2)         //
                       (uuid)                   //
)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedPageDeviceConfig&> p_config,
                                const PageDeviceConfigOptions& options)
{
  const i64 page_size = (i64{1} << options.page_size_log2);
  const i64 pages_total_size = page_size * options.page_count;

  const Interval<i64> pages_offset = txn.reserve_aligned(options.page_size_log2, pages_total_size);

  p_config.absolute_page_0_offset(pages_offset.lower_bound);
  if (!options.device_id) {
    p_config->device_id = txn.reserve_device_id();
  } else {
    p_config->device_id = *options.device_id;
  }
  p_config->page_count = options.page_count;
  p_config->page_size_log2 = options.page_size_log2;
  p_config->uuid = options.uuid.value_or(boost::uuids::random_generator{}());

  return OkStatus();
}

}  // namespace llfs
