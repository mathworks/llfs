//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_config.hpp>
//

#include <llfs/page_recycler.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_PRINT_OBJECT_IMPL(PackedVolumeConfig,  //
                       (tag)                //
                       (name)               //
                       (uuid)               //
                       (max_refs_per_page)  //
                       (root_log_uuid)      //
                       (recycler_log_uuid)  //
)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedVolumeConfig&> p_config,
                                const VolumeConfigOptions& options)
{
#if 0
  if (options.base.name.size() > VolumeOptions::kMaxNameLength) {
    return {batt::StatusCode::kInvalidArgument};
  }

  p_config->max_refs_per_page = options.base.max_refs_per_page;
  p_config->volume_uuid = options.base.uuid.value_or(boost::uuids::random_generator{}());

  if (!txn.packer().pack_string_to(&p_config->name, options.base.name)) {
    return {batt::StatusCode::kResourceExhausted};
  }

  {
    LogDeviceConfigOptions root_log_config_options{
        .log_size = options.root_log_size,
        .uuid = None,
        .pages_per_block_log2 = options.root_log_pages_per_block_log2,
    };

    BATT_ASSIGN_OK_RESULT(FileOffsetPtr<PackedLogDeviceConfig&> p_root_log,
                          txn.add_object(root_log_config_options));

    p_config->root_log.reset(p_root_log.get(), &txn.packer());
  }
  {
    LogDeviceConfigOptions recycler_log_config_options{
        .log_size = PageRecycler::calculate_log_size(options.base.max_refs_per_page,
                                                     options.recycler_max_buffered_page_count),
        .uuid = None,
        .pages_per_block_log2 = options.recycler_log_pages_per_block_log2,
    };

    BATT_ASSIGN_OK_RESULT(FileOffsetPtr<PackedLogDeviceConfig&> p_recycler_log,
                          txn.add_object(recycler_log_config_options));

    p_config->recycler_log.reset(p_recycler_log.get(), &txn.packer());
  }

  return OkStatus();
#endif
  return batt::StatusCode::kUnimplemented;
}

}  // namespace llfs
