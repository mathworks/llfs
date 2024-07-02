//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_config.hpp>
//

#include <llfs/ioring_log_device.hpp>
#include <llfs/page_recycler.hpp>
#include <llfs/uuid.hpp>

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

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<FileOffsetPtr<const PackedLogDeviceConfig&>> add_recycler_log(
    StorageFileBuilder::Transaction& txn, batt::StaticType<LogDeviceConfigOptions>,
    const VolumeConfigOptions& options)
{
  const LogDeviceConfigOptions recycler_log_options{
      .uuid = random_uuid(),
      .pages_per_block_log2 = IoRingLogConfig::kDefaultPagesPerBlockLog2 + 1,
      .log_size = PageRecycler::calculate_log_size(
          PageRecyclerOptions{}.set_max_refs_per_page(options.base.max_refs_per_page),
          options.recycler_max_buffered_page_count),
  };

  return txn.add_object(recycler_log_options);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<FileOffsetPtr<const PackedLogDeviceConfig2&>> add_recycler_log(
    StorageFileBuilder::Transaction& txn, batt::StaticType<LogDeviceConfigOptions2>,
    const VolumeConfigOptions& options)
{
  const LogDeviceConfigOptions2 recycler_log_options{
      .uuid = random_uuid(),
      .log_size = PageRecycler::calculate_log_size(
          PageRecyclerOptions{}.set_max_refs_per_page(options.base.max_refs_per_page),
          options.recycler_max_buffered_page_count),
      .device_page_size_log2 = None,
      .data_alignment_log2 = None,
  };

  return txn.add_object(recycler_log_options);
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedVolumeConfig&> p_config,
                                const VolumeConfigOptions& options)
{
  return batt::case_of(options.root_log, [&](const auto& root_log_options) -> Status {
    BATT_CHECK(!root_log_options.uuid)
        << "Creating a Volume from a pre-existing root log is not supported";

    using LogDeviceOptionsType = std::decay_t<decltype(root_log_options)>;

    auto p_root_log_config = txn.add_object(root_log_options);
    BATT_REQUIRE_OK(p_root_log_config);

    auto p_recycler_log_config =
        add_recycler_log(txn, batt::StaticType<LogDeviceOptionsType>{}, options);
    BATT_REQUIRE_OK(p_recycler_log_config);

    p_config->uuid = options.base.uuid.value_or(random_uuid());
    p_config->slot_i = 0;
    p_config->n_slots = 2;
    p_config->max_refs_per_page = options.base.max_refs_per_page;
    p_config->root_log_uuid = (*p_root_log_config)->uuid;
    p_config->recycler_log_uuid = (*p_recycler_log_config)->uuid;

    p_config->slot_1.tag = PackedConfigSlotBase::Tag::kVolumeContinuation;
    p_config->slot_1.slot_i = 1;
    p_config->slot_1.n_slots = 2;
    p_config->trim_lock_update_interval_bytes = options.base.trim_lock_update_interval;
    p_config->trim_delay_byte_count = options.base.trim_delay_byte_count;

    if (!txn.packer().pack_string_to(&p_config->name, options.base.name)) {
      return ::batt::StatusCode::kResourceExhausted;
    }

    return OkStatus();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<Volume>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context, const std::string /*file_name*/,
    const FileOffsetPtr<const PackedVolumeConfig&>& p_volume_config,
    VolumeRuntimeOptions&& volume_runtime_options)
{
  StatusOr<batt::SharedPtr<PageCache>> page_cache = storage_context->get_page_cache();
  BATT_REQUIRE_OK(page_cache);

  StatusOr<std::unique_ptr<LogDeviceFactory>> root_log_factory =
      storage_context->recover_log_device(p_volume_config->root_log_uuid,
                                          volume_runtime_options.root_log_options);
  BATT_REQUIRE_OK(root_log_factory);

  StatusOr<std::unique_ptr<LogDeviceFactory>> recycler_log_factory =
      storage_context->recover_log_device(p_volume_config->recycler_log_uuid,
                                          volume_runtime_options.recycler_log_options);
  BATT_REQUIRE_OK(recycler_log_factory);

  VolumeRecoverParams params{
      .scheduler = &storage_context->get_scheduler(),
      .options =
          VolumeOptions{
              .name = std::string{p_volume_config->name.as_str()},
              .uuid = p_volume_config->uuid,
              .max_refs_per_page = MaxRefsPerPage{p_volume_config->max_refs_per_page},
              .trim_lock_update_interval =
                  TrimLockUpdateInterval{p_volume_config->trim_lock_update_interval_bytes},
              .trim_delay_byte_count = TrimDelayByteCount{p_volume_config->trim_delay_byte_count},
          },
      .cache = *page_cache,
      .root_log_factory = root_log_factory->get(),
      .recycler_log_factory = recycler_log_factory->get(),
      .trim_control = std::move(volume_runtime_options.trim_control),
  };

  return Volume::recover(std::move(params), volume_runtime_options.slot_visitor_fn);
}

}  // namespace llfs
