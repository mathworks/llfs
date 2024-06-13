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

#include <batteries/bounds.hpp>
#include <batteries/case_of.hpp>
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
                                const PageArenaConfigOptions& arena_options)
{
  p_config->uuid = arena_options.uuid.value_or(random_uuid());

  Optional<page_device_id_int> page_device_id;
  Optional<i64> page_count;
  Optional<u16> page_size_log2;
  bool link_new_page_device = false;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  BATT_ASSIGN_OK_RESULT(
      p_config->page_device_uuid,
      batt::case_of(
          arena_options.page_device,

          //----- --- -- -  -  -   -
          [&](const CreateNewPageDevice& create_new) -> StatusOr<boost::uuids::uuid> {
            Status cross_check = batt::case_of(
                arena_options.page_allocator,

                //----- --- -- -  -  -   -
                [&](const CreateNewPageAllocator& create_new_allocator) -> Status {
                  return batt::case_of(
                      create_new_allocator.options.page_device,

                      //----- --- -- -  -  -   -
                      [&](const CreateNewPageDevice& opts) -> Status {
                        if (opts != create_new) {
                          LLFS_LOG_ERROR() << "PageDevice config doesn't match (between "
                                              "page_device and page_allocator.page_device)!";

                          return batt::StatusCode::kInvalidArgument;
                        }
                        link_new_page_device = true;
                        return OkStatus();
                      },

                      //----- --- -- -  -  -   -
                      [&](const LinkToExistingPageDevice& /*opts*/) -> Status {
                        LLFS_LOG_ERROR() << "Config not supported: arena.page_device = CreateNew, "
                                            "arena.page_allocator.page_device = LinkToExisting";
                        return batt::StatusCode::kInvalidArgument;
                      },

                      //----- --- -- -  -  -   -
                      [&](const LinkToNewPageDevice& /*opts*/) -> Status {
                        link_new_page_device = true;
                        return OkStatus();
                      });
                },

                //----- --- -- -  -  -   -
                [&](const LinkToExistingPageAllocator&) -> Status {
                  // It might not be ok, but we can't verify it at this point!  If there are any
                  // cross-check issues, they will be found at recovery time.
                  //
                  return OkStatus();
                });

            BATT_REQUIRE_OK(cross_check);

            BATT_ASSIGN_OK_RESULT(
                const FileOffsetPtr<const PackedPageDeviceConfig&> p_page_device_config,
                txn.add_object(create_new.options));

            page_device_id = p_page_device_config->device_id;
            page_count = p_page_device_config->page_count;
            page_size_log2 = p_page_device_config->page_size_log2;

            return p_page_device_config->uuid;
          },

          //----- --- -- -  -  -   -
          [&](const LinkToExistingPageDevice& link_to_existing) -> StatusOr<boost::uuids::uuid> {
            page_device_id = link_to_existing.device_id;
            return link_to_existing.uuid;
          },

          //----- --- -- -  -  -   -
          [&](const LinkToNewPageDevice& /*link_to_new*/) -> StatusOr<boost::uuids::uuid> {
            LLFS_LOG_ERROR() << "Config not supported: arena.page_device = LinkToNew";
            return {batt::StatusCode::kInvalidArgument};
          }));

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  BATT_ASSIGN_OK_RESULT(
      p_config->page_allocator_uuid,
      batt::case_of(
          arena_options.page_allocator,

          //----- --- -- -  -  -   -
          [&](CreateNewPageAllocator create_new) -> StatusOr<boost::uuids::uuid> {
            if (link_new_page_device) {
              BATT_CHECK(page_device_id);

              create_new.options.page_device = LinkToExistingPageDevice{
                  .uuid = p_config->page_device_uuid,
                  .device_id = *page_device_id,
              };
            }

            BATT_ASSIGN_OK_RESULT(
                const FileOffsetPtr<const PackedPageAllocatorConfig&> p_allocator_config,
                txn.add_object(create_new.options));

            if (page_size_log2 && p_allocator_config->page_size_log2 != *page_size_log2) {
              LLFS_LOG_ERROR() << "Cross-validation failed: page_size_log2 values not equal;"
                               << BATT_INSPECT(p_allocator_config->page_size_log2)
                               << BATT_INSPECT(*page_size_log2);

              return {batt::StatusCode::kInvalidArgument};
            }

            if (page_count && p_allocator_config->page_count != *page_count) {
              LLFS_LOG_ERROR() << "Cross-validation failed: page_count values not equal;"
                               << BATT_INSPECT(p_allocator_config->page_count)
                               << BATT_INSPECT(*page_count);

              return {batt::StatusCode::kInvalidArgument};
            }

            return p_allocator_config->uuid;
          },

          //----- --- -- -  -  -   -
          [&](const LinkToExistingPageAllocator& link_to_existing) -> StatusOr<boost::uuids::uuid> {
            return link_to_existing.uuid;
          }));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context,       //
    const std::string& /*file_name*/,                             //
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

  auto arena = PageArena{
      std::move(*page_device),
      std::move(*page_allocator),
  };

  BATT_CHECK_EQ(arena.id(), arena.allocator().get_device_id());
  return arena;
}

}  // namespace llfs
