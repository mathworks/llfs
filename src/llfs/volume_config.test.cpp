//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_config.hpp>
//
#include <llfs/volume_config.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/constants.hpp>
#include <llfs/ioring.hpp>
#include <llfs/packed_bytes.hpp>
#include <llfs/storage_context.hpp>

#include <filesystem>

namespace {

using namespace llfs::constants;

const std::filesystem::path storage_file_path{"/tmp/llfs_volume_config_test_file"};
const std::string kTestVolumeName = "MyTestVolume";

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST(VolumeConfigTest, ConfigRestore)
{
  llfs::delete_file(storage_file_path.string()).IgnoreError();
  EXPECT_FALSE(std::filesystem::exists(storage_file_path));

  llfs::StatusOr<llfs::ScopedIoRing> io =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{64}, llfs::ThreadPoolSize{1});
  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  // Create a StorageContext.
  //
  batt::SharedPtr<llfs::StorageContext> storage_context = batt::make_shared<llfs::StorageContext>(
      batt::Runtime::instance().default_scheduler(), io->get());

  // We want to save the uuid generated for the volume so we can reload it later.
  //
  boost::uuids::uuid volume_uuid;
  boost::uuids::uuid root_log_uuid;

  // Create a storage file with two page arenas, one for small pages (4kb), one for large (2mb).
  //
  llfs::Status file_create_status = storage_context->add_new_file(
      storage_file_path.string(), [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedVolumeConfig&>> p_volume_config =
            builder.add_object(llfs::VolumeConfigOptions{
                .base =
                    llfs::VolumeOptions{
                        .name = kTestVolumeName,
                        .uuid = llfs::None,
                        .max_refs_per_page = llfs::MaxRefsPerPage{1},
                        .trim_lock_update_interval = llfs::TrimLockUpdateInterval{4 * kKiB},
                    },
                .root_log =
                    llfs::LogDeviceConfigOptions{
                        .uuid = llfs::None,
                        .pages_per_block_log2 = llfs::None,
                        .log_size = 8 * kMiB,
                    },
                .recycler_max_buffered_page_count = llfs::None,
            });

        BATT_REQUIRE_OK(p_volume_config);

        volume_uuid = (*p_volume_config)->uuid;
        root_log_uuid = (*p_volume_config)->root_log_uuid;

        return llfs::OkStatus();
      });

  ASSERT_TRUE(file_create_status.ok()) << BATT_INSPECT(file_create_status);

  std::cerr << BATT_INSPECT(volume_uuid) << BATT_INSPECT(root_log_uuid) << std::endl;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  const auto make_volume_runtime_options = [&] {
    return llfs::VolumeRuntimeOptions{
        .slot_visitor_fn = [&](const llfs::SlotParse& slot,
                               std::string_view user_data) -> llfs::Status {
          (void)slot;
          (void)user_data;
          return llfs::OkStatus();
        },
        .root_log_options = llfs::IoRingLogDriverOptions{},
        .recycler_log_options = llfs::IoRingLogDriverOptions{},
        .trim_control = nullptr,
    };
  };

  {
    // Verify that we get the correct error status if we try to recover a Volume from the wrong type
    // of config slot.
    {
      llfs::StatusOr<std::unique_ptr<llfs::Volume>> expect_fail =
          storage_context->recover_object(batt::StaticType<llfs::PackedVolumeConfig>{},
                                          root_log_uuid, make_volume_runtime_options());

      EXPECT_FALSE(expect_fail.ok());
      EXPECT_EQ(expect_fail.status(), llfs::StatusCode::kStorageObjectTypeError);
    }

    // Now restore the volume, write some events.
    //
    llfs::StatusOr<std::unique_ptr<llfs::Volume>> maybe_volume = storage_context->recover_object(
        batt::StaticType<llfs::PackedVolumeConfig>{}, volume_uuid, make_volume_runtime_options());

    ASSERT_TRUE(maybe_volume.ok()) << BATT_INSPECT(maybe_volume.status());

    llfs::Volume& volume = **maybe_volume;
    llfs::StatusOr<batt::Grant> grant =
        volume.reserve(volume.calculate_grant_size("alpha") +        //
                           volume.calculate_grant_size("bravo") +    //
                           volume.calculate_grant_size("charlie") +  //
                           volume.calculate_grant_size("delta"),
                       batt::WaitForResource::kTrue);

    ASSERT_TRUE(grant.ok()) << BATT_INSPECT(grant.status());

    llfs::StatusOr<llfs::SlotRange> alpha_slot = volume.append("alpha", *grant);
    ASSERT_TRUE(alpha_slot.ok()) << BATT_INSPECT(alpha_slot.status());

    llfs::StatusOr<llfs::SlotRange> bravo_slot = volume.append("bravo", *grant);
    ASSERT_TRUE(bravo_slot.ok()) << BATT_INSPECT(bravo_slot.status());

    llfs::StatusOr<llfs::SlotRange> charlie_slot = volume.append("charlie", *grant);
    ASSERT_TRUE(charlie_slot.ok()) << BATT_INSPECT(charlie_slot.status());

    llfs::StatusOr<llfs::SlotRange> delta_slot = volume.append("delta", *grant);
    ASSERT_TRUE(delta_slot.ok()) << BATT_INSPECT(delta_slot.status());

    llfs::Status sync_status = volume
                                   .sync(llfs::LogReadMode::kDurable,
                                         llfs::SlotUpperBoundAt{.offset = delta_slot->upper_bound})
                                   .status();
    ASSERT_TRUE(sync_status.ok()) << BATT_INSPECT(sync_status);
  }
}

}  // namespace
