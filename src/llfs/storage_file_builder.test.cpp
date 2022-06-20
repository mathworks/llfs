#include <llfs/storage_file_builder.hpp>
//
#include <llfs/storage_file_builder.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/mock_raw_block_device.hpp>
#include <llfs/page_device_config.hpp>

namespace {

using namespace llfs::int_types;

// Test Plan:
//  1. Create StorageFileBuilder, add nothing, don't flush, no action taken.
//  2. Add a PageDeviceConfig, verify the correct slot is "written" to the file mock.
//

TEST(StorageFileBuilderTest, NoConfigs)
{
  ::testing::StrictMock<llfs::MockRawBlockDevice> mock_device;

  llfs::StorageFileBuilder builder{mock_device, /*base_offset=*/0};
}

TEST(StorageFileBuilderTest, PageDeviceConfig_NoFlush)
{
  ::testing::StrictMock<llfs::MockRawBlockDevice> mock_device;

  llfs::StorageFileBuilder builder{mock_device, /*base_offset=*/0};

  llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
      builder.add_object(llfs::PageDeviceConfigOptions{
          .page_size_log2 = llfs::PageSizeLog2{12},
          .page_count = llfs::PageCount{1024 * 1024},
          .device_id = llfs::None,
          .uuid = llfs::None,
      });

  ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());
}

TEST(StorageFileBuilderTest, PageDeviceConfig_Flush)
{
  ::testing::StrictMock<llfs::MockRawBlockDevice> mock_device;

  llfs::StorageFileBuilder builder{mock_device, /*base_offset=*/0};

  llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
      builder.add_object(llfs::PageDeviceConfigOptions{
          .page_size_log2 = llfs::PageSizeLog2{12},
          .page_count = llfs::PageCount{1024 * 1024},
          .device_id = llfs::None,
          .uuid = llfs::None,
      });

  ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

  EXPECT_CALL(
      mock_device,
      write_some(::testing::Eq(0), ::testing::Truly([](const llfs::ConstBuffer& data) {
                   if (data.size() != 4096) {
                     return false;
                   }
                   auto& config_block = *static_cast<const llfs::PackedConfigBlock*>(data.data());

                   return config_block.magic == llfs::PackedConfigBlock::kMagic &&
                          config_block.version == llfs::make_version_u64(0, 1, 0) &&
                          config_block.prev_offset == llfs::PackedConfigBlock::kNullFileOffset &&
                          config_block.next_offset == llfs::PackedConfigBlock::kNullFileOffset &&
                          config_block.slots.size() == 1u &&
                          config_block.slots[0].tag == llfs::PackedConfigSlot::Tag::kPageDevice &&
                          // TODO [tastolfi 2022-06-20] Verify the rest of the fields of slot[0].
                          config_block.crc64 == config_block.true_crc64();
                 })))
      .WillOnce(::testing::Return(llfs::StatusOr<i64>{4096}));

  llfs::Status flush_status = builder.flush_all();

  ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
}

}  // namespace
