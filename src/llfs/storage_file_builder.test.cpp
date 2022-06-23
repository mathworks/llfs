#include <llfs/storage_file_builder.hpp>
//
#include <llfs/storage_file_builder.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/filesystem.hpp>
#include <llfs/ioring.hpp>
#include <llfs/mock_raw_block_device.hpp>
#include <llfs/page_device_config.hpp>
#include <llfs/raw_block_device_impl.hpp>
#include <llfs/storage_file.hpp>

#include <batteries/math.hpp>

namespace {

using namespace llfs::int_types;

constexpr usize kTestPageCount = 10;

class StorageFileBuilderTest : public ::testing::Test
{
 protected:
  bool verify_config_block(const llfs::PackedConfigBlock& config_block,
                           const i64 kExpectedPage0Offset, const i64 kExpectedConfigBlockOffset,
                           u32 page_size_log2)
  {
    return config_block.magic == llfs::PackedConfigBlock::kMagic &&
           config_block.version == llfs::make_version_u64(0, 1, 0) &&
           config_block.prev_offset == llfs::PackedConfigBlock::kNullFileOffset &&
           config_block.next_offset == llfs::PackedConfigBlock::kNullFileOffset &&
           config_block.slots.size() == 1u &&
           config_block.slots[0].tag == llfs::PackedConfigSlot::Tag::kPageDevice &&
           [&] {
             auto* page_device_config =
                 llfs::config_slot_cast<llfs::PackedPageDeviceConfig>(&config_block.slots[0]);

             return page_device_config->tag == llfs::PackedConfigSlot::Tag::kPageDevice &&

                    // `page_0_offset` is relative to the start of the
                    // `PackedPageDeviceConfig` struct, *not* the beginning of
                    // the file, so subtract the expected position of slot[0].
                    //
                    page_device_config->page_0_offset ==
                        (kExpectedPage0Offset - (kExpectedConfigBlockOffset + 64)) &&

                    page_device_config->device_id == 0 &&

                    page_device_config->page_count == kTestPageCount &&

                    page_device_config->page_size_log2 == page_size_log2;
           }() &&
           config_block.crc64 == config_block.true_crc64();
  }
};

// Test Plan:
//  1. Create StorageFileBuilder, add nothing, don't flush, no action taken.
//  2. Add a PageDeviceConfig, verify the correct slot is "written" to the file mock.
//  3. Fill all the slots in the PackedConfigBlock, verify the chain is set up, in the correct
//  order.
//

TEST_F(StorageFileBuilderTest, NoConfigs)
{
  ::testing::StrictMock<llfs::MockRawBlockDevice> mock_device;

  llfs::StorageFileBuilder builder{mock_device, /*base_offset=*/0};
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_NoFlush)
{
  ::testing::StrictMock<llfs::MockRawBlockDevice> mock_device;

  llfs::StorageFileBuilder builder{mock_device, /*base_offset=*/0};

  llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
      builder.add_object(llfs::PageDeviceConfigOptions{
          .page_size_log2 = llfs::PageSizeLog2{12},
          .page_count = llfs::PageCount{kTestPageCount},
          .device_id = llfs::None,
          .uuid = llfs::None,
      });

  ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_Flush)
{
  for (i64 base_file_offset : {0, 128, 65536}) {
    for (u32 page_size_log2 : {9, 10, 11, 12, 13, 16, 24}) {
      ::testing::StrictMock<llfs::MockRawBlockDevice> mock_device;

      llfs::StorageFileBuilder builder{mock_device, base_file_offset};

      const auto options = llfs::PageDeviceConfigOptions{
          .page_size_log2 = llfs::PageSizeLog2{page_size_log2},
          .page_count = llfs::PageCount{kTestPageCount},
          .device_id = llfs::None,
          .uuid = llfs::None,
      };
      const usize kTestPageSize = 1ll << options.page_size_log2;

      llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
          builder.add_object(options);

      ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

      const i64 kExpectedConfigBlockOffset = batt::round_up_bits(12, base_file_offset);

      const i64 kExpectedPage0Offset = batt::round_up_bits(
          page_size_log2, kExpectedConfigBlockOffset + sizeof(llfs::PackedConfigBlock));

      const i64 kExpectedFileSize = kExpectedPage0Offset + options.page_count * kTestPageSize;

      ::testing::Sequence flush_sequence;

      EXPECT_CALL(mock_device, truncate_at_least(::testing::Eq(kExpectedFileSize)))
          .InSequence(flush_sequence)
          .WillOnce(::testing::Return(llfs::OkStatus()));

      EXPECT_CALL(mock_device, write_some(::testing::Gt(kExpectedConfigBlockOffset),
                                          ::testing::Truly([](const llfs::ConstBuffer& b) {
                                            return b.size() == 512;
                                          })))
          .Times(options.page_count)
          .InSequence(flush_sequence)
          .WillRepeatedly(::testing::Return(512));

      EXPECT_CALL(mock_device,
                  write_some(::testing::Eq(kExpectedConfigBlockOffset),
                             ::testing::Truly([&](const llfs::ConstBuffer& data) {
                               if (data.size() != 4096) {
                                 return false;
                               }
                               auto& config_block =
                                   *static_cast<const llfs::PackedConfigBlock*>(data.data());

                               return this->verify_config_block(config_block, kExpectedPage0Offset,
                                                                kExpectedConfigBlockOffset,
                                                                page_size_log2);
                             })))
          .InSequence(flush_sequence)
          .WillOnce(::testing::Return(llfs::StatusOr<i64>{4096}));

      llfs::Status flush_status = builder.flush_all();

      ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
    }
  }
}

TEST_F(StorageFileBuilderTest, WriteReadFile)
{
  int test_fd = fileno(tmpfile());

  ASSERT_GT(test_fd, 0);

  {
    llfs::Status status = llfs::enable_raw_io_fd(test_fd, /*enabled=*/true);
    ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
  }

  llfs::StatusOr<llfs::IoRing> ioring = llfs::IoRing::make_new(/*entries=*/64);
  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  ioring->on_work_started();
  std::thread ioring_thread{[&] {
    ioring->run().IgnoreError();
  }};

  auto on_scope_exit = batt::finally([&] {
    ioring->on_work_finished();
    ioring_thread.join();
  });

  llfs::IoRing::File test_file{*ioring, test_fd};
  llfs::IoRingRawBlockDevice test_device{std::move(test_file)};

  const auto page_device_options = llfs::PageDeviceConfigOptions{
      .page_size_log2 = llfs::PageSizeLog2{12} /* 4096 */,
      .page_count = llfs::PageCount{kTestPageCount},
      .device_id = llfs::None,
      .uuid = llfs::None,
  };

  {
    llfs::StorageFileBuilder builder{test_device, /*base_offset=*/0};

    llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
        builder.add_object(page_device_options);

    ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

    llfs::Status flush_status = builder.flush_all();

    EXPECT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
  }

  {
    llfs::StatusOr<std::vector<std::unique_ptr<llfs::StorageFileConfigBlock>>> config_blocks =
        llfs::read_storage_file(test_device, /*start_offset=*/0);

    ASSERT_TRUE(config_blocks.ok()) << BATT_INSPECT(config_blocks.status());

    ASSERT_EQ(config_blocks->size(), 1u);

    EXPECT_TRUE(this->verify_config_block(config_blocks->front()->get_const(),
                                          /*kExpectedPage0Offset=*/4096,
                                          /*kExpectedConfigBlockOffset=*/0, /*page_size_log2=*/12));

    llfs::StorageFile storage_file{"test_file", std::move(*config_blocks)};

    EXPECT_EQ(
        storage_file.find_objects_by_type<llfs::PackedPageDeviceConfig>() | llfs::seq::count(), 1u);
  }
}

}  // namespace
