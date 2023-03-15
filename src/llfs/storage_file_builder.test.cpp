#include <llfs/storage_file_builder.hpp>
//
#include <llfs/storage_file_builder.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/filesystem.hpp>
#include <llfs/ioring.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_device_config.hpp>
#include <llfs/raw_block_file_impl.hpp>
#include <llfs/raw_block_file_mock.hpp>
#include <llfs/storage_file.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/math.hpp>

namespace {

using namespace llfs::int_types;

constexpr usize kTestPageCount = 10;

class StorageFileBuilderTest : public ::testing::Test
{
 protected:
  bool verify_packed_config_block(const llfs::PackedConfigBlock& config_block,
                                  const i64 kExpectedPage0Offset,
                                  const i64 kExpectedConfigBlockOffset, u32 page_size_log2)
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

  bool verify_storage_file_config_blocks(
      std::vector<std::unique_ptr<llfs::StorageFileConfigBlock>>& config_blocks, u32 page_size_log2)
  {
    // Cycle through each packed config block
    //  Cycle through each of the slots, calculating the offset for each
    //  Check that packed config block properties are correct correct

    i64 kExpectedPrevOffset = 0;
    i64 kExpectedNextOffset = 0;
    u64 kExpectedDeviceId = 0;
    u64 kExpectedNumSlots;

    // Iterate through each StorageFileConfigBlock
    for (std::unique_ptr<llfs::StorageFileConfigBlock>& config_block : config_blocks) {
      const llfs::PackedConfigBlock& p_config_block = config_block->get_const();
      i64 kExpectedPage0Offset = 4032;

      // Iterate through each PackedPageDeviceConfig
      for (const llfs::PackedConfigSlot slot : p_config_block.slots) {
        // Verify PackedPageDeviceConfig
        llfs::PackedPageDeviceConfig page_device_config =
            llfs::config_slot_cast<llfs::PackedPageDeviceConfig>(slot);
        if (!(page_device_config.tag == llfs::PackedConfigSlot::Tag::kPageDevice &&
              page_device_config.page_0_offset == kExpectedPage0Offset &&

              page_device_config.device_id == kExpectedDeviceId &&

              page_device_config.page_count == kTestPageCount &&

              page_device_config.page_size_log2 == page_size_log2)) {
          return false;
        }
        // Update page offset numbers and device ID
        kExpectedPage0Offset += page_device_config.page_size() * kTestPageCount - /*slot_size*/ 64;
        kExpectedDeviceId += 1;
      }
      if (kExpectedDeviceId % 62 == 0) {
        kExpectedNumSlots = 62;
      } else {
        kExpectedNumSlots = kExpectedDeviceId % 62;
      }
      // Set offsets of config block based on position
      if (config_block == config_blocks.front()) {
        kExpectedPrevOffset = llfs::PackedConfigBlock::kNullFileOffset;
      } else {
        kExpectedPrevOffset = -kExpectedNextOffset;
      }
      if (config_block == config_blocks.back()) {
        kExpectedNextOffset = llfs::PackedConfigBlock::kNullFileOffset;
      } else {
        kExpectedNextOffset =
            batt::round_up_bits(12, kExpectedPage0Offset + 64 * kExpectedNumSlots);
      }
      // Verify the packed congig block
      if (!(p_config_block.magic == llfs::PackedConfigBlock::kMagic &&
            p_config_block.version == llfs::make_version_u64(0, 1, 0) &&
            p_config_block.prev_offset == kExpectedPrevOffset &&
            p_config_block.next_offset == kExpectedNextOffset &&
            p_config_block.slots.size() == kExpectedNumSlots &&
            p_config_block.crc64 == p_config_block.true_crc64())) {
        return false;
      }
    }
    return true;
  }

  struct alignas(512) randStruct {
    char randData[512];
  };

  void write_rand_data(llfs::IoRingRawBlockFile& test_file, i64 write_offset, i64 rand_buffer_len)
  {
    i64 struct_array_len = batt::round_up_bits(9, rand_buffer_len);
    i64 write_offset_aligned = batt::round_up_bits(12, write_offset);
    randStruct randStructArray[struct_array_len / 512];
    int rand_fd = open("/dev/random", O_RDONLY);
    i64 rand_bytes_read = 0;

    for (int i = 0; i < (struct_array_len / 512); i++) {
      randStructArray[i] = randStruct();
      rand_bytes_read += ::read(rand_fd, &randStructArray[i].randData, 512);
    }
    ASSERT_EQ(struct_array_len, rand_bytes_read);

    const llfs::ConstBuffer randBuffer(randStructArray, struct_array_len);

    llfs::StatusOr<i64> randWriteLen = test_file.write_some(write_offset_aligned, randBuffer);
    ASSERT_TRUE(randWriteLen.ok()) << BATT_INSPECT(randWriteLen.status());
    ASSERT_EQ(struct_array_len, *randWriteLen);
  }
};

// Test Plan:
//  1. Create StorageFileBuilder, add nothing, don't flush, no action taken.
//  2. Add a PageDeviceConfig, verify the correct slot is "written" to the file mock.
//  3. Fill all the slots in the PackedConfigBlock, verify the chain is set up, in the correct
//  order.
//

TEST_F(StorageFileBuilderTest, NoConfigs_Mock)
{
  ::testing::StrictMock<llfs::RawBlockFileMock> file_mock;

  llfs::StorageFileBuilder builder{file_mock, /*base_offset=*/0};
}

TEST_F(StorageFileBuilderTest, NoConfigs)
{
  llfs::StatusOr<llfs::ScopedIoRing> ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  const char* const test_file_name = "/dev/nvme8n1";
  llfs::StatusOr<int> test_fd =
      llfs::open_file_read_write(test_file_name, llfs::OpenForAppend{false}, llfs::OpenRawIO{true});
  ASSERT_TRUE(test_fd.ok()) << BATT_INSPECT(test_fd.status());

  llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

  llfs::StorageFileBuilder builder{test_file, /*base_offset=*/0};
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_NoFlush_Mock)
{
  ::testing::StrictMock<llfs::RawBlockFileMock> file_mock;

  llfs::StorageFileBuilder builder{file_mock, /*base_offset=*/0};

  llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
      builder.add_object(llfs::PageDeviceConfigOptions{
          .uuid = llfs::None,
          .device_id = llfs::None,
          .page_count = llfs::PageCount{kTestPageCount},
          .page_size_log2 = llfs::PageSizeLog2{12},
      });

  ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_NoFlush)
{
  llfs::StatusOr<llfs::ScopedIoRing> ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  const char* const test_file_name = "/dev/nvme8n1";
  llfs::StatusOr<int> test_fd =
      llfs::open_file_read_write(test_file_name, llfs::OpenForAppend{false}, llfs::OpenRawIO{true});
  ASSERT_TRUE(test_fd.ok()) << BATT_INSPECT(test_fd.status());

  llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

  llfs::StorageFileBuilder builder{test_file, /*base_offset=*/0};

  llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
      builder.add_object(llfs::PageDeviceConfigOptions{
          .uuid = llfs::None,
          .device_id = llfs::None,
          .page_count = llfs::PageCount{kTestPageCount},
          .page_size_log2 = llfs::PageSizeLog2{12},
      });

  ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_Flush_Mock)
{
  for (i64 base_file_offset : {0, 128, 65536}) {
    for (u32 page_size_log2 : {9, 10, 11, 12, 13, 16, 24}) {
      ::testing::StrictMock<llfs::RawBlockFileMock> file_mock;

      llfs::StorageFileBuilder builder{file_mock, base_file_offset};

      const auto options = llfs::PageDeviceConfigOptions{
          .uuid = llfs::None,
          .device_id = llfs::None,
          .page_count = llfs::PageCount{kTestPageCount},
          .page_size_log2 = llfs::PageSizeLog2{page_size_log2},
      };
      const usize kTestPageSize = usize{1} << options.page_size_log2;

      llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
          builder.add_object(options);

      ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

      const i64 kExpectedConfigBlockOffset = batt::round_up_bits(12, base_file_offset);

      const i64 kExpectedPage0Offset = batt::round_up_bits(
          page_size_log2, kExpectedConfigBlockOffset + sizeof(llfs::PackedConfigBlock));

      const i64 kExpectedFileSize = kExpectedPage0Offset + options.page_count * kTestPageSize;

      ::testing::Sequence flush_sequence;

      EXPECT_CALL(file_mock, truncate_at_least(::testing::Eq(kExpectedFileSize)))
          .InSequence(flush_sequence)
          .WillOnce(::testing::Return(llfs::OkStatus()));

      if (!llfs::kFastIoRingPageDeviceInit) {
        EXPECT_CALL(file_mock, write_some(::testing::Gt(kExpectedConfigBlockOffset),
                                          ::testing::Truly([](const llfs::ConstBuffer& b) {
                                            return b.size() == 512;
                                          })))
            .Times(options.page_count)
            .InSequence(flush_sequence)
            .WillRepeatedly(::testing::Return(512));
      }

      EXPECT_CALL(file_mock,
                  write_some(::testing::Eq(kExpectedConfigBlockOffset),
                             ::testing::Truly([&](const llfs::ConstBuffer& data) {
                               if (data.size() != 4096) {
                                 return false;
                               }
                               auto& config_block =
                                   *static_cast<const llfs::PackedConfigBlock*>(data.data());

                               return this->verify_packed_config_block(
                                   config_block, kExpectedPage0Offset, kExpectedConfigBlockOffset,
                                   page_size_log2);
                             })))
          .InSequence(flush_sequence)
          .WillOnce(::testing::Return(llfs::StatusOr<i64>{4096}));

      llfs::Status flush_status = builder.flush_all();

      ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
    }
  }
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_Flush)
{
  for (i64 base_file_offset : {0, 128, 65536}) {
    for (u32 page_size_log2 : {9, 10, 11, 12, 13, 16, 24}) {
      llfs::StatusOr<llfs::ScopedIoRing> ioring =
          llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

      ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

      const char* const test_file_name = "/dev/nvme8n1";
      llfs::StatusOr<int> test_fd = llfs::open_file_read_write(
          test_file_name, llfs::OpenForAppend{false}, llfs::OpenRawIO{true});
      ASSERT_TRUE(test_fd.ok()) << BATT_INSPECT(test_fd.status());

      llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

      llfs::StorageFileBuilder builder{test_file, base_file_offset};

      const auto options = llfs::PageDeviceConfigOptions{
          .uuid = llfs::None,
          .device_id = llfs::None,
          .page_count = llfs::PageCount{kTestPageCount},
          .page_size_log2 = llfs::PageSizeLog2{page_size_log2},
      };
      const usize kTestPageSize = 1ll << options.page_size_log2;

      llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
          builder.add_object(options);

      ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

      const i64 kExpectedConfigBlockOffset = batt::round_up_bits(12, base_file_offset);

      const i64 kExpectedPage0Offset = batt::round_up_bits(
          page_size_log2, kExpectedConfigBlockOffset + sizeof(llfs::PackedConfigBlock));

      const i64 kExpectedFileSize = kExpectedPage0Offset + options.page_count * kTestPageSize;

      const llfs::ConstBuffer& buffer = llfs::ConstBuffer();

      llfs::Status truncate_status = test_file.truncate_at_least(kExpectedFileSize);
      ASSERT_TRUE(truncate_status.ok()) << BATT_INSPECT(truncate_status);

      if (!llfs::kFastIoRingPageDeviceInit) {
        llfs::StatusOr<i64> write_some_result =
            test_file.write_some(kExpectedConfigBlockOffset, buffer);
        ASSERT_TRUE(write_some_result.ok()) << BATT_INSPECT(write_some_result.status());
        ASSERT_EQ(*write_some_result, 512);
      }

      llfs::PackedConfigBlock configBlock = llfs::PackedConfigBlock{};
      const llfs::ConstBuffer configBlockBuffer = llfs::ConstBuffer(&configBlock, 4096);

      llfs::StatusOr<i64> blockStructWriteResult =
          test_file.write_some(kExpectedConfigBlockOffset, configBlockBuffer);
      ASSERT_TRUE(blockStructWriteResult.ok()) << BATT_INSPECT(blockStructWriteResult.status());
      ASSERT_EQ(*blockStructWriteResult, 4096);

      llfs::Status flush_status = builder.flush_all();

      ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(StorageFileBuilderTest, WriteReadFile)
{
  llfs::StatusOr<llfs::ScopedIoRing> ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  auto storage_context = batt::make_shared<llfs::StorageContext>(
      batt::Runtime::instance().default_scheduler(), ioring->get_io_ring());

  const char* const test_file_name = "/tmp/llfs_test_file";
  std::filesystem::remove(test_file_name);
  boost::uuids::uuid page_device_uuid;

  {
    llfs::StatusOr<int> test_fd =
        llfs::create_file_read_write(test_file_name, llfs::OpenForAppend{false});
    ASSERT_TRUE(test_fd.ok()) << BATT_INSPECT(test_fd.status());
    {
      llfs::Status status = llfs::enable_raw_io_fd(*test_fd, /*enabled=*/true);
      ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
    }
    llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

    const auto page_device_options = llfs::PageDeviceConfigOptions{
        .uuid = llfs::None,
        .device_id = llfs::None,
        .page_count = llfs::PageCount{kTestPageCount},
        .page_size_log2 = llfs::PageSizeLog2{12} /* 4096 */,
    };

    {
      llfs::StorageFileBuilder builder{test_file, /*base_offset=*/0};

      llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
          builder.add_object(page_device_options);

      ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

      // Save this for later.
      //
      page_device_uuid = (*packed_config)->uuid;

      llfs::Status flush_status = builder.flush_all();

      ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
    }

    {
      llfs::StatusOr<std::vector<std::unique_ptr<llfs::StorageFileConfigBlock>>> config_blocks =
          llfs::read_storage_file(test_file, /*start_offset=*/0);

      ASSERT_TRUE(config_blocks.ok()) << BATT_INSPECT(config_blocks.status());

      ASSERT_EQ(config_blocks->size(), 1u);

      EXPECT_TRUE(this->verify_storage_file_config_blocks(*config_blocks, /*page_size_log2=*/12));

      auto storage_file =
          batt::make_shared<llfs::StorageFile>(test_file_name, std::move(*config_blocks));

      EXPECT_EQ(
          storage_file->find_objects_by_type<llfs::PackedPageDeviceConfig>() | llfs::seq::count(),
          1u);

      {
        llfs::Status status = storage_context->add_existing_file(storage_file);
        ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
      }
    }
  }

  llfs::StatusOr<std::unique_ptr<llfs::PageDevice>> recovered_device =
      storage_context->recover_object(
          batt::StaticType<llfs::PackedPageDeviceConfig>{}, page_device_uuid,
          llfs::IoRingFileRuntimeOptions::with_default_values(ioring->get_io_ring()));

  ASSERT_TRUE(recovered_device.ok()) << BATT_INSPECT(recovered_device.status());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(StorageFileBuilderTest, WriteReadBlockFile)
{
  llfs::StatusOr<llfs::ScopedIoRing> ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  auto storage_context = batt::make_shared<llfs::StorageContext>(
      batt::Runtime::instance().default_scheduler(), ioring->get_io_ring());

  const char* const test_file_name = "/dev/nvme8n1";
  boost::uuids::uuid page_device_uuid;

  {
    llfs::StatusOr<int> test_fd = llfs::open_file_read_write(
        test_file_name, llfs::OpenForAppend{false}, llfs::OpenRawIO{true});
    ASSERT_TRUE(test_fd.ok()) << BATT_INSPECT(test_fd.status());

    llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

    this->write_rand_data(test_file, /*write_offset=*/0, /*rand_buffer_len=*/8192);

    const auto page_device_options = llfs::PageDeviceConfigOptions{
        .uuid = llfs::None,
        .device_id = llfs::None,
        .page_count = llfs::PageCount{kTestPageCount},
        .page_size_log2 = llfs::PageSizeLog2{12} /* 4096 */,
    };

    {
      llfs::StorageFileBuilder builder{test_file, /*base_offset=*/0};

      llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
          builder.add_object(page_device_options);

      ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

      // Save this for later.
      //
      page_device_uuid = (*packed_config)->uuid;

      llfs::Status flush_status = builder.flush_all();

      ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
    }

    {
      llfs::StatusOr<std::vector<std::unique_ptr<llfs::StorageFileConfigBlock>>> config_blocks =
          llfs::read_storage_file(test_file, /*start_offset=*/0);

      ASSERT_TRUE(config_blocks.ok()) << BATT_INSPECT(config_blocks.status());

      ASSERT_EQ(config_blocks->size(), 1u);

      EXPECT_TRUE(this->verify_storage_file_config_blocks(*config_blocks, /*page_size_log2=*/12));

      auto storage_file =
          batt::make_shared<llfs::StorageFile>(test_file_name, std::move(*config_blocks));

      EXPECT_EQ(
          storage_file->find_objects_by_type<llfs::PackedPageDeviceConfig>() | llfs::seq::count(),
          1u);

      {
        llfs::Status status = storage_context->add_existing_file(storage_file);
        ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
      }
    }
  }

  llfs::StatusOr<std::unique_ptr<llfs::PageDevice>> recovered_device =
      storage_context->recover_object(
          batt::StaticType<llfs::PackedPageDeviceConfig>{}, page_device_uuid,
          llfs::IoRingFileRuntimeOptions::with_default_values(ioring->get_io_ring()));

  ASSERT_TRUE(recovered_device.ok()) << BATT_INSPECT(recovered_device.status());
}

//---------------------------------------------------------------------------------------------------

TEST_F(StorageFileBuilderTest, WriteReadManyPackedConfigs)
{
  i64 base_file_offset = 0;
  llfs::StatusOr<llfs::ScopedIoRing> ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  const char* const test_file_name = "/dev/nvme8n1";
  llfs::StatusOr<int> test_fd =
      llfs::open_file_read_write(test_file_name, llfs::OpenForAppend{false}, llfs::OpenRawIO{true});
  ASSERT_TRUE(test_fd.ok()) << BATT_INSPECT(test_fd.status());

  llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

  this->write_rand_data(test_file, /*write_offset=*/base_file_offset, /*rand_buffer_len=*/700000);

  llfs::StorageFileBuilder builder{test_file, base_file_offset};

  boost::uuids::uuid page_device_uuid;

  auto storage_context = batt::make_shared<llfs::StorageContext>(
      batt::Runtime::instance().default_scheduler(), ioring->get_io_ring());

  // Fill enough slots to create 3 PackedConfigBlocks
  for (i64 config_options_count = 0; config_options_count < 125; config_options_count++) {
    const auto options = llfs::PageDeviceConfigOptions{
        .uuid = llfs::None,
        .device_id = llfs::None,
        .page_count = llfs::PageCount{kTestPageCount},
        .page_size_log2 = llfs::PageSizeLog2{9},
    };

    llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
        builder.add_object(options);

    ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

    // Save this for later.
    //
    page_device_uuid = (*packed_config)->uuid;
  }

  llfs::Status flush_status = builder.flush_all();

  ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);

  {
    llfs::StatusOr<std::vector<std::unique_ptr<llfs::StorageFileConfigBlock>>> config_blocks =
        llfs::read_storage_file(test_file, base_file_offset);

    ASSERT_TRUE(config_blocks.ok()) << BATT_INSPECT(config_blocks.status());

    ASSERT_EQ(config_blocks->size(), 3u);

    EXPECT_TRUE(this->verify_storage_file_config_blocks(*config_blocks, /*page_size_log2=*/9));

    auto storage_file =
        batt::make_shared<llfs::StorageFile>(test_file_name, std::move(*config_blocks));

    EXPECT_EQ(
        storage_file->find_objects_by_type<llfs::PackedPageDeviceConfig>() | llfs::seq::count(),
        125u);

    {
      llfs::Status status = storage_context->add_existing_file(storage_file);
      ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
    }

    llfs::StatusOr<std::unique_ptr<llfs::PageDevice>> recovered_device =
        storage_context->recover_object(
            batt::StaticType<llfs::PackedPageDeviceConfig>{}, page_device_uuid,
            llfs::IoRingFileRuntimeOptions::with_default_values(ioring->get_io_ring()));

    ASSERT_TRUE(recovered_device.ok()) << BATT_INSPECT(recovered_device.status());
  }
}

}  // namespace
