//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_file_builder.hpp>
//
#include <llfs/storage_file_builder.hpp>

#include <random>

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

  void verify_storage_file_config_blocks(
      const std::vector<std::unique_ptr<llfs::StorageFileConfigBlock>>& config_blocks,
      u32 page_size_log2)
  {
    // Cycle through each packed config block
    //  Cycle through each of the slots, calculating the offset for each
    //  Check that packed config block properties are correct

    i64 expected_prev_offset = 0;
    i64 expected_next_offset = 0;
    u64 expected_device_id = 0;

    // Iterate through each StorageFileConfigBlock
    //
    for (const std::unique_ptr<llfs::StorageFileConfigBlock>& config_block : config_blocks) {
      const llfs::PackedConfigBlock& p_config_block = config_block->get_const();
      i64 expected_page0_offset =
          llfs::PackedConfigBlock::kPayloadCapacity + sizeof(llfs::PackedConfigBlock::crc64);

      // Iterate through each PackedPageDeviceConfig
      //
      for (const llfs::PackedConfigSlot& slot : p_config_block.slots) {
        // Verify PackedPageDeviceConfig
        //
        ASSERT_EQ(slot.tag, llfs::PackedConfigSlotBase::Tag::kPageDevice);
        const llfs::PackedPageDeviceConfig& page_device_config =
            llfs::config_slot_cast<llfs::PackedPageDeviceConfig>(slot);
        ASSERT_EQ(page_device_config.page_0_offset, expected_page0_offset);
        ASSERT_EQ(page_device_config.device_id, expected_device_id);
        ASSERT_EQ(page_device_config.page_count, (little_i64)kTestPageCount);
        ASSERT_EQ(page_device_config.page_size_log2, page_size_log2);

        // Update page offset numbers and device ID
        //
        expected_page0_offset +=
            page_device_config.page_size() * kTestPageCount - llfs::PackedConfigSlot::kSize;
        expected_device_id += 1;
      }

      // Assuming 1 slot filled if back slot, all slots filled otherwise.
      //
      u64 expected_num_slots =
          (config_block == config_blocks.back()) ? 1 : llfs::PackedConfigBlock::kMaxSlots;

      // Set offsets of config block based on position
      //
      if (config_block == config_blocks.front()) {
        expected_prev_offset = llfs::PackedConfigBlock::kNullFileOffset;
      } else {
        expected_prev_offset = -expected_next_offset;
      }
      if (config_block == config_blocks.back()) {
        expected_next_offset = llfs::PackedConfigBlock::kNullFileOffset;
      } else {
        expected_next_offset = batt::round_up_bits(
            12, expected_page0_offset + llfs::PackedPageDeviceConfig::kSize * expected_num_slots);
      }

      // Verify the packed config block
      //
      ASSERT_EQ(p_config_block.magic, llfs::PackedConfigBlock::kMagic);
      ASSERT_EQ(p_config_block.version, llfs::make_version_u64(0, 1, 0));
      ASSERT_EQ(p_config_block.prev_offset, expected_prev_offset);
      ASSERT_EQ(p_config_block.next_offset, expected_next_offset);
      ASSERT_EQ(p_config_block.slots.size(), expected_num_slots);
      ASSERT_EQ(p_config_block.crc64, p_config_block.true_crc64());
    }
  }

  // Writes random data to disk given the file, offset, and amount of data to write. write_offset
  // and write_size must be multiples of the block size.
  //
  llfs::Status write_rand_data(llfs::IoRingRawBlockFile& test_file, i64 write_offset,
                               i64 write_size)
  {
    constexpr i64 kMaxBufferSize = 32768;
    constexpr i64 kBufferAlign = llfs::kDirectIOBlockAlign;

    if (write_offset % kBufferAlign != 0 || write_size % kBufferAlign != 0) {
      return batt::Status{batt::StatusCode::kInvalidArgument};
    }

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<u64> distrib(std::numeric_limits<u64>::min(),
                                               std::numeric_limits<u64>::max());

    union {
      std::aligned_storage_t<kBufferAlign, kBufferAlign> alignment;
      std::array<u64, kMaxBufferSize / sizeof(u64)> items;
    } rand_data;

    while (write_size > 0) {
      u64 buffer_size = std::min(kMaxBufferSize, write_size);

      // Fill random data in block aligned rand_data
      //
      for (u64 i = 0; i < buffer_size / sizeof(u64); i++) {
        rand_data.items[i] = distrib(gen);
      }

      // Write random data to disk and iterate offset and size counters
      //
      llfs::ConstBuffer write_buffer(&rand_data, buffer_size);

      llfs::StatusOr<u64> write_count = test_file.write_some(write_offset, write_buffer);
      BATT_REQUIRE_OK(write_count);
      if (*write_count != buffer_size) {
        return batt::Status{batt::StatusCode::kUnknown};
      }

      write_offset += *write_count;
      write_size -= *write_count;
    }

    return (write_size == 0) ? batt::OkStatus() : batt::StatusCode::kUnknown;
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
  ::testing::StrictMock<llfs::RawBlockFileMock> file_mock;

  llfs::StorageFileBuilder builder{file_mock, /*base_offset=*/0};
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_NoFlush)
{
  ::testing::StrictMock<llfs::RawBlockFileMock> file_mock;

  llfs::StorageFileBuilder builder{file_mock, /*base_offset=*/0};

  llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
      builder.add_object(llfs::PageDeviceConfigOptions{
          .uuid = llfs::None,
          .device_id = llfs::None,
          .page_count = llfs::PageCount{kTestPageCount},
          .max_page_count = llfs::None,
          .page_size_log2 = llfs::PageSizeLog2{12},
          .last_in_file = llfs::None,
      });

  ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());
}

TEST_F(StorageFileBuilderTest, PageDeviceConfig_Flush)
{
  for (i64 base_file_offset : {0, 128, 65536}) {
    for (u32 page_size_log2 : {9, 10, 11, 12, 13, 16, 24}) {
      ::testing::StrictMock<llfs::RawBlockFileMock> file_mock;

      llfs::StorageFileBuilder builder{file_mock, base_file_offset};

      const auto options = llfs::PageDeviceConfigOptions{
          .uuid = llfs::None,
          .device_id = llfs::None,
          .page_count = llfs::PageCount{kTestPageCount},
          .max_page_count = llfs::None,
          .page_size_log2 = llfs::PageSizeLog2{page_size_log2},
          .last_in_file = llfs::None,
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
                                            return b.size() == llfs::kDirectIOBlockSize;
                                          })))
            .Times(options.page_count)
            .InSequence(flush_sequence)
            .WillRepeatedly(::testing::Return(llfs::kDirectIOBlockSize));
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(StorageFileBuilderTest, WriteReadFile)
{
  llfs::StatusOr<llfs::ScopedIoRing> ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  auto storage_context = llfs::StorageContext::make_shared(
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
      EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);
    }
    llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

    const auto page_device_options = llfs::PageDeviceConfigOptions{
        .uuid = llfs::None,
        .device_id = llfs::None,
        .page_count = llfs::PageCount{kTestPageCount},
        .max_page_count = llfs::None,
        .page_size_log2 = llfs::PageSizeLog2{12} /* 4096 */,
        .last_in_file = llfs::None,
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

      ASSERT_NO_FATAL_FAILURE(
          this->verify_storage_file_config_blocks(*config_blocks, /*page_size_log2=*/12));

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
// Create a storage file builder and populate with enough llfs::PageDeviceConfigOptions to create 3
// llfs::PackedConfigBlock. Flush/Write to a flat file, and read from that file to verify each
// llfs::PackedConfigBlock and associated llfs::PackedPageDeviceConfig is correct. Lastly, verify
// PageDevice can be recovered from UUID.
//
TEST_F(StorageFileBuilderTest, WriteReadManyPackedConfigs)
{
  const i64 kBaseFileOffset = 0;
  const u64 kNumPackedConfigBlocks = 3;
  const u64 kSlots = (llfs::PackedConfigBlock::kMaxSlots * (kNumPackedConfigBlocks - 1)) + 1;

  llfs::StatusOr<llfs::ScopedIoRing> ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(ioring.ok()) << BATT_INSPECT(ioring.status());

  // Create flat storage file for testing
  //
  const char* const test_file_name = "/tmp/llfs_test_file";
  std::filesystem::remove(test_file_name);
  boost::uuids::uuid page_device_uuid;

  llfs::StatusOr<int> test_fd =
      llfs::create_file_read_write(test_file_name, llfs::OpenForAppend{false});
  ASSERT_TRUE(test_fd.ok()) << BATT_INSPECT(test_fd.status());
  {
    llfs::Status status = llfs::enable_raw_io_fd(*test_fd, /*enabled=*/true);
    EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);
  }
  llfs::IoRingRawBlockFile test_file{llfs::IoRing::File{ioring->get_io_ring(), *test_fd}};

  // Write random data to test storage file to eliminate drool from causing false test passes
  //
  {
    llfs::Status write_status = this->write_rand_data(
        test_file, /*write_offset=*/kBaseFileOffset,
        /*write_size=*/kBaseFileOffset + (kNumPackedConfigBlocks * llfs::PackedConfigBlock::kSize) +
            (kSlots * kTestPageCount * llfs::kDirectIOBlockSize));

    ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);
  }

  llfs::StorageFileBuilder builder{test_file, kBaseFileOffset};

  // Create StorageContext for testing recovery of PageDevice using UUID later
  //
  auto storage_context = llfs::StorageContext::make_shared(
      batt::Runtime::instance().default_scheduler(), ioring->get_io_ring());

  // Add PageDeviceConfigOptions to StorageFileBuilder to create 3 PackedConfigBlocks, where the
  // last only has 1 slot filled
  //
  for (u64 slot_count = 0; slot_count < kSlots; slot_count++) {
    const auto options = llfs::PageDeviceConfigOptions{
        .uuid = llfs::None,
        .device_id = llfs::None,
        .page_count = llfs::PageCount{kTestPageCount},
        .max_page_count = llfs::None,
        .page_size_log2 = llfs::PageSizeLog2{llfs::kDirectIOBlockSizeLog2},
        .last_in_file = llfs::None,
    };

    llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config =
        builder.add_object(options);

    ASSERT_TRUE(packed_config.ok()) << BATT_INSPECT(packed_config.status());

    // Save this for later.
    //
    page_device_uuid = (*packed_config)->uuid;
  }

  // Flush StorageFileBuilder to the test storage file
  //
  llfs::Status flush_status = builder.flush_all();

  ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);

  {
    // Read storage file and verify count and contents of the llfs::PackedConfigBlock and
    // associated llfs::PackedPageDeviceConfig
    //
    llfs::StatusOr<std::vector<std::unique_ptr<llfs::StorageFileConfigBlock>>> config_blocks =
        llfs::read_storage_file(test_file, kBaseFileOffset);

    ASSERT_TRUE(config_blocks.ok()) << BATT_INSPECT(config_blocks.status());

    ASSERT_EQ(config_blocks->size(), kNumPackedConfigBlocks);

    ASSERT_NO_FATAL_FAILURE(this->verify_storage_file_config_blocks(
        *config_blocks, /*page_size_log2=*/llfs::kDirectIOBlockSizeLog2));

    // Create StorageFile and add to StorageContext
    //
    auto storage_file =
        batt::make_shared<llfs::StorageFile>(test_file_name, std::move(*config_blocks));

    EXPECT_EQ(
        storage_file->find_objects_by_type<llfs::PackedPageDeviceConfig>() | llfs::seq::count(),
        kSlots);

    {
      llfs::Status status = storage_context->add_existing_file(storage_file);
      ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
    }
  }

  // Verify PageDevice can be recovered using previously saved UUID from StorageContext
  //
  llfs::StatusOr<std::unique_ptr<llfs::PageDevice>> recovered_device =
      storage_context->recover_object(
          batt::StaticType<llfs::PackedPageDeviceConfig>{}, page_device_uuid,
          llfs::IoRingFileRuntimeOptions::with_default_values(ioring->get_io_ring()));

  ASSERT_TRUE(recovered_device.ok()) << BATT_INSPECT(recovered_device.status());
}

}  // namespace
