//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_context.hpp>
//
#include <llfs/storage_context.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/constants.hpp>
#include <llfs/page_arena_config.hpp>
#include <llfs/raw_block_file_impl.hpp>
#include <llfs/uuid.hpp>

#include <boost/uuid/random_generator.hpp>

#include <filesystem>
#include <thread>

namespace {

using namespace llfs::constants;
using namespace llfs::int_types;

TEST(StorageContextTest, GetPageCache)
{
  const char* storage_file_name = "/tmp/llfs_StorageContextTest_GetPageCache_storage_file.llfs";

  llfs::delete_file(storage_file_name).IgnoreError();
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{storage_file_name}));

  llfs::StatusOr<llfs::ScopedIoRing> io =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  // Create a StorageContext.
  //
  batt::SharedPtr<llfs::StorageContext> storage_context = batt::make_shared<llfs::StorageContext>(
      batt::Runtime::instance().default_scheduler(), io->get_io_ring());

  boost::uuids::uuid arena_uuid_4kb = llfs::random_uuid();
  boost::uuids::uuid arena_uuid_2mb = llfs::random_uuid();

  // Create a storage file with two page arenas, one for small pages (4kb), one for large (2mb).
  //
  llfs::Status file_create_status = storage_context->add_new_file(
      storage_file_name, [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> config_4kb =
            builder.add_object(
                llfs::PageArenaConfigOptions{
                    .uuid = arena_uuid_4kb,
                    .page_allocator =
                        llfs::CreateNewPageAllocator{
                            .options =
                                llfs::PageAllocatorConfigOptions{
                                    .uuid = llfs::None,
                                    .max_attachments = 32,
                                    .page_count = llfs::PageCount{32},
                                    .log_device =
                                        llfs::CreateNewLogDevice2WithDefaultSize{
                                            .uuid = llfs::None,
                                            .device_page_size_log2 = 9,
                                            .data_alignment_log2 = 12,
                                        },
                                    .page_size_log2 = llfs::PageSizeLog2{12},
                                    .page_device = llfs::LinkToNewPageDevice{},
                                },
                        },
                    .page_device =
                        llfs::CreateNewPageDevice{
                            .options =
                                llfs::PageDeviceConfigOptions{
                                    .uuid = llfs::None,
                                    .device_id = llfs::None,
                                    .page_count = llfs::PageCount{32},
                                    .max_page_count = llfs::None,
                                    .page_size_log2 = llfs::PageSizeLog2{12},
                                    .last_in_file = llfs::None,
                                },
                        },
                });

        BATT_REQUIRE_OK(config_4kb);

        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> config_2mb =
            builder.add_object(
                llfs::PageArenaConfigOptions{
                    .uuid = arena_uuid_2mb,
                    .page_allocator =
                        llfs::CreateNewPageAllocator{
                            .options =
                                llfs::PageAllocatorConfigOptions{
                                    .uuid = llfs::None,
                                    .max_attachments = 32,
                                    .page_count = llfs::PageCount{32},
                                    .log_device =
                                        llfs::CreateNewLogDevice2WithDefaultSize{
                                            .uuid = llfs::None,
                                            .device_page_size_log2 = 9,
                                            .data_alignment_log2 = 12,
                                        },
                                    .page_size_log2 = llfs::PageSizeLog2{21},
                                    .page_device = llfs::LinkToNewPageDevice{},
                                },
                        },
                    .page_device =
                        llfs::CreateNewPageDevice{
                            .options =
                                llfs::PageDeviceConfigOptions{
                                    .uuid = llfs::None,
                                    .device_id = llfs::None,
                                    .page_count = llfs::PageCount{32},
                                    .max_page_count = llfs::None,
                                    .page_size_log2 = llfs::PageSizeLog2{21},
                                    .last_in_file = llfs::None,
                                },
                        },
                });

        BATT_REQUIRE_OK(config_2mb);

        return llfs::OkStatus();
      });
  ASSERT_TRUE(file_create_status.ok()) << BATT_INSPECT(file_create_status);

  llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> cache = storage_context->get_page_cache();
  ASSERT_TRUE(cache.ok()) << BATT_INSPECT(cache.status());
  ASSERT_NE(*cache, nullptr);

  llfs::Slice<llfs::PageDeviceEntry* const> devices_4kb =
      (*cache)->devices_with_page_size(4 * kKiB);
  EXPECT_EQ(devices_4kb.size(), 1u);

  llfs::Slice<llfs::PageDeviceEntry* const> devices_2mb =
      (*cache)->devices_with_page_size(2 * kMiB);
  EXPECT_EQ(devices_2mb.size(), 1u);
}

class DynamicStorageProvisioning : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->storage_file_name = "/tmp/llfs_StorageContextTest_GetPageCache_storage_file.llfs";

    llfs::delete_file(this->storage_file_name).IgnoreError();
    EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{this->storage_file_name}));

    this->io = llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

    ASSERT_TRUE(this->io.ok()) << BATT_INSPECT(this->io.status());

    // Create a StorageContext.
    //
    this->storage_context = batt::make_shared<llfs::StorageContext>(
        batt::Runtime::instance().default_scheduler(), this->io->get_io_ring());
  }

  batt::SharedPtr<llfs::StorageContext> storage_context;

  std::unique_ptr<llfs::PageDevice> page_device;

  llfs::StatusOr<llfs::ScopedIoRing> io;

  const char* storage_file_name;
};

TEST_F(DynamicStorageProvisioning, CreateMultipleLastFiles)
{
  // Create a storage file with one arena (4kb)
  //
  llfs::Status file_create_status = this->storage_context->add_new_file(
      this->storage_file_name, [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config1 =
            builder.add_object(llfs::PageDeviceConfigOptions{
                .uuid = llfs::None,
                .device_id = llfs::None,
                .page_count = llfs::PageCount{0},
                .max_page_count = llfs::PageCount{32},
                .page_size_log2 = llfs::PageSizeLog2{21},
                .last_in_file = true,
            });
        BATT_REQUIRE_OK(packed_config1);

        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config2 =
            builder.add_object(llfs::PageDeviceConfigOptions{
                .uuid = llfs::None,
                .device_id = llfs::None,
                .page_count = llfs::PageCount{0},
                .max_page_count = llfs::PageCount{32},
                .page_size_log2 = llfs::PageSizeLog2{21},
                .last_in_file = true,
            });
        BATT_REQUIRE_OK(packed_config2);

        return llfs::OkStatus();
      });

  // We expect this to fail because we have attempted to mark multiple PageDevices as
  // "last_in_file".
  //
  ASSERT_TRUE(file_create_status == llfs::StatusCode::kStorageObjectNotLastInFile)
      << BATT_INSPECT(file_create_status);
}

TEST_F(DynamicStorageProvisioning, InvalidOptionsDeath)
{
  // IgnoreError causes failed to die error when regex matches the error output.
  //
  EXPECT_DEATH(
      this->storage_context
          ->add_new_file(this->storage_file_name,
                         [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
                           llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>>
                               packed_config = builder.add_object(llfs::PageDeviceConfigOptions{
                                   .uuid = llfs::None,
                                   .device_id = llfs::None,
                                   .page_count = llfs::PageCount{1},
                                   .max_page_count = llfs::PageCount{0},
                                   .page_size_log2 = llfs::PageSizeLog2{21},
                                   .last_in_file = true,
                               });

                           return llfs::OkStatus();
                         })
          .IgnoreError(),
      "Invalid PageDevice Configuration");

  ASSERT_TRUE(llfs::delete_file(this->storage_file_name).ok());
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{this->storage_file_name}));

  EXPECT_DEATH(
      this->storage_context
          ->add_new_file(this->storage_file_name,
                         [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
                           llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>>
                               packed_config = builder.add_object(llfs::PageDeviceConfigOptions{
                                   .uuid = llfs::None,
                                   .device_id = llfs::None,
                                   .page_count = llfs::PageCount{1},
                                   .max_page_count = llfs::PageCount{2},
                                   .page_size_log2 = llfs::PageSizeLog2{21},
                                   .last_in_file = false,
                               });

                           return llfs::OkStatus();
                         })
          .IgnoreError(),
      "Invalid PageDevice Configuration");
}

class DynamicPageAllocation
    : public DynamicStorageProvisioning
    , public testing::WithParamInterface<std::pair<u64, u64>>
{
 public:
  void SetUp() override
  {
    DynamicStorageProvisioning::SetUp();

    std::tie(this->initial_page_count, this->max_page_count) = this->GetParam();
  }

  u64 initial_page_count;

  u64 max_page_count;
};

TEST_P(DynamicPageAllocation, PageDeviceGrows)
{
  boost::uuids::uuid page_device_uuid;

  // Create a storage file with one page device.
  //
  llfs::Status file_create_status = storage_context->add_new_file(
      storage_file_name, [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config1 =
            builder.add_object(llfs::PageDeviceConfigOptions{
                .uuid = llfs::None,
                .device_id = llfs::None,
                .page_count = llfs::PageCount{this->initial_page_count},
                .max_page_count = llfs::PageCount{this->max_page_count},
                .page_size_log2 = llfs::PageSizeLog2{21},
                .last_in_file = true,
            });
        BATT_REQUIRE_OK(packed_config1);
        page_device_uuid = (*packed_config1)->uuid;

        return llfs::OkStatus();
      });

  ASSERT_TRUE(file_create_status.ok()) << BATT_INSPECT(file_create_status);

  llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> cache = this->storage_context->get_page_cache();
  ASSERT_TRUE(cache.ok()) << BATT_INSPECT(cache.status());
  ASSERT_NE(*cache, nullptr);

  llfs::StatusOr<std::unique_ptr<llfs::PageDevice>> recovered_device =
      this->storage_context->recover_object(
          batt::StaticType<llfs::PackedPageDeviceConfig>{}, page_device_uuid,
          llfs::IoRingFileRuntimeOptions::with_default_values(io->get_io_ring()));

  ASSERT_TRUE(recovered_device.ok());
  ASSERT_TRUE((*recovered_device)->is_last_in_file());

  this->page_device = std::move(*recovered_device);

  std::filesystem::path llfs_file{this->storage_file_name};
  i64 original_file_size = std::filesystem::file_size(llfs_file);
  i64 prev_file_size = std::filesystem::file_size(llfs_file);
  i64 current_file_size = std::filesystem::file_size(llfs_file);
  u64 config_size =
      std::max(static_cast<usize>(this->page_device->page_size()), llfs::PackedConfigBlock::kSize);

  boost::asio::io_context io;

  batt::Task task(io.get_executor(), [&] {
    // Verify allocating the first this->initial_page_count pages doesn't grow the llfs file. These
    // pages should already exist in the file.
    //
    for (u64 i = 0; i < this->initial_page_count; ++i) {
      // Verify the file_size does not grow here.
      //
      ASSERT_EQ(current_file_size, original_file_size);

      batt::StatusOr<std::shared_ptr<llfs::PageBuffer>> page_buffer =
          this->page_device->prepare(static_cast<llfs::PageId>(i));

      ASSERT_TRUE(page_buffer.ok()) << BATT_INSPECT(page_buffer);

      batt::Status write_status = batt::Task::await<batt::Status>([&](auto&& handler) {
        this->page_device->write(*page_buffer, BATT_FORWARD(handler));
      });

      ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);

      // Verify the file is growing a single page at a time.
      //
      current_file_size = std::filesystem::file_size(llfs_file);
    }

    for (u64 i = this->initial_page_count; i < this->max_page_count; ++i) {
      // Verify the total file_size is equivalent to the number of allocated pages +
      // config size (in bytes).
      //
      u64 expected_file_size = config_size + i * this->page_device->page_size();
      ASSERT_EQ(current_file_size, expected_file_size);

      batt::StatusOr<std::shared_ptr<llfs::PageBuffer>> page_buffer =
          this->page_device->prepare(static_cast<llfs::PageId>(i));
      ASSERT_TRUE(page_buffer.ok()) << BATT_INSPECT(page_buffer);

      batt::Status write_status = batt::Task::await<batt::Status>([&](auto&& handler) {
        this->page_device->write(*page_buffer, BATT_FORWARD(handler));
      });

      ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);

      // Verify the file is growing a single page at a time.
      //
      current_file_size = std::filesystem::file_size(llfs_file);
      ASSERT_EQ(current_file_size - prev_file_size, this->page_device->page_size());

      prev_file_size = current_file_size;
    }
  });

  io.run();

  task.join();

  // Verify that the test suite ran
  //
  ASSERT_GT(current_file_size, original_file_size);
}

}  // namespace

INSTANTIATE_TEST_SUITE_P(FileGrowth, DynamicPageAllocation,
                         testing::Values(std::pair{0, 1}, std::pair{0, 8}, std::pair{0, 128},
                                         std::pair{1, 5}, std::pair{1, 16}, std::pair{5, 256},
                                         std::pair{255, 256}));
