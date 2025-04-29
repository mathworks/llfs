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
                                    .max_page_count = llfs::PageCount{32},
                                    .page_size_log2 = llfs::PageSizeLog2{12},
                                    .last_in_file = false,
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
                                    .max_page_count = llfs::PageCount{32},
                                    .page_size_log2 = llfs::PageSizeLog2{21},
                                    .last_in_file = false,
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

    llfs::delete_file(storage_file_name).IgnoreError();
    EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{storage_file_name}));

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

TEST_F(DynamicStorageProvisioning, PageDeviceGrows)
{
  boost::uuids::uuid page_device_uuid;

  u64 max_page_count = 128;

  // Create a storage file with one arena (4kb)
  //
  llfs::Status file_create_status = storage_context->add_new_file(
      storage_file_name, [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageDeviceConfig&>> packed_config1 =
            builder.add_object(llfs::PageDeviceConfigOptions{
                .uuid = llfs::None,
                .device_id = llfs::None,
                .page_count = llfs::PageCount{0},
                .max_page_count = llfs::PageCount{max_page_count},
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

  BATT_CHECK_OK(recovered_device);
  BATT_CHECK((*recovered_device)->is_last_in_file());

  this->page_device = std::move(*recovered_device);

  std::filesystem::path llfs_file{this->storage_file_name};
  int original_file_size = std::filesystem::file_size(llfs_file);
  int current_file_size = std::filesystem::file_size(llfs_file);
  LOG(INFO) << "original_file_size==" << original_file_size;

  boost::asio::io_context io;

  boost::asio::post(io.get_executor(), [&] {
    for (u64 i = 0; i < max_page_count; ++i) {
      auto handler = [](batt::Status status) {
        LOG(INFO) << "Handler invoked";
        BATT_CHECK_OK(status);
      };

      batt::StatusOr<std::shared_ptr<llfs::PageBuffer>> page_buffer =
          this->page_device->prepare(static_cast<llfs::PageId>(i));
      BATT_CHECK_OK(page_buffer);
      this->page_device->write(*page_buffer, handler);

      current_file_size = std::filesystem::file_size(llfs_file);
      LOG(INFO) << "original_file_size==" << original_file_size
                << ", current_file_size==" << current_file_size;
    }
  });

  std::thread thread1{[&io] {
    io.run();
  }};

  thread1.join();

  // We expect file size to have grown after several writes.
  // Note: File growth characteristics appear to vary. Sometimes, file size grows with every write.
  // Sometimes, it will not grow after several writes.
  //
  BATT_CHECK_GT(current_file_size, original_file_size);

  llfs::PageCacheMetrics& metrics = (*cache)->metrics();
  LOG(INFO) << "metrics.get_page_view_count: " << metrics.get_page_view_count.load();
}

TEST_F(DynamicStorageProvisioning, CreateMultipleLastFiles)
{
  // Create a storage file with one arena (4kb)
  //
  llfs::Status file_create_status = storage_context->add_new_file(
      storage_file_name, [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
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
  ASSERT_FALSE(file_create_status.ok()) << BATT_INSPECT(file_create_status);
}

}  // namespace
