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
#include <llfs/ioring_log_device.hpp>
#include <llfs/opaque_page_view.hpp>
#include <llfs/page_arena_config.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/raw_block_file_impl.hpp>
#include <llfs/uuid.hpp>

#include <boost/uuid/random_generator.hpp>

#include <filesystem>
#include <thread>

namespace {

using namespace llfs::constants;
using namespace llfs::int_types;

class StorageContextTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->dir_name = "/tmp/";
    const char* storage_file_name = "/tmp/llfs_StorageContextTest_GetPageCache_storage_file.llfs";

    llfs::delete_file(storage_file_name).IgnoreError();
    EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{storage_file_name}));

    this->io = llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});

    ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());
    // Create a StorageContext.
    //
    this->storage_context = batt::make_shared<llfs::StorageContext>(
        batt::Runtime::instance().default_scheduler(), io->get_io_ring());

    boost::uuids::uuid arena_uuid_4kb = llfs::random_uuid();
    boost::uuids::uuid arena_uuid_2mb = llfs::random_uuid();

    // Create a storage file with two page arenas, one for small pages (4kb), one for large (2mb).
    //
    llfs::Status file_create_status = storage_context->add_new_file(
        storage_file_name, [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
          llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> config_4kb =
              builder.add_object(llfs::PageArenaConfigOptions{
                  .uuid = arena_uuid_4kb,
                  .page_allocator =
                      llfs::CreateNewPageAllocator{
                          .options =
                              llfs::PageAllocatorConfigOptions{
                                  .uuid = llfs::None,
                                  .max_attachments = 32,
                                  .page_count = llfs::PageCount{32},
                                  .log_device =
                                      llfs::CreateNewLogDeviceWithDefaultSize{
                                          .uuid = llfs::None,
                                          .pages_per_block_log2 =
                                              llfs::IoRingLogConfig::kDefaultPagesPerBlockLog2 + 1,
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
                                  .page_size_log2 = llfs::PageSizeLog2{12},
                              },
                      },
              });
          BATT_REQUIRE_OK(config_4kb);

          llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> config_2mb =
              builder.add_object(llfs::PageArenaConfigOptions{
                  .uuid = arena_uuid_2mb,
                  .page_allocator =
                      llfs::CreateNewPageAllocator{
                          .options =
                              llfs::PageAllocatorConfigOptions{
                                  .uuid = llfs::None,
                                  .max_attachments = 32,
                                  .page_count = llfs::PageCount{1},
                                  .log_device =
                                      llfs::CreateNewLogDeviceWithDefaultSize{
                                          .uuid = llfs::None,
                                          .pages_per_block_log2 =
                                              llfs::IoRingLogConfig::kDefaultPagesPerBlockLog2 + 1,
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
                                  .page_count = llfs::PageCount{1},
                                  .page_size_log2 = llfs::PageSizeLog2{21},
                              },
                      },
              });

          BATT_REQUIRE_OK(config_2mb);
          return llfs::OkStatus();
        });
    ASSERT_TRUE(file_create_status.ok()) << BATT_INSPECT(file_create_status);

    llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> cache =
        this->storage_context->get_page_cache();
    ASSERT_TRUE(cache.ok()) << BATT_INSPECT(cache.status());
    ASSERT_NE(*cache, nullptr);
    llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_4kb =
        (*cache)->devices_with_page_size(4 * kKiB);
    EXPECT_EQ(devices_4kb.size(), 1u);
    llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_2mb =
        (*cache)->devices_with_page_size(2 * kMiB);
    EXPECT_EQ(devices_2mb.size(), 1u);
  }

  batt::SharedPtr<llfs::StorageContext> storage_context;

  llfs::StatusOr<llfs::ScopedIoRing> io;

  const char* dir_name;
};

TEST_F(StorageContextTest, GetPageCache)
{
  // This test just runs SetUp()
  //
}

TEST_F(StorageContextTest, IncreasePageCacheStorage)
{
  const char* storage_file_name1 = "llfs_StorageContextTest_GetPageCache_storage_file2.llfs";
  const char* storage_file_name2 = "llfs_StorageContextTest_GetPageCache_storage_file3.llfs";

  u8 node_size_log2 = 12 /*4kb*/;
  u8 leaf_size_log2 = 21 /*2mb*/;

  llfs::delete_file(storage_file_name1).IgnoreError();
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{storage_file_name1}));
  llfs::delete_file(storage_file_name2).IgnoreError();
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{storage_file_name2}));

  BATT_CHECK_OK(this->storage_context->increase_storage_capacity(
      this->dir_name, 4096, llfs::PageSize{batt::checked_cast<u32>(u64{1} << leaf_size_log2)},
      llfs::PageSizeLog2{leaf_size_log2},
      llfs::PageSize{batt::checked_cast<u32>(u64{1} << node_size_log2)},
      llfs::PageSizeLog2{node_size_log2}, storage_file_name1));

  llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> cache = this->storage_context->get_page_cache();
  ASSERT_TRUE(cache.ok()) << BATT_INSPECT(cache.status());
  ASSERT_NE(*cache, nullptr);
  llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_4kb =
      (*cache)->devices_with_page_size(4 * kKiB);
  EXPECT_EQ(devices_4kb.size(), 2u);
  llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_2mb =
      (*cache)->devices_with_page_size(2 * kMiB);
  EXPECT_EQ(devices_2mb.size(), 2u);

  BATT_CHECK_OK(this->storage_context->increase_storage_capacity(
      this->dir_name, 4096, llfs::PageSize{batt::checked_cast<u32>(u64{1} << leaf_size_log2)},
      llfs::PageSizeLog2{leaf_size_log2},
      llfs::PageSize{batt::checked_cast<u32>(u64{1} << node_size_log2)},
      llfs::PageSizeLog2{node_size_log2}, storage_file_name2));

  ASSERT_TRUE(cache.ok()) << BATT_INSPECT(cache.status());
  ASSERT_NE(*cache, nullptr);
  devices_4kb = (*cache)->devices_with_page_size(4 * kKiB);
  EXPECT_EQ(devices_4kb.size(), 3u);
  devices_2mb = (*cache)->devices_with_page_size(2 * kMiB);
  EXPECT_EQ(devices_2mb.size(), 3u);

  llfs::PageCacheMetrics& metrics = (*cache)->metrics();
  LOG(INFO) << "metrics.get_page_view_count: " << metrics.get_page_view_count.load();

  // TODO: [Gabe Bornstein 6/18/24] Add other invariants to be verified here.
  //
}

TEST_F(StorageContextTest, RunOutOfMemory)
{
  std::string file_name = "llfs_StorageContextTest_RunOutOfMemory_storage_file";
  std::string file_extension = ".llfs";
  //   const char*  = file_name + ".llfs";
  //   const char* storage_file_name2 = "llfs_StorageContextTest_GetPageCache_storage_file5.llfs";

  u8 node_size_log2 = 12 /*4kb*/;
  u8 leaf_size_log2 = 21 /*2mb*/;
  u8 num_storage_increases = 100;
  u8 num_pages_per_device = 1;

  llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> cache = this->storage_context->get_page_cache();

  std::thread new_page_thread([&] {
    for (usize i = 0; i < num_pages_per_device * num_storage_increases; ++i) {
      LOG(INFO) << "new_page_thread start: " << i;
      std::unique_ptr<llfs::PageCacheJob> job1 = (*cache)->new_job();
      batt::StatusOr<std::shared_ptr<llfs::PageBuffer>> page1 =
          job1->new_page(llfs::PageSize{4096}, batt::WaitForResource::kFalse,
                         llfs::OpaquePageView::page_layout_id(), llfs::Caller::Unknown,
                         /*cancel_token=*/llfs::None);
      LOG(INFO) << "new_page_thread end: " << i;
    }
  });

  std::thread increase_storage_capacity_thread([&] {
    for (int i = 0; i < num_storage_increases + 1; ++i) {
      LOG(INFO) << "increase_storage_capacity_thread start: " << i;
      const char* storage_file_name = (file_name + batt::to_string(i) + file_extension).c_str();
      llfs::delete_file(storage_file_name).IgnoreError();
      EXPECT_FALSE(std::filesystem::exists(std::filesystem::path{storage_file_name}));

      BATT_CHECK_OK(this->storage_context->increase_storage_capacity(
          this->dir_name, 4096, llfs::PageSize{batt::checked_cast<u32>(u64{1} << leaf_size_log2)},
          llfs::PageSizeLog2{leaf_size_log2},
          llfs::PageSize{batt::checked_cast<u32>(u64{1} << node_size_log2)},
          llfs::PageSizeLog2{node_size_log2}, storage_file_name));

      ASSERT_TRUE(cache.ok()) << BATT_INSPECT(cache.status());
      ASSERT_NE(*cache, nullptr);
      llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_4kb =
          (*cache)->devices_with_page_size(4 * kKiB);
      EXPECT_EQ(devices_4kb.size(), i * 1u + 2u);
      llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_2mb =
          (*cache)->devices_with_page_size(2 * kMiB);
      EXPECT_EQ(devices_2mb.size(), i * 1u + 2u);
      LOG(INFO) << "increase_storage_capacity_thread end: " << i;
    }
  });

  llfs::PageCacheMetrics& metrics = (*cache)->metrics();
  LOG(INFO) << "metrics.get_page_view_count: " << metrics.get_page_view_count.load();

  new_page_thread.join();
  increase_storage_capacity_thread.join();
}

}  // namespace
