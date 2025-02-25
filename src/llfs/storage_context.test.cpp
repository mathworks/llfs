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
                                .page_count = llfs::PageCount{32},
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
                                .page_count = llfs::PageCount{32},
                                .page_size_log2 = llfs::PageSizeLog2{21},
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

  llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_4kb =
      (*cache)->devices_with_page_size(4 * kKiB);
  EXPECT_EQ(devices_4kb.size(), 1u);

  llfs::Slice<std::shared_ptr<const llfs::PageCache::PageDeviceEntry>> devices_2mb =
      (*cache)->devices_with_page_size(2 * kMiB);
  EXPECT_EQ(devices_2mb.size(), 1u);
}

}  // namespace
