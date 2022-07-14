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

#include <thread>

namespace {

using namespace llfs::constants;
using namespace llfs::int_types;

TEST(StorageContextTest, GetPageCache)
{
  const char* storage_file_name = "/tmp/llfs_StorageContextTest_GetPageCache_storage_file.llfs";

  llfs::StatusOr<llfs::ScopedIoRing> io =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{64}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  boost::uuids::uuid arena_uuid_4kb = llfs::random_uuid();
  boost::uuids::uuid arena_uuid_2mb = llfs::random_uuid();

  // Create a storage file with two page arenas, one for small pages (4kb), one for large (2mb).
  //
  {
    llfs::StatusOr<std::unique_ptr<llfs::IoRingRawBlockFile>> file = llfs::IoRingRawBlockFile::open(
        io->get(), storage_file_name, /*flags=*/O_RDWR | O_CREAT | O_DIRECT | O_SYNC,
        /*mode=*/0600);
    ASSERT_TRUE(file.ok()) << BATT_INSPECT(file.status());

    llfs::StorageFileBuilder builder{**file, /*base_offset=*/0};

    llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> config_4kb =
        builder.add_object(llfs::PageArenaConfigOptions{
            .uuid = arena_uuid_4kb,
            .page_allocator =
                llfs::PageAllocatorConfigOptions{
                    .uuid = llfs::None,
                    .max_attachments = 32,
                    .page_count = llfs::PageCount{32},
                    .log_device =
                        llfs::LogDeviceConfigOptions{
                            .uuid = llfs::None,
                            .pages_per_block_log2 = 1,
                            .log_size = 0,
                        },
                    .page_size_log2 = llfs::PageSizeLog2{12},
                    .page_device_id = llfs::None,
                },
            .page_device =
                llfs::PageDeviceConfigOptions{
                    .uuid = llfs::None,
                    .device_id = llfs::None,
                    .page_count = llfs::PageCount{32},
                    .page_size_log2 = llfs::PageSizeLog2{12},
                },
        });

    ASSERT_TRUE(config_4kb.ok()) << BATT_INSPECT(config_4kb.status());

    llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> config_2mb =
        builder.add_object(llfs::PageArenaConfigOptions{
            .uuid = arena_uuid_2mb,
            .page_allocator =
                llfs::PageAllocatorConfigOptions{
                    .uuid = llfs::None,
                    .max_attachments = 32,
                    .page_count = llfs::PageCount{32},
                    .log_device =
                        llfs::LogDeviceConfigOptions{
                            .uuid = llfs::None,
                            .pages_per_block_log2 = 1,
                            .log_size = 0,
                        },
                    .page_size_log2 = llfs::PageSizeLog2{21},
                    .page_device_id = llfs::None,
                },
            .page_device =
                llfs::PageDeviceConfigOptions{
                    .uuid = llfs::None,
                    .device_id = llfs::None,
                    .page_count = llfs::PageCount{32},
                    .page_size_log2 = llfs::PageSizeLog2{21},
                },
        });

    ASSERT_TRUE(config_2mb.ok()) << BATT_INSPECT(config_2mb.status());

    llfs::Status flush_status = builder.flush_all();
    ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
  }

  // Create a StorageContext and recover the page arenas.
  //
  batt::SharedPtr<llfs::StorageContext> storage_context = batt::make_shared<llfs::StorageContext>(
      batt::Runtime::instance().default_scheduler(), io->get());
  {
    llfs::Status status = storage_context->add_existing_named_file(storage_file_name);
    ASSERT_TRUE(status.ok()) << status;
  }

  llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> cache = storage_context->get_page_cache();
  ASSERT_TRUE(cache.ok()) << BATT_INSPECT(cache.status());
  ASSERT_NE(*cache, nullptr);

  llfs::Slice<const llfs::PageArena> arenas_4kb = (*cache)->arenas_for_page_size(4 * kKiB);
  EXPECT_EQ(arenas_4kb.size(), 1u);

  llfs::Slice<const llfs::PageArena> arenas_2mb = (*cache)->arenas_for_page_size(2 * kMiB);
  EXPECT_EQ(arenas_2mb.size(), 1u);
}

}  // namespace
