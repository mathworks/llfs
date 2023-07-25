//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_buffer_pool.hpp>
//
#include <llfs/ioring_buffer_pool.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/constants.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/ioring_file.hpp>

#include <batteries/async/simple_executor.hpp>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

// clang-format off
const std::string_view kExpectHeader = R"cpp(
//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_buffer_pool.hpp>
//
#include <llfs/ioring_buffer_pool.hpp>
)cpp";
// clang-format on

TEST(IoRingBufferPoolTest, Test)
{
  batt::SimpleExecutionContext ctx;

  LLFS_VLOG(1) << "making task";

  batt::Task task{
      ctx.get_executor(),
      [&] {
        LLFS_VLOG(1) << "making io ring";

        const usize pool_size = 32;
        const usize buffer_size = 256 * kKiB;
        const usize buffers_per_unit = llfs::IoRingBufferPool::kMemoryUnitSize / buffer_size;

        llfs::StatusOr<llfs::ScopedIoRing> io =
            llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{16}, llfs::ThreadPoolSize{1});

        ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

        LLFS_VLOG(1) << "making pool";

        llfs::IoRingBufferPool pool{io->get_io_ring(), llfs::BufferCount{pool_size},
                                    llfs::BufferSize{buffer_size}};

        LLFS_VLOG(1) << "initializing pool";

        llfs::Status pool_init = pool.initialize();

        ASSERT_TRUE(pool_init.ok()) << BATT_INSPECT(pool_init);

        EXPECT_EQ(pool.in_use(), 0u);
        EXPECT_EQ(pool.available(), pool_size);

        LLFS_VLOG(1) << "allocating...";

        std::vector<llfs::IoRingBufferPool::Buffer> buffers;

        for (usize i = 0; i < pool_size; ++i) {
          llfs::StatusOr<llfs::IoRingBufferPool::Buffer> buf = pool.await_allocate();

          ASSERT_TRUE(buf.ok()) << BATT_INSPECT(buf.status());

          buffers.push_back(*buf);

          LLFS_VLOG(1) << "success!";

          EXPECT_EQ(pool.in_use(), i + 1);
          EXPECT_EQ(pool.available(), pool_size - (i + 1));

          EXPECT_TRUE(*buf);
          EXPECT_EQ(buf->size(), buffer_size);
          EXPECT_EQ(buf->index(), i32(i / buffers_per_unit));
          EXPECT_EQ(&(buf->pool()), &pool);

          llfs::StatusOr<int> fd = llfs::open_file_read_only(__FILE__);

          ASSERT_TRUE(fd.ok()) << BATT_INSPECT(fd.status());

          llfs::StatusOr<i64> file_size = llfs::sizeof_fd(*fd);

          ASSERT_TRUE(file_size.ok()) << BATT_INSPECT(file_size.status());

          usize n_to_read = std::min(buffers[i].size(), usize(*file_size));

          ASSERT_GT(n_to_read, kExpectHeader.size());

          LLFS_VLOG(1) << BATT_INSPECT(n_to_read);

          llfs::IoRing::File file{io->get_io_ring(), *fd};

          llfs::Status read_status = file.read_all_fixed(
              0, llfs::MutableBuffer{buffers[i].data(), n_to_read}, buffers[i].index());

          ASSERT_TRUE(read_status.ok()) << BATT_INSPECT(read_status);

          EXPECT_THAT((std::string_view{(const char*)buffers[i].data(), kExpectHeader.size() - 1}),
                      ::testing::StrEq(kExpectHeader.substr(1)));
        }

        buffers.resize(1);

        EXPECT_EQ(pool.in_use(), 1u);
        EXPECT_EQ(pool.available(), pool_size - 1);
      },
  };

  ctx.run();

  task.join();
}

}  // namespace
