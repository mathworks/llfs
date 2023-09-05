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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
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

        llfs::StatusOr<std::unique_ptr<llfs::IoRingBufferPool>> pool_status =
            llfs::IoRingBufferPool::make_new(io->get_io_ring(), llfs::BufferCount{pool_size},
                                             llfs::BufferSize{buffer_size});

        ASSERT_TRUE(pool_status.ok()) << BATT_INSPECT(pool_status.status());

        auto& pool = **pool_status;

        EXPECT_EQ(pool.in_use(), 0u);
        EXPECT_EQ(pool.available(), pool_size);

        LLFS_VLOG(1) << "allocating...";

        std::vector<llfs::IoRingBufferPool::Buffer> buffers;

        for (usize i = 0; i < pool_size; ++i) {
          llfs::StatusOr<llfs::IoRingBufferPool::Buffer> buf = [&] {
            if (i % 2 == 0) {
              return pool.await_allocate();
            }
            return pool.try_allocate();
          }();

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

        {
          llfs::StatusOr<llfs::IoRingBufferPool::Buffer> buf = pool.try_allocate();

          EXPECT_FALSE(buf.ok());
          EXPECT_EQ(buf.status(), batt::StatusCode::kResourceExhausted);
        }

        buffers.resize(1);

        EXPECT_EQ(pool.in_use(), 1u);
        EXPECT_EQ(pool.available(), pool_size - 1);

        {
          llfs::StatusOr<llfs::IoRingBufferPool::Buffer> buf = pool.try_allocate();

          EXPECT_TRUE(buf.ok()) << BATT_INSPECT(buf.status());
        }
      },
      "IoRingBufferPoolTest.Test",
  };

  ctx.run();

  task.join();
}

class MockBufferHandler
{
 public:
  MOCK_METHOD(void, invoke, (batt::StatusOr<llfs::IoRingBufferPool::Buffer>), ());

  void operator()(batt::StatusOr<llfs::IoRingBufferPool::Buffer> buffer)
  {
    this->invoke(buffer);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingBufferPoolTest, DeallocateUnblocksWaiter)
{
  batt::SimpleExecutionContext ctx;

  batt::Task task{
      ctx.get_executor(),
      [&] {
        const usize pool_size = 1;
        const usize buffer_size = llfs::IoRingBufferPool::kMemoryUnitSize;

        llfs::StatusOr<llfs::ScopedIoRing> io =
            llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{16}, llfs::ThreadPoolSize{1});

        ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

        LLFS_VLOG(1) << "making pool";

        llfs::StatusOr<std::unique_ptr<llfs::IoRingBufferPool>> pool_status =
            llfs::IoRingBufferPool::make_new(io->get_io_ring(), llfs::BufferCount{pool_size},
                                             llfs::BufferSize{buffer_size});

        ASSERT_TRUE(pool_status.ok()) << BATT_INSPECT(pool_status.status());

        auto& pool = **pool_status;

        EXPECT_EQ(pool.in_use(), 0u);
        EXPECT_EQ(pool.available(), 1u);

        llfs::StatusOr<llfs::IoRingBufferPool::Buffer> buf = pool.try_allocate();

        ASSERT_TRUE(buf.ok()) << BATT_INSPECT(buf.status());
        EXPECT_EQ(pool.in_use(), 1u);
        EXPECT_EQ(pool.available(), 0u);

        ::testing::StrictMock<MockBufferHandler> mock_handler;

        pool.async_allocate(std::ref(mock_handler));

        llfs::StatusOr<llfs::IoRingBufferPool::Buffer> buf2;

        EXPECT_CALL(mock_handler, invoke(::testing::_))  //
            .WillOnce(::testing::DoAll(::testing::SaveArg<0>(&buf2), ::testing::Return()));

        void* const buf_ptr = buf->data();

        ASSERT_NE(buf_ptr, nullptr);

        *buf = {};

        EXPECT_FALSE(buf->is_valid());
        ASSERT_TRUE(buf2.ok()) << BATT_INSPECT(buf2.status());
        EXPECT_EQ(buf2->data(), buf_ptr);
      },
      "IoRingBufferPoolTest.DeallocateUnblocksWaiter"};

  ctx.run();

  task.join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingBufferPoolTest, FailedInitialize)
{
  llfs::StatusOr<llfs::ScopedIoRing> io =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{16}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  LLFS_VLOG(1) << "making pool";

  llfs::StatusOr<std::unique_ptr<llfs::IoRingBufferPool>> pool_status =
      llfs::IoRingBufferPool::make_new(
          io->get_io_ring(), llfs::BufferCount{1},
          llfs::BufferSize{llfs::IoRingBufferPool::kMemoryUnitSize * 2});

  EXPECT_EQ(pool_status.status(), batt::StatusCode::kInvalidArgument);
}

#ifdef BATT_PLATFORM_IS_LINUX
//
// Only compile/run this test on Linux because of the specific errno value it assumes (EINVAL).

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingBufferPoolTest, EmptyPool)
{
  llfs::StatusOr<llfs::ScopedIoRing> io =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{16}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  LLFS_VLOG(1) << "making pool";

  llfs::StatusOr<std::unique_ptr<llfs::IoRingBufferPool>> pool_status =
      llfs::IoRingBufferPool::make_new(io->get_io_ring(), llfs::BufferCount{0},
                                       llfs::BufferSize{llfs::IoRingBufferPool::kMemoryUnitSize});

  EXPECT_EQ(pool_status.status(), batt::status_from_errno(EINVAL));
}

#endif  // BATT_PLATFORM_IS_LINUX

}  // namespace
