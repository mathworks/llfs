//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring.hpp>
//
#include <llfs/ioring.hpp>

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring_file.hpp>
#include <llfs/page_view.hpp>
#include <llfs/ring_buffer.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/stream_util.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <array>
#include <cstdlib>

namespace {

using llfs::ConstBuffer;
using llfs::IoRing;
using llfs::MutableBuffer;
using llfs::RingBuffer;
using llfs::ScopedIoRing;
using llfs::Status;
using llfs::StatusOr;

using batt::Watch;

using namespace llfs::int_types;
using namespace llfs::constants;

TEST(IoRingTest, Test)
{
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << io.status();

  int fd = open("/tmp/llfs_ioring_test_file", O_CREAT | O_RDWR | O_DIRECT | O_SYNC, /*mode=*/0644);
  ASSERT_GE(fd, 0) << std::strerror(errno);

  std::array<char, 1023> buf;
  std::intptr_t i = (std::intptr_t)buf.data();
  i = (i + 511) & ~std::intptr_t{511};

  std::ostringstream oss;
  oss << "hello, io_uring world!";

  char* data = (char*)i;
  std::string str = oss.str();
  std::memcpy(data, str.data(), str.size());

  IoRing::File f{*io, fd};

  for (u64 offset = 0; offset < 1 * kMiB; offset += 512 * 100) {
    if (offset % (100 * 10 * kMiB) == 0) {
      LLFS_LOG_INFO() << BATT_INSPECT(offset);
    }
    bool ok = false;
    f.async_write_some(
        offset, std::array{ConstBuffer{data, 512}}, /*handler=*/[&](StatusOr<i32> result) {
          if (!result.ok()) {
            LLFS_LOG_INFO() << "write failed: " << result.status();
            return;
          }
          EXPECT_EQ(*result, 512);

          std::memset(data, 'x', 512);

          f.async_read_some(offset, std::array{MutableBuffer{data, 512}},
                            /*handler=*/[&](StatusOr<i32> result) {
                              ok = result.ok();

                              EXPECT_EQ(*result, 512);
                              EXPECT_THAT(std::string(data, str.size()), ::testing::StrEq(str));
                            });
        });

    Status io_status = io->run();

    EXPECT_TRUE(ok);
    EXPECT_TRUE(io_status.ok());

    io->reset();
  }
}

TEST(IoRingTest, DISABLED_BlockDev)
{
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << io.status();

  int fd = open("/dev/nvme3n1", O_RDWR | O_DIRECT | O_SYNC);
  int fd2 = open("/dev/nvme3n1", O_RDWR | O_DIRECT | O_SYNC);

  ASSERT_GE(fd, 0) << std::strerror(errno);

  LLFS_LOG_INFO() << BATT_INSPECT(fd) << BATT_INSPECT(fd2);
  close(fd2);

  using llfs::PageBuffer;

  auto page = PageBuffer::allocate(llfs::PageSize{2 * kMiB});
  MutableBuffer buf = page->mutable_buffer();
  std::memset(buf.data(), '@', buf.size());

  io->on_work_started();
  std::thread t{[&] {
    BATT_CHECK(io->run().ok());
  }};

  IoRing::File f{*io, fd};

  LLFS_LOG_INFO() << "==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -";
  LLFS_LOG_INFO() << "start (uring)";
  ConstBuffer data = buf;
  Watch<i64> sem{0};
  for (u64 off = 0; off < 40 * kGiB;) {
    sem.set_value(0);
    f.async_write_some(off, data + (off % data.size()), [&](StatusOr<i32> result) {
      BATT_CHECK(result.ok());
      off += *result;
      if (off % 10000 * kMiB == 0) {
        LLFS_LOG_INFO() << BATT_INSPECT(off);
      }
      sem.set_value(1);
    });
    BATT_CHECK(sem.await_equal(1).ok());
  }
  LLFS_LOG_INFO() << "done (uring)";

  t.detach();
}

TEST(IoRingTest, MultipleThreads)
{
  StatusOr<ScopedIoRing> scoped_io_ring =
      ScopedIoRing::make_new(llfs::MaxQueueDepth{64}, llfs::ThreadPoolSize{8});

  const auto file_path = "/tmp/llfs_ioring_test_file";

  int fd = open(file_path, O_CREAT | O_RDWR, /*mode=*/0644);
  ASSERT_GE(fd, 0) << std::strerror(errno);

  IoRing::File f{scoped_io_ring->get_io_ring(), fd};

  f.raw_io_ = false;

  std::string message = "Hello, World.";

  Status write_status = f.write_all(/*offset=*/0, ConstBuffer{message.data(), message.size()});

  ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);

  std::array<char, 512> buffer;

  Status read_status = f.read_all(/*offset=*/0, MutableBuffer{buffer.data(), message.size()});

  ASSERT_TRUE(read_status.ok()) << BATT_INSPECT(read_status);
  EXPECT_THAT((std::string_view{buffer.data(), message.size()}), ::testing::StrEq(message));
}

}  // namespace

#endif  // LLFS_DISABLE_IO_URING
