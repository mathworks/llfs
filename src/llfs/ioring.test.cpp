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

#include <llfs/filesystem.hpp>
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
#include <random>
#include <thread>
#include <vector>

namespace {

using llfs::ConstBuffer;
using llfs::IoRing;
using llfs::kDirectIOBlockAlign;
using llfs::kDirectIOBlockSize;
using llfs::MutableBuffer;
using llfs::RingBuffer;
using llfs::ScopedIoRing;
using llfs::Status;
using llfs::StatusOr;

using batt::Watch;

using namespace llfs::int_types;
using namespace llfs::constants;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

const std::string kFileBanner =
    "//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++\n"
    "//\n"
    "// Part of the LLFS Project, under Apache License v2.0.\n"
    "// See https://www.apache.org/licenses/LICENSE-2.0 for license information.\n"
    "// SPDX short identifier: Apache-2.0\n"
    "//\n"
    "//+++++++++++-+-+--+----- --- -- -  -  -   -\n";

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingTest, Test)
{
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << io.status();

  int fd = llfs::system_open3("/tmp/llfs_ioring_test_file", O_CREAT | O_RDWR | O_DIRECT | O_SYNC,
                              /*mode=*/0644);
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

  for (u64 offset = 0; offset < 1 * kMiB; offset += kDirectIOBlockSize * 100) {
    if (offset % (100 * 10 * kMiB) == 0) {
      LLFS_LOG_INFO() << BATT_INSPECT(offset);
    }
    bool ok = false;
    f.async_write_some(offset, std::array{ConstBuffer{data, kDirectIOBlockSize}},
                       /*handler=*/[&](StatusOr<i32> result) {
                         if (!result.ok()) {
                           LLFS_LOG_INFO() << "write failed: " << result.status();
                           return;
                         }
                         EXPECT_EQ(*result, kDirectIOBlockSize);

                         std::memset(data, 'x', kDirectIOBlockSize);

                         f.async_read_some(
                             offset, std::array{MutableBuffer{data, kDirectIOBlockSize}},
                             /*handler=*/[&](StatusOr<i32> result) {
                               ok = result.ok();

                               EXPECT_EQ(*result, kDirectIOBlockSize);
                               EXPECT_THAT(std::string(data, str.size()), ::testing::StrEq(str));
                             });
                       });

    Status io_status = io->run();

    EXPECT_TRUE(ok);
    EXPECT_TRUE(io_status.ok());

    io->reset();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingTest, DISABLED_BlockDev)
{
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << io.status();

  int fd = llfs::system_open2("/dev/nvme3n1", O_RDWR | O_DIRECT | O_SYNC);
  int fd2 = llfs::system_open2("/dev/nvme3n1", O_RDWR | O_DIRECT | O_SYNC);

  ASSERT_GE(fd, 0) << std::strerror(errno);

  LLFS_LOG_INFO() << BATT_INSPECT(fd) << BATT_INSPECT(fd2);
  close(fd2);

  using llfs::PageBuffer;

  auto page = PageBuffer::allocate(llfs::PageSize{2 * kMiB}, llfs::PageId{});
  MutableBuffer buf = get_mutable_buffer(page);
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingTest, MultipleThreads)
{
  StatusOr<ScopedIoRing> scoped_io_ring =
      ScopedIoRing::make_new(llfs::MaxQueueDepth{64}, llfs::ThreadPoolSize{8});

  ASSERT_TRUE(scoped_io_ring.ok()) << BATT_INSPECT(scoped_io_ring.status());
  EXPECT_TRUE(*scoped_io_ring);

  const auto file_path = "/tmp/llfs_ioring_test_file";

  int fd = llfs::system_open3(file_path, O_CREAT | O_RDWR, /*mode=*/0644);
  ASSERT_GE(fd, 0) << std::strerror(errno);

  IoRing::File f{scoped_io_ring->get_io_ring(), fd};

  f.set_raw_io(false);

  std::string message = "Hello, World.";

  Status write_status = f.write_all(/*offset=*/0, ConstBuffer{message.data(), message.size()});

  ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);

  std::array<char, kDirectIOBlockSize> buffer;

  Status read_status = f.read_all(/*offset=*/0, MutableBuffer{buffer.data(), message.size()});

  ASSERT_TRUE(read_status.ok()) << BATT_INSPECT(read_status);
  EXPECT_THAT((std::string_view{buffer.data(), message.size()}), ::testing::StrEq(message));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingTest, StartStopWorkCount)
{
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  io->on_work_started();

  llfs::Status status;

  std::thread helper_thread{[&io, &status] {
    status = io->run();
  }};

  io->on_work_finished();

  helper_thread.join();

  EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingTest, EnqueueManyHandlers)
{
  constexpr usize kNumThreads = 3;
  constexpr usize kNumHandlers = 50;
  constexpr usize kNumIterations = 5000;

  for (usize n = 0; n < kNumIterations; ++n) {
    StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
    ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

    io->on_work_started();

    std::array<llfs::Status, kNumThreads> status;
    status.fill(batt::StatusCode::kUnknown);

    std::atomic<bool> begin{false};
    std::atomic<i32> counter{0};

    std::vector<std::thread> helper_threads;
    for (usize i = 0; i < kNumThreads; ++i) {
      helper_threads.emplace_back([&io, &begin, &status, i] {
        while (!begin) {
          std::this_thread::yield();
        }
        status[i] = io->run();
      });
    }

    for (usize i = 0; i < kNumHandlers; ++i) {
      io->post([&counter](llfs::StatusOr<i32>) {
        counter++;
      });
    }

    begin = true;

    io->on_work_finished();

    {
      usize i = 0;
      for (std::thread& t : helper_threads) {
        t.join();
        EXPECT_TRUE(status[i].ok()) << BATT_INSPECT(status[i]) << BATT_INSPECT(i);
        ++i;
      };
    }

    EXPECT_EQ(counter, kNumHandlers);
  }
}

#ifdef BATT_PLATFORM_IS_LINUX
//
// Only compile/run this test on Linux because of the specific errno value it assumes (EBADF).

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingTest, ErrorRetval)
{
  const int kBadFileDescriptor = 99999;

  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  IoRing::File file{*io, kBadFileDescriptor};

  std::aligned_storage_t<kDirectIOBlockSize, kDirectIOBlockAlign> buffer;

  llfs::Status run_status = batt::StatusCode::kUnknown;

  io->on_work_started();

  std::thread helper_thread{[&io, &run_status] {
    run_status = io->run();
  }};

  // Reading should fail with 'Bad file descriptor' EBADF
  //
  llfs::Status read_status =
      file.read_all(/*offset=*/0, llfs::MutableBuffer{&buffer, sizeof(buffer)});

  io->on_work_finished();

  helper_thread.join();

  EXPECT_EQ(read_status, batt::status_from_errno(EBADF));
  EXPECT_TRUE(run_status.ok()) << BATT_INSPECT(run_status);

  // Trying to register the fd with the IoRing context should also fail with the same status.
  //
  LLFS_LOG_INFO() << "Expect to see log message: \"register_fd failed!  rfd.status() == 4105:Bad "
                     "file descriptor\" (it is OK!)";
  llfs::Status register_status = file.register_fd();

  EXPECT_EQ(register_status, batt::status_from_errno(EBADF));
}

#endif  // BATT_PLATFORM_IS_LINUX

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Throw an exception out of an I/O completion handler and verify that the implementation catches it
// and does everything correctly.
//
TEST(IoRingTest, ThrowExceptionFromHandler)
{
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  LLFS_LOG_INFO() << "Expect to see log message: \"Uncaught exception\" (it is OK!)";

  io->post([](StatusOr<i32> /*ignored*/) {
    throw std::runtime_error("A test exception");
  });

  llfs::Status run_status = io->run();

  EXPECT_TRUE(run_status.ok()) << BATT_INSPECT(run_status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Re-register buffers, with and without update flag.
//
TEST(IoRingTest, ReRegisterBuffers)
{
  static constexpr usize kBufferSize = 4096;

  for (bool update : {false, true}) {
    auto buffer1 = std::make_unique<std::aligned_storage_t<kBufferSize, kDirectIOBlockAlign>>();
    auto buffer2 = std::make_unique<std::aligned_storage_t<kBufferSize, kDirectIOBlockAlign>>();

    StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
    ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

    io->on_work_started();

    std::thread helper_thread{[&io] {
      io->run().IgnoreError();
    }};

    auto on_scope_exit = batt::finally([&] {
      io->on_work_finished();
      helper_thread.join();
    });

    StatusOr<int> fd = llfs::open_file_read_only(__FILE__);
    ASSERT_TRUE(fd.ok()) << BATT_INSPECT(fd.status());

    IoRing::File file{*io, *fd};

    // Register buffers.
    //
    StatusOr<usize> buf_index_0 = io->register_buffers(
        batt::seq::single_item(batt::MutableBuffer{buffer1.get(), kBufferSize}) |
            batt::seq::boxed(),
        /*update=*/false);

    ASSERT_TRUE(buf_index_0.ok()) << BATT_INSPECT(buf_index_0.status());
    EXPECT_EQ(*buf_index_0, 0u);

    // Read using a registered buffer, verifying the data.
    {
      std::memset(buffer1.get(), 0, kBufferSize);

      Status read_status = file.read_all_fixed(
          /*offset=*/0, batt::MutableBuffer{buffer1.get(), kBufferSize}, /*buf_index=*/0);

      ASSERT_TRUE(read_status.ok()) << BATT_INSPECT(read_status);
      EXPECT_THAT((std::string_view{(const char*)buffer1.get(), kFileBanner.size()}),
                  ::testing::StrEq(kFileBanner));
    }

    // Re-Register buffers.
    //
    StatusOr<usize> buf_index_1 = io->register_buffers(
        batt::seq::single_item(batt::MutableBuffer{buffer2.get(), kBufferSize}) |
            batt::seq::boxed(),
        update);

    ASSERT_TRUE(buf_index_1.ok()) << BATT_INSPECT(buf_index_0.status());
    if (update) {
      EXPECT_EQ(*buf_index_1, 1u);
    } else {
      EXPECT_EQ(*buf_index_1, 0u);
    }

    // Read using a registered buffer, verifying the data.
    {
      std::memset(buffer2.get(), 0, kBufferSize);

      Status read_status = file.read_all_fixed(
          /*offset=*/0, batt::MutableBuffer{buffer2.get(), kBufferSize},
          /*buf_index=*/*buf_index_1);

      ASSERT_TRUE(read_status.ok()) << BATT_INSPECT(read_status) << BATT_INSPECT(update);
      EXPECT_THAT((std::string_view{(const char*)buffer2.get(), kFileBanner.size()}),
                  ::testing::StrEq(kFileBanner));
    }

    // If update is true, then re-test with the first buffer to make sure it is still valid.
    //
    if (update) {
      std::memset(buffer1.get(), 0, kBufferSize);

      Status read_status = file.read_all_fixed(
          /*offset=*/0, batt::MutableBuffer{buffer1.get(), kBufferSize},
          /*buf_index=*/0);

      ASSERT_TRUE(read_status.ok()) << BATT_INSPECT(read_status) << BATT_INSPECT(update);
      EXPECT_THAT((std::string_view{(const char*)buffer1.get(), kFileBanner.size()}),
                  ::testing::StrEq(kFileBanner));
    }
  }
}

#ifdef BATT_PLATFORM_IS_LINUX
//
// Only compile/run this test on Linux because of the specific errno value it assumes.

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Force an error while registering buffers.
//
TEST(IoRingTest, FailRegisterBuffers)
{
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  StatusOr<usize> buf_index = io->register_buffers(
      batt::seq::single_item(batt::MutableBuffer{(void*)~0ull, 65536}) | batt::seq::boxed(),
      /*update=*/false);

  EXPECT_EQ(buf_index.status(), batt::status_from_errno(EOVERFLOW));
}

#endif  // BATT_PLATFORM_IS_LINUX

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Test registering and unregistering fds with an IoRing.
//
// Plan:
//  - Create an IoRing::File for up to kMaxFiles of the `.cpp` files next to this one.
//  - Loop for kNumIterations:
//    - Randomly select a file and action (register, unregister, read)
//    - Perform the action and verify the result
//
TEST(IoRingTest, RegisterFd)
{
  static constexpr usize kMaxFiles = 10;
  static constexpr usize kNumIterations = 10 * 1000;

  std::filesystem::path this_file = std::filesystem::path{__FILE__};
  std::filesystem::path this_dir = this_file.parent_path();

  std::aligned_storage_t<kDirectIOBlockSize, kDirectIOBlockAlign> memory;
  batt::MutableBuffer buffer{&memory, sizeof(memory)};
  std::string_view buffer_str{(const char*)buffer.data(), kFileBanner.size()};

  // Enumerate all .cpp files in the same directory as this one.
  //
  std::vector<std::string> source_files;
  for (const auto& dirent : std::filesystem::directory_iterator{this_dir}) {
    if (dirent.path().extension() == ".cpp" && dirent.file_size() >= buffer.size()) {
      source_files.emplace_back(dirent.path().string());
    }
  }

  // Make sure the number of open files doesn't get too large.
  //
  if (source_files.size() > kMaxFiles) {
    source_files.resize(kMaxFiles);
  }

  // Create an IoRing and a background helper thread.
  //
  StatusOr<IoRing> io = IoRing::make_new(llfs::MaxQueueDepth{64});
  ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

  io->on_work_started();

  std::thread helper_thread{[&io] {
    io->run().IgnoreError();
  }};

  auto on_scope_exit = batt::finally([&] {
    io->on_work_finished();
    helper_thread.join();
  });

  // Open all files.
  //
  std::vector<IoRing::File> files;

  for (const std::string& path_str : source_files) {
    StatusOr<int> fd = llfs::open_file_read_only(path_str);
    ASSERT_TRUE(fd.ok()) << fd.status();

    files.emplace_back(IoRing::File{*io, *fd});
  }

  // Set up the randomized test case generator.
  //
  enum PossibleActions : usize {
    kRegisterFile = 0,
    kUnregisterFile,
    kReadFile,
    kNumActions,
  };

  std::string_view action_names[kNumActions] = {
      "kRegisterFile",
      "kUnregisterFile",
      "kReadFile",
  };

  std::default_random_engine rng{1};
  for (usize i = 0; i < sizeof(usize) * 8; ++i) {
    (void)rng();
  }

  std::uniform_int_distribution<usize> pick_action{0u, kNumActions - 1};
  std::uniform_int_distribution<usize> pick_file{0, files.size() - 1};

  // On each iteration, randomly pick a file and action, perform it.
  //
  for (usize i = 0; i < kNumIterations; ++i) {
    const usize file_i = pick_file(rng);
    const usize action_i = pick_action(rng);
    const std::string& file_name = source_files[file_i];
    const std::string_view action = action_names[action_i];

    LLFS_VLOG(1) << BATT_INSPECT(file_name) << BATT_INSPECT(action);

    switch (action_i) {
      case kRegisterFile: {
        Status status = files[file_i].register_fd();
        ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
        break;
      }
      case kUnregisterFile: {
        Status status = files[file_i].unregister_fd();
        ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
        break;
      }
      case kReadFile: {
        std::memset(buffer.data(), 0, buffer.size());
        Status status = files[file_i].read_all(/*offset=*/0, buffer);
        ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
        EXPECT_THAT(buffer_str, ::testing::StrEq(kFileBanner));
        break;
      }
      default:
        BATT_PANIC() << "Invalid action chosen!";
        BATT_UNREACHABLE();
    }
  }
}

}  // namespace

#endif  // LLFS_DISABLE_IO_URING
