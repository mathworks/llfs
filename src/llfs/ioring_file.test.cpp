//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_file.hpp>
//
#include <llfs/ioring_file.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/filesystem.hpp>
#include <llfs/track_fds.hpp>

namespace {

using namespace llfs::int_types;

using llfs::Status;
using llfs::StatusOr;

TEST(IoringFileTest, FdLeakTest)
{
  bool saved = llfs::set_fd_tracking_enabled(true);
  auto on_scope_exit = batt::finally([&] {
    llfs::set_fd_tracking_enabled(saved);
  });

  StatusOr<llfs::ScopedIoRing> scoped_io_ring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{32}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(scoped_io_ring.ok()) << BATT_INSPECT(scoped_io_ring.status());

  for (usize i = 0; i < 1000; ++i) {
    StatusOr<std::set<int>> before_fds = llfs::get_open_fds();
    ASSERT_TRUE(before_fds.ok()) << BATT_INSPECT(before_fds.status());
    {
      StatusOr<int> fd = llfs::open_file_read_only(__FILE__);
      ASSERT_TRUE(fd.ok()) << BATT_INSPECT(fd.status());
      {
        llfs::IoRing::File file{scoped_io_ring->get_io_ring(), *fd};

        Status register_status = file.register_fd();

        EXPECT_TRUE(register_status.ok()) << BATT_INSPECT(register_status);

        StatusOr<std::set<int>> after_fds = llfs::get_open_fds();

        ASSERT_TRUE(after_fds.ok()) << BATT_INSPECT(after_fds.status());
        EXPECT_NE(before_fds, after_fds)
            << BATT_INSPECT_RANGE(*before_fds) << BATT_INSPECT_RANGE(*after_fds);
      }
    }
    StatusOr<std::set<int>> after_fds = llfs::get_open_fds();

    ASSERT_TRUE(after_fds.ok()) << BATT_INSPECT(after_fds.status());
    EXPECT_EQ(before_fds, after_fds)
        << BATT_INSPECT_RANGE(*before_fds) << BATT_INSPECT_RANGE(*after_fds) << BATT_INSPECT(i);
  }
}

}  // namespace
