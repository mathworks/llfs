//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/track_fds.hpp>
//
#include <llfs/track_fds.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/logging.hpp>

#include <batteries/stream_util.hpp>

namespace {

TEST(TrackFdsTest, Test)
{
  llfs::StatusOr<std::set<int>> fds = llfs::get_open_fds();
  ASSERT_TRUE(fds.ok()) << BATT_INSPECT(fds.status());

  EXPECT_GE(fds->size(), 3u);
  EXPECT_EQ(fds->count(0), 1u);
  EXPECT_EQ(fds->count(1), 1u);
  EXPECT_EQ(fds->count(2), 1u);

  LLFS_LOG_INFO() << batt::dump_range(*fds);
}

}  // namespace
