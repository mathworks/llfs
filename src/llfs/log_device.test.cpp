//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/log_device.hpp>
//
#include <llfs/log_device.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/stream_util.hpp>

namespace {

TEST(LogDeviceTest, LogReadModeOstream)
{
  EXPECT_THAT(batt::to_string(llfs::LogReadMode::kSpeculative), ::testing::StrEq("Speculative"));
  EXPECT_THAT(batt::to_string(llfs::LogReadMode::kInconsistent), ::testing::StrEq("Inconsistent"));
  EXPECT_THAT(batt::to_string(llfs::LogReadMode::kDurable), ::testing::StrEq("Durable"));
  EXPECT_THAT(batt::to_string((llfs::LogReadMode)12385746),
              ::testing::StrEq("(bad value:12385746)"));
}

}  // namespace
