//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_interval.hpp>
//
#include <llfs/packed_interval.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace llfs::int_types;

TEST(PackedIntervalTest, PackAndUnpack)
{
  batt::Interval<i32> a{-2, 7};

  const auto b = llfs::PackedInterval<little_i32>::from(a);

  EXPECT_TRUE((std::is_same_v<decltype(b), const llfs::PackedInterval<little_i32>>));
  EXPECT_TRUE((std::is_same_v<decltype(b)::lower_bound_type, little_i32>));
  EXPECT_TRUE((std::is_same_v<decltype(b)::upper_bound_type, little_i32>));
  EXPECT_EQ(a.lower_bound, b.lower_bound);
  EXPECT_EQ(a.upper_bound, b.upper_bound);

  const auto a2 = b.unpack();

  EXPECT_TRUE((std::is_same_v<decltype(a2), const batt::Interval<i32>>));
  EXPECT_EQ(a2, a);
}

}  // namespace
