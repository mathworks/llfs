//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot.hpp>
//
#include <llfs/slot.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/stream_util.hpp>

namespace {

TEST(PackedSlotOffsetTest, GetSlotOffset)
{
  llfs::PackedSlotOffset i = 12345ull;
  EXPECT_EQ(llfs::get_slot_offset(i), 12345ull);
}

TEST(SlotWithPayloadTest, ToString)
{
  llfs::SlotWithPayload<int> obj{
      .slot_range =
          llfs::SlotRange{
              .lower_bound = 0,
              .upper_bound = 100,
          },
      .payload = -88,
  };
  EXPECT_THAT(batt::to_string(obj), ::testing::StrEq("{.slot_range=[0,100), .payload=-88,}"));
}

TEST(SlotRangeSpecTest, ToString)
{
  EXPECT_THAT(batt::to_string(llfs::SlotRangeSpec{batt::None, batt::None}),
              ::testing::StrEq("SlotRangeSpec{.lower_bound=--, .upper_bound=--,}"));

  EXPECT_THAT(batt::to_string(llfs::SlotRangeSpec{batt::None, 200}),
              ::testing::StrEq("SlotRangeSpec{.lower_bound=--, .upper_bound=200,}"));

  EXPECT_THAT(batt::to_string(llfs::SlotRangeSpec{100, batt::None}),
              ::testing::StrEq("SlotRangeSpec{.lower_bound=100, .upper_bound=--,}"));

  EXPECT_THAT(batt::to_string(llfs::SlotRangeSpec{100, 200}),
              ::testing::StrEq("SlotRangeSpec{.lower_bound=100, .upper_bound=200,}"));
}

TEST(SlotClampDistanceTest, Test)
{
  llfs::slot_offset_type a = 0;
  llfs::slot_offset_type b = a - 100;
  llfs::slot_offset_type c = a + 100;

  EXPECT_EQ(llfs::slot_clamp_distance(a, a), 0);

  EXPECT_EQ(llfs::slot_clamp_distance(a, c), 100);
  EXPECT_EQ(llfs::slot_clamp_distance(b, c), 200);
  EXPECT_EQ(llfs::slot_clamp_distance(b, a), 100);

  EXPECT_EQ(llfs::slot_clamp_distance(a, b), 0);
  EXPECT_EQ(llfs::slot_clamp_distance(c, b), 0);
  EXPECT_EQ(llfs::slot_clamp_distance(c, a), 0);
}

TEST(SlotAbsDistanceTest, Test)
{
  llfs::slot_offset_type a = 0;
  llfs::slot_offset_type b = a - 100;
  llfs::slot_offset_type c = a + 100;

  EXPECT_EQ(llfs::slot_abs_distance(a, a), 0);

  EXPECT_EQ(llfs::slot_abs_distance(a, c), 100);
  EXPECT_EQ(llfs::slot_abs_distance(b, c), 200);
  EXPECT_EQ(llfs::slot_abs_distance(b, a), 100);

  EXPECT_EQ(llfs::slot_abs_distance(a, b), 100);
  EXPECT_EQ(llfs::slot_abs_distance(c, b), 200);
  EXPECT_EQ(llfs::slot_abs_distance(c, a), 100);
}

TEST(SlotDifferenceTest, Test)
{
  llfs::slot_offset_type a = 0;
  llfs::slot_offset_type b = a - 100;
  llfs::slot_offset_type c = a + 100;

  EXPECT_EQ(llfs::slot_difference(a, a), 0);

  EXPECT_EQ(llfs::slot_difference(a, c), 100);
  EXPECT_EQ(llfs::slot_difference(b, c), 200);
  EXPECT_EQ(llfs::slot_difference(b, a), 100);

  EXPECT_EQ(llfs::slot_difference(a, b), -100);
  EXPECT_EQ(llfs::slot_difference(c, b), -200);
  EXPECT_EQ(llfs::slot_difference(c, a), -100);
}

TEST(SlotCheckSlotTest, OkTests)
{
  llfs::slot_offset_type a = 0;
  llfs::slot_offset_type b = a - 100;
  llfs::slot_offset_type c = a + 100;

  LLFS_CHECK_SLOT_LT(b, a);
  LLFS_CHECK_SLOT_LT(a, c);
  LLFS_CHECK_SLOT_LT(b, c);

  LLFS_CHECK_SLOT_GT(a, b);
  LLFS_CHECK_SLOT_GT(c, a);
  LLFS_CHECK_SLOT_GT(c, b);

  LLFS_CHECK_SLOT_LE(a, a);
  LLFS_CHECK_SLOT_LE(b, a);
  LLFS_CHECK_SLOT_LE(a, c);
  LLFS_CHECK_SLOT_LE(b, c);

  LLFS_CHECK_SLOT_GE(a, a);
  LLFS_CHECK_SLOT_GE(a, b);
  LLFS_CHECK_SLOT_GE(c, a);
  LLFS_CHECK_SLOT_GE(c, b);

  EXPECT_EQ(LLFS_CHECKED_SLOT_DISTANCE(a, a), 0);
  EXPECT_EQ(LLFS_CHECKED_SLOT_DISTANCE(a, c), 100);
  EXPECT_EQ(LLFS_CHECKED_SLOT_DISTANCE(b, c), 200);
  EXPECT_EQ(LLFS_CHECKED_SLOT_DISTANCE(b, a), 100);
}

TEST(SlotCheckSlotTest, DeathTests)
{
  llfs::slot_offset_type a = 0;
  llfs::slot_offset_type b = a - 100;
  llfs::slot_offset_type c = a + 100;

  EXPECT_DEATH(LLFS_CHECK_SLOT_LT(a, a) << " A_lt_A", "a == 0 a == 0 A_lt_A");
  EXPECT_DEATH(LLFS_CHECK_SLOT_LT(a, b) << " A_lt_B", "a == 0 b == 18446744073709551516 A_lt_B");
  EXPECT_DEATH(LLFS_CHECK_SLOT_LT(c, a) << " C_lt_A", "c == 100 a == 0 C_lt_A");
  EXPECT_DEATH(LLFS_CHECK_SLOT_LT(c, b) << " C_lt_B", "c == 100 b == 18446744073709551516 C_lt_B");

  EXPECT_DEATH(LLFS_CHECK_SLOT_GT(a, a) << " A_gt_A", "a == 0 a == 0 A_gt_A");
  EXPECT_DEATH(LLFS_CHECK_SLOT_GT(b, a) << " B_gt_A", "a == 0 b == 18446744073709551516 B_gt_A");
  EXPECT_DEATH(LLFS_CHECK_SLOT_GT(a, c) << " A_gt_C", "c == 100 a == 0 A_gt_C");
  EXPECT_DEATH(LLFS_CHECK_SLOT_GT(b, c) << " B_gt_C", "c == 100 b == 18446744073709551516 B_gt_C");

  EXPECT_DEATH(LLFS_CHECK_SLOT_LE(a, b) << " A_le_B", "b == 18446744073709551516 a == 0 A_le_B");
  EXPECT_DEATH(LLFS_CHECK_SLOT_LE(c, a) << " C_le_A", "a == 0 c == 100 C_le_A");
  EXPECT_DEATH(LLFS_CHECK_SLOT_LE(c, b) << " C_le_B", "b == 18446744073709551516 c == 100 C_le_B");

  EXPECT_DEATH(LLFS_CHECK_SLOT_GE(b, a) << " B_ge_A", "b == 18446744073709551516 a == 0 B_ge_A");
  EXPECT_DEATH(LLFS_CHECK_SLOT_GE(a, c) << " A_ge_C", "a == 0 c == 100 A_ge_C");
  EXPECT_DEATH(LLFS_CHECK_SLOT_GE(b, c) << " B_ge_C", "b == 18446744073709551516 c == 100 B_ge_C");

  EXPECT_DEATH(LLFS_CHECKED_SLOT_DISTANCE(c, a), "c == 100 a == 0");
  EXPECT_DEATH(LLFS_CHECKED_SLOT_DISTANCE(c, b), "c == 100 b == 18446744073709551516");
  EXPECT_DEATH(LLFS_CHECKED_SLOT_DISTANCE(a, b), "a == 0 b == 18446744073709551516");
}

}  // namespace
