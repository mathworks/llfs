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

}  // namespace
