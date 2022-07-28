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

namespace {

TEST(PackedSlotOffsetTest, GetSlotOffset)
{
  llfs::PackedSlotOffset i = 12345ull;
  EXPECT_EQ(llfs::get_slot_offset(i), 12345ull);
}

}  // namespace
