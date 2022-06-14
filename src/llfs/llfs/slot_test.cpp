#include <llfs/slot.hpp>
//
#include <llfs/slot.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

TEST(SlotTest, SlotLessThan)
{
  llfs::slot_offset_type a = 0, b = 0;

  EXPECT_FALSE(llfs::slot_less_than(a, b));
  EXPECT_FALSE(llfs::slot_less_than(b, a));

  EXPECT_TRUE(llfs::slot_less_than(a, b + 1));
  EXPECT_FALSE(llfs::slot_less_than(b + 1, a));

  EXPECT_TRUE(llfs::slot_less_than(a - 1, b));
  EXPECT_FALSE(llfs::slot_less_than(b, a - 1));
}

TEST(SlotTest, SlotRelativeMinMax)
{
  llfs::slot_offset_type initial = 0x010dcba987654321ull;
  //                                 +...+...+...+...

  for (llfs::slot_offset_type slot = initial; slot >= initial; slot += initial) {
    EXPECT_TRUE(llfs::slot_less_than(llfs::slot_relative_min(slot), slot));
    EXPECT_TRUE(llfs::slot_less_than(slot, llfs::slot_relative_max(slot)));
    EXPECT_FALSE(llfs::slot_less_than(llfs::slot_relative_min(slot) - 2, slot));
    EXPECT_FALSE(llfs::slot_less_than(slot, llfs::slot_relative_max(slot) + 2));
  }
}

}  // namespace
