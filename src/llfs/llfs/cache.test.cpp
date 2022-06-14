#include <llfs/cache.hpp>
//
#include <llfs/cache.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using llfs::Cache;
using llfs::CacheSlotRef;
using llfs::PinnedCacheSlot;

TEST(CacheTest, Basic)
{
  auto p_c = Cache<int, std::string>::make_new(4, "Test");
  auto& c = *p_c;

  auto slot1 = c.find_or_insert(1, [] {
    return std::make_shared<std::string>("foo");
  });

  ASSERT_TRUE(slot1.ok());
  EXPECT_THAT(**slot1, ::testing::StrEq("foo"));

  auto also_slot1 = c.find_or_insert(1, [] {
    BATT_PANIC() << "key 1 is already in cache";
    return std::make_shared<std::string>("not foo");
  });

  ASSERT_TRUE(also_slot1.ok());
  EXPECT_THAT(**also_slot1, ::testing::StrEq("foo"));
  EXPECT_EQ(*slot1, *also_slot1);
  EXPECT_FALSE(*slot1 != *also_slot1);

  auto slot2 = c.find_or_insert(2, [] {
    return std::make_shared<std::string>("foo2");
  });

  ASSERT_TRUE(slot2.ok());
  EXPECT_EQ(slot2->ref_count(), 1u);
  EXPECT_EQ(slot2->pin_count(), 1u);

  auto slot3 = c.find_or_insert(3, [] {
    return std::make_shared<std::string>("foo3");
  });

  ASSERT_TRUE(slot3.ok());
  EXPECT_EQ(slot3->ref_count(), 1u);
  EXPECT_EQ(slot3->pin_count(), 1u);
  {
    CacheSlotRef<int, std::string> ref3 = *slot3;

    EXPECT_EQ(slot3->ref_count(), 2u);
    EXPECT_EQ(slot3->pin_count(), 1u);

    auto slot3_copy = *slot3;

    EXPECT_EQ(slot3->ref_count(), 2u);
    EXPECT_EQ(slot3->pin_count(), 2u);
  }

  auto slot4 = c.find_or_insert(4, [] {
    return std::make_shared<std::string>("foo4");
  });

  ASSERT_TRUE(slot4.ok());
  EXPECT_EQ(slot4->ref_count(), 1u);
  EXPECT_EQ(slot4->pin_count(), 1u);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Verify that we can't insert into a full cache when all slots are pinned.
  {
    auto no_slot = c.find_or_insert(5, [] {
      BATT_PANIC() << "cache should be full";
      return std::make_shared<std::string>("foo5");
    });

    EXPECT_FALSE(no_slot.ok());
  }

  CacheSlotRef<int, std::string> slot2_ref = *slot2;

  *slot2 = {};

  EXPECT_FALSE(*slot2);

  EXPECT_TRUE(slot2_ref);

  auto slot2_copy = slot2_ref.pin();

  EXPECT_TRUE(slot2_copy);
  EXPECT_THAT(*slot2_copy, ::testing::StrEq("foo2"));

  auto slot2_copy2 = std::move(slot2_copy);

  EXPECT_FALSE(slot2_copy);
  EXPECT_TRUE(slot2_copy2);

  auto slot2_copy3 = slot2_copy2;

  EXPECT_FALSE(slot2_copy);
  EXPECT_TRUE(slot2_copy2);
  EXPECT_TRUE(slot2_copy3);

  PinnedCacheSlot<int, std::string> slot2_copy4;
  slot2_copy4 = std::move(slot2_copy2);

  EXPECT_FALSE(slot2_copy);
  EXPECT_FALSE(slot2_copy2);
  EXPECT_TRUE(slot2_copy3);
  EXPECT_TRUE(slot2_copy4);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Test PinnedSlot copy assign to self.
  //
  slot2_copy4 = slot2_copy4;

  EXPECT_TRUE(slot2_copy4);
  EXPECT_THAT(*slot2_copy4, ::testing::StrEq("foo2"));

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Test PinnedSlot move assign to self.
  //
  slot2_copy4 = std::move(slot2_copy4);

  EXPECT_TRUE(slot2_copy4);
  EXPECT_THAT(*slot2_copy4, ::testing::StrEq("foo2"));

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Verify that nothing can be put into the cache until the last pin for some slot is released.
  {
    auto no_slot = c.find_or_insert(5, [] {
      BATT_PANIC() << "cache should be full";
      return std::make_shared<std::string>("foo5");
    });

    EXPECT_FALSE(no_slot.ok());
  }
  slot2_copy4 = {};
  {
    auto no_slot = c.find_or_insert(5, [] {
      BATT_PANIC() << "cache should be full";
      return std::make_shared<std::string>("foo5");
    });

    EXPECT_FALSE(no_slot.ok());
  }
  slot2_copy3 = {};

  auto slot5 = c.find_or_insert(5, [] {
    return std::make_shared<std::string>("foo5");
  });

  EXPECT_TRUE(slot5.ok());
  EXPECT_THAT(**slot5, ::testing::StrEq("foo5"));

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Verify that trying to pin 2 will fail because it was evicted to insert 5.
  {
    PinnedCacheSlot<int, std::string> no_slot2 = slot2_ref.pin();

    EXPECT_FALSE(no_slot2);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Replace all pins with refs.
  //
  CacheSlotRef<int, std::string> slot1_ref = std::move(*slot1);
  CacheSlotRef<int, std::string> slot3_ref = std::move(*slot3);
  CacheSlotRef<int, std::string> slot4_ref = std::move(*slot4);
  CacheSlotRef<int, std::string> slot5_ref = std::move(*slot5);

  EXPECT_FALSE(*slot1);
  EXPECT_FALSE(*slot2);
  EXPECT_FALSE(*slot3);
  EXPECT_FALSE(*slot4);
  EXPECT_FALSE(*slot5);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Verify that we can still pin all the cache slots when there is no eviction pressure.
  //
  {
    auto slot1_pin = slot1_ref.pin();
    EXPECT_TRUE(slot1_pin);
    EXPECT_THAT(*slot1_pin, ::testing::StrEq("foo"));
  }
  {
    auto slot3_pin = slot3_ref.pin();
    EXPECT_TRUE(slot3_pin);
    EXPECT_THAT(*slot3_pin, ::testing::StrEq("foo3"));
  }
  {
    auto slot4_pin = slot4_ref.pin();
    EXPECT_TRUE(slot4_pin);
    EXPECT_THAT(*slot4_pin, ::testing::StrEq("foo4"));
  }
  {
    auto slot5_pin = slot5_ref.pin();
    EXPECT_TRUE(slot5_pin);
    EXPECT_THAT(*slot5_pin, ::testing::StrEq("foo5"));
  }
}

}  // namespace
