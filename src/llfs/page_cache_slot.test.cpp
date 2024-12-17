//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_slot.hpp>
//
#include <llfs/page_cache_slot.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/utility.hpp>

namespace {

// Test Plan:
//
//  1. Create slots with different index values in a Pool; verify index()
//     - also verify initial is_valid state
//  2. ref_count test - add_ref/remove_ref should affect the ref_count, and also the use count of
//     the pool but only in the case of add: 0 -> 1 and remove: 1 -> 0.
//  3. State transition test
//     a. Invalid --(clear)--> Valid + Cleared
//     b. Invalid --(fill)--> Valid + Filled
//     c. Valid + Cleared --(evict)--> Invalid
//     d. Valid + Filled --(evict)--> Invalid
//     e. Valid + Filled --(acquire_pin)--> Valid + Filled + Pinned
//     f. Valid + Filled + Pinned --(acquire_pin)-- >Valid + Filled + Pinned
//     g. Valid + Filled + Pinned --(release_pin)-- >Valid + Filled + Pinned
//     h. Valid + Filled + Pinned --(release_pin)-- >Valid + Filled
//     i. Valid + Cleared --(acquire_pin)--> Valid + Cleared + Pinned
//     j. Valid + Cleared + Pinned --(release_pin)--> Valid + Cleared
//  4. extend_pin increases pin count
//     a. success if already > 0
//     b. panic otherwise
//  5. evict fails if pin count != 0
//  6. evict_if_key_equals
//     a. success
//     b. fail because pin count != 0
//     c. fail because key is wrong
//  7. fill fails when state is not Invalid:
//     a. Valid + Filled
//     b. Valid + Cleared
//     c. Valid + Filled + Pinned
//     d. Valid + Cleared + Pinned
//  8. update_latest_use
//  9. set_obsolete_hint
//  10. concurrent ref and pin count testing
//

using namespace llfs::int_types;

constexpr usize kNumTestSlots = 4;
const std::string kTestPoolName = "Test PageCacheSlot Pool";

class PageCacheSlotTest : public ::testing::Test
{
 public:
  boost::intrusive_ptr<llfs::PageCacheSlot::Pool> pool_ = llfs::PageCacheSlot::Pool::make_new(
      /*n_slots=*/kNumTestSlots, batt::make_copy(kTestPoolName));
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  1. Create slots with different index values in a Pool; verify index()
//     - also verify initial is_valid state
//
TEST_F(PageCacheSlotTest, CreateSlots)
{
  for (usize i = 0; i < kNumTestSlots; ++i) {
    llfs::PageCacheSlot* slot = this->pool_->allocate();

    ASSERT_NE(slot, nullptr);
    EXPECT_EQ(slot, this->pool_->get_slot(i));
    EXPECT_EQ(slot->index(), i);
    EXPECT_EQ(this->pool_->index_of(slot), i);
    EXPECT_FALSE(slot->is_valid());

    if (i == 0) {
      EXPECT_EQ(slot->value(), nullptr);
    }

    EXPECT_FALSE(slot->key().is_valid());
    EXPECT_EQ(slot->ref_count(), 0u);
    EXPECT_EQ(slot->pin_count(), 0u);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  2. ref_count test - add_ref/remove_ref should affect the ref_count, and also the use count of
//     the pool but only in the case of add: 0 -> 1 and remove: 1 -> 0.
//
TEST_F(PageCacheSlotTest, AddRemoveRefDeath)
{
  EXPECT_EQ(this->pool_->use_count(), 1u);

  llfs::PageCacheSlot* slot = this->pool_->allocate();

  EXPECT_EQ(slot->ref_count(), 0u);
  EXPECT_DEATH(slot->remove_ref(), "Assert.*failed:.*observed_count.*>.*0");

  slot->add_ref();

  EXPECT_EQ(slot->ref_count(), 1u);
  EXPECT_EQ(this->pool_->use_count(), 2u);

  slot->add_ref();
  slot->add_ref();

  EXPECT_EQ(slot->ref_count(), 3u);
  EXPECT_EQ(this->pool_->use_count(), 2u);

  slot->remove_ref();

  EXPECT_EQ(slot->ref_count(), 2u);
  EXPECT_EQ(this->pool_->use_count(), 2u);

  slot->remove_ref();
  slot->remove_ref();

  EXPECT_EQ(slot->ref_count(), 0u);
  EXPECT_EQ(this->pool_->use_count(), 1u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. State transition test
//     a. Invalid --(clear)--> Valid + Cleared
//     b. Invalid --(fill)--> Valid + Filled
//     c. Valid + Cleared --(evict)--> Invalid
//     d. Valid + Filled --(evict)--> Invalid
//     e. Valid + Filled --(acquire_pin)--> Valid + Filled + Pinned
//     f. Valid + Filled + Pinned --(acquire_pin)-- >Valid + Filled + Pinned
//     g. Valid + Filled + Pinned --(release_pin)-- >Valid + Filled + Pinned
//     h. Valid + Filled + Pinned --(release_pin)-- >Valid + Filled
//     i. Valid + Cleared --(acquire_pin)--> Valid + Cleared + Pinned
//     j. Valid + Cleared + Pinned --(release_pin)--> Valid + Cleared
//
TEST_F(PageCacheSlotTest, StateTransitions)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();

  EXPECT_FALSE(slot->is_valid());

  //     a. Invalid --(clear)--> Valid + Cleared
  //
  slot->clear();
  EXPECT_TRUE(slot->is_valid());

  //     i. Valid + Cleared --(acquire_pin)--> Valid + Cleared + Pinned
  //
  {
    llfs::PageCacheSlot::PinnedRef valid_cleared_ref =
        slot->acquire_pin(llfs::PageId{}, /*ignore_key=*/true);
    EXPECT_EQ(valid_cleared_ref.value(), nullptr);
    EXPECT_EQ(slot->pin_count(), 1u);
    EXPECT_EQ(slot->ref_count(), 1u);
  }

  //     j. Valid + Cleared + Pinned --(release_pin)--> Valid + Cleared
  //
  EXPECT_EQ(slot->pin_count(), 0);
  EXPECT_EQ(slot->ref_count(), 0);

  //     c. Valid + Cleared --(evict)--> Invalid
  //
  EXPECT_TRUE(slot->evict());
  EXPECT_FALSE(slot->is_valid());

  //     b. Invalid --(fill)--> Valid + Filled
  //
  {
    llfs::PageCacheSlot::PinnedRef pinned_ref = slot->fill(llfs::PageId{1});

    EXPECT_TRUE(slot->is_valid());
    EXPECT_EQ(slot->key(), llfs::PageId{1});
    EXPECT_NE(slot->value(), nullptr);
    EXPECT_TRUE(pinned_ref);
    EXPECT_EQ(pinned_ref.slot(), slot);
    EXPECT_EQ(pinned_ref.key(), slot->key());
    EXPECT_EQ(pinned_ref.value(), slot->value());
    EXPECT_EQ(pinned_ref.get(), slot->value());
    EXPECT_EQ(pinned_ref.pin_count(), 1u);
    EXPECT_EQ(slot->pin_count(), 1u);
    EXPECT_EQ(pinned_ref.ref_count(), 1u);
    EXPECT_EQ(slot->ref_count(), 1u);

    //     f. Valid + Filled + Pinned --(acquire_pin)-- >Valid + Filled + Pinned
    //
    llfs::PageCacheSlot::PinnedRef ref2 = pinned_ref;

    EXPECT_TRUE(ref2);
    EXPECT_EQ(ref2.slot(), slot);
    EXPECT_EQ(pinned_ref.pin_count(), 2u);
    EXPECT_EQ(slot->pin_count(), 2u);
    EXPECT_EQ(pinned_ref.ref_count(), 1u);
    EXPECT_EQ(slot->ref_count(), 1u);

    llfs::PageCacheSlot::PinnedRef ref3;

    EXPECT_FALSE(ref3);

    ref3 = ref2;

    EXPECT_TRUE(ref3);
    EXPECT_EQ(ref3.slot(), slot);
    EXPECT_EQ(pinned_ref.pin_count(), 3u);
    EXPECT_EQ(slot->pin_count(), 3u);
    EXPECT_EQ(pinned_ref.ref_count(), 1u);
    EXPECT_EQ(slot->ref_count(), 1u);

    {
      llfs::PageCacheSlot::PinnedRef ref4 = std::move(ref2);

      EXPECT_FALSE(ref2);
      EXPECT_EQ(ref4.slot(), slot);
      EXPECT_TRUE(ref4);
      EXPECT_EQ(pinned_ref.pin_count(), 3u);
      EXPECT_EQ(slot->pin_count(), 3u);
      EXPECT_EQ(pinned_ref.ref_count(), 1u);
      EXPECT_EQ(slot->ref_count(), 1u);

      {
        llfs::PageCacheSlot::PinnedRef ref5;

        EXPECT_FALSE(ref5);

        ref5 = std::move(ref3);

        EXPECT_EQ(ref5.slot(), slot);
        EXPECT_FALSE(ref3);
        EXPECT_TRUE(ref5);
        EXPECT_EQ(pinned_ref.pin_count(), 3u);
        EXPECT_EQ(slot->pin_count(), 3u);
        EXPECT_EQ(pinned_ref.ref_count(), 1u);
        EXPECT_EQ(slot->ref_count(), 1u);
      }
      //
      //     g. Valid + Filled + Pinned --(release_pin)-- >Valid + Filled + Pinned

      EXPECT_EQ(pinned_ref.pin_count(), 2u);
      EXPECT_EQ(slot->pin_count(), 2u);
      EXPECT_EQ(pinned_ref.ref_count(), 1u);
      EXPECT_EQ(slot->ref_count(), 1u);
    }

    EXPECT_EQ(pinned_ref.pin_count(), 1u);
    EXPECT_EQ(slot->pin_count(), 1u);
    EXPECT_EQ(pinned_ref.ref_count(), 1u);
    EXPECT_EQ(slot->ref_count(), 1u);
  }
  //
  //     h. Valid + Filled + Pinned --(release_pin)-- >Valid + Filled

  //----- --- -- -  -  -   -

  //     e. Valid + Filled --(acquire_pin)--> Valid + Filled + Pinned
  //
  EXPECT_EQ(slot->pin_count(), 0u);
  EXPECT_EQ(slot->ref_count(), 0u);
  EXPECT_TRUE(slot->is_valid());
  {
    llfs::PageCacheSlot::PinnedRef pinned_ref =
        slot->acquire_pin(llfs::PageId{}, /*ignore_key=*/true);

    EXPECT_TRUE(pinned_ref);
    EXPECT_TRUE(slot->is_valid());
    EXPECT_EQ(slot->pin_count(), 1u);
  }
  EXPECT_EQ(slot->pin_count(), 0u);
  EXPECT_EQ(slot->ref_count(), 0u);
  EXPECT_TRUE(slot->is_valid());
  {
    llfs::PageCacheSlot::PinnedRef pinned_ref =
        slot->acquire_pin(llfs::PageId{1}, /*ignore_key=*/false);

    EXPECT_TRUE(pinned_ref);
    EXPECT_TRUE(slot->is_valid());
    EXPECT_EQ(slot->pin_count(), 1u);
  }
  EXPECT_EQ(slot->pin_count(), 0u);
  EXPECT_EQ(slot->ref_count(), 0u);
  EXPECT_TRUE(slot->is_valid());
  {
    // Try to acquire pin using the wrong PageId; expect to fail.
    //
    llfs::PageCacheSlot::PinnedRef pinned_ref =
        slot->acquire_pin(llfs::PageId{2}, /*ignore_key=*/false);

    EXPECT_FALSE(pinned_ref);
    EXPECT_TRUE(slot->is_valid());
    EXPECT_EQ(slot->pin_count(), 0u);
  }

  //     b. Invalid --(fill)--> Valid + Filled
  //
  EXPECT_TRUE(slot->evict());
  EXPECT_FALSE(slot->is_valid());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. extend_pin increases pin count
//     a. success if already > 0
//
TEST_F(PageCacheSlotTest, ExtendPinSuccess)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();

  llfs::PageCacheSlot::PinnedRef pinned_ref = slot->fill(llfs::PageId{1});

  EXPECT_EQ(slot->pin_count(), 1u);

  slot->extend_pin();

  EXPECT_EQ(slot->pin_count(), 2u);

  slot->release_pin();

  EXPECT_EQ(slot->pin_count(), 1u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. extend_pin increases pin count
//     b. panic otherwise
//
TEST_F(PageCacheSlotTest, ExtendPinDeath)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();

  EXPECT_DEATH(slot->extend_pin(), "Assert.*failed:.*is.*pinned");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  5. evict fails if pin count != 0
//
TEST_F(PageCacheSlotTest, EvictFailure)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();

  llfs::PageCacheSlot::PinnedRef pinned_ref = slot->fill(llfs::PageId{1});

  EXPECT_FALSE(slot->evict());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. evict_if_key_equals
//     a. success
//
TEST_F(PageCacheSlotTest, EvictIfKeyEqualsSuccess)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();
  {
    llfs::PageCacheSlot::PinnedRef pinned_ref = slot->fill(llfs::PageId{1});
  }

  EXPECT_TRUE(slot->evict_if_key_equals(llfs::PageId{1}));
  EXPECT_FALSE(slot->is_valid());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. evict_if_key_equals
//     b. fail because pin count != 0
//
TEST_F(PageCacheSlotTest, EvictIfKeyEqualsFailurePinned)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();
  {
    llfs::PageCacheSlot::PinnedRef pinned_ref = slot->fill(llfs::PageId{1});

    EXPECT_FALSE(slot->evict_if_key_equals(llfs::PageId{1}));
  }
  EXPECT_TRUE(slot->is_valid());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. evict_if_key_equals
//     c. fail because key is wrong
//
TEST_F(PageCacheSlotTest, EvictIfKeyEqualsFailureWrongKey)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();
  {
    llfs::PageCacheSlot::PinnedRef pinned_ref = slot->fill(llfs::PageId{1});
  }

  EXPECT_FALSE(slot->evict_if_key_equals(llfs::PageId{2}));
  EXPECT_TRUE(slot->is_valid());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  7. fill fails when state is not Invalid:
//     a. Valid + Filled
//     c. Valid + Filled + Pinned
//
TEST_F(PageCacheSlotTest, FillFailureAlreadyFilledDeath)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();
  {
    llfs::PageCacheSlot::PinnedRef pinned_ref = slot->fill(llfs::PageId{1});

    EXPECT_EQ(slot->pin_count(), 1u);
    EXPECT_TRUE(pinned_ref);
    EXPECT_DEATH(slot->fill(llfs::PageId{2}), "Assert.*fail.*is.*valid");
  }
  EXPECT_EQ(slot->pin_count(), 0u);
  EXPECT_DEATH(slot->fill(llfs::PageId{2}), "Assert.*fail.*is.*valid");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  7. fill fails when state is not Invalid:
//     b. Valid + Cleared
//     d. Valid + Cleared + Pinned
//
TEST_F(PageCacheSlotTest, FillFailureClearedDeath)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();

  EXPECT_FALSE(slot->is_valid());

  slot->clear();

  {
    llfs::PageCacheSlot::PinnedRef valid_cleared_ref =
        slot->acquire_pin(llfs::PageId{}, /*ignore_key=*/true);
    EXPECT_TRUE(slot->is_valid());
    EXPECT_DEATH(slot->fill(llfs::PageId{2}), "Assert.*fail.*is.*valid");
  }

  EXPECT_EQ(slot->pin_count(), 0u);
  EXPECT_TRUE(slot->is_valid());
  EXPECT_DEATH(slot->fill(llfs::PageId{2}), "Assert.*fail.*is.*valid");
  EXPECT_DEATH(slot->clear(), "Assert.*fail.*is.*valid");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  8. update_latest_use
//  9. set_obsolete_hint
//
TEST_F(PageCacheSlotTest, LatestUse)
{
  llfs::PageCacheSlot* slot = this->pool_->allocate();

  i64 t0 = slot->get_latest_use();
  slot->update_latest_use();
  i64 t1 = slot->get_latest_use();

  EXPECT_GT(t1 - t0, 0);

  slot->set_obsolete_hint();
  i64 t2 = slot->get_latest_use();

  EXPECT_LT(t2 - t1, 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  10. Concurrent ref and pin count testing
//
TEST_F(PageCacheSlotTest, RefCounting)
{
  // The slot will start off in an Invalid state with a pin count and ref count of 0.
  //
  llfs::PageCacheSlot* slot = this->pool_->allocate();
  EXPECT_FALSE(slot->is_valid());
  EXPECT_EQ(slot->pin_count(), 0);
  EXPECT_EQ(slot->ref_count(), 0);

  u64 numThreads = std::thread::hardware_concurrency();
  std::vector<std::thread> threads;
  std::atomic<bool> start{false};
  for (u64 i = 0; i < numThreads; ++i) {
    threads.emplace_back([&slot, &start, i]() {
      while (!start.load()) {
        continue;
      }

      // Split the workload: let half the threads perform acquire_pin calls and
      // let the other half perform evictions.
      //
      if (i % 2) {
        {
          llfs::PageCacheSlot::PinnedRef pinned =
              slot->acquire_pin(llfs::PageId{}, /*ignore_key=*/true);
        }
      } else {
        slot->evict_if_key_equals(llfs::PageId{1});
      }
    });
  }

  start.store(true);

  for (std::thread &t : threads) {
    t.join();
  }

  // By the end of this test, both the pin count and ref count should still be 0,
  // since evict_if_key_equals and acquire_pin/release_pin ensure that these values are
  // incremented and decremented symmetrically, accounting for new pinning and new unpinning.
  //
  EXPECT_EQ(slot->pin_count(), 0);
  EXPECT_EQ(slot->ref_count(), 0);
}

}  // namespace
