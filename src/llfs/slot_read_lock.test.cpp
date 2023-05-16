//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_read_lock.hpp>
//
#include <llfs/slot_read_lock.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/slot_lock_manager.hpp>

namespace {

class MockSponsor : public llfs::SlotReadLock::Sponsor
{
 public:
  MOCK_METHOD(void, unlock_slots, (llfs::SlotReadLock * read_lock), (override));
  MOCK_METHOD(llfs::SlotReadLock, clone_lock, (const llfs::SlotReadLock* read_lock), (override));
};

TEST(SlotReadLock, Test)
{
  ::testing::StrictMock<MockSponsor> sponsor;

  llfs::SlotReadLock lock0{&sponsor, llfs::SlotRange{100, 200}, {}};

  EXPECT_TRUE(lock0);
  EXPECT_EQ(lock0.slot_range().lower_bound, 100u);
  EXPECT_EQ(lock0.slot_range().upper_bound, 200u);
  EXPECT_FALSE(lock0.is_upper_bound_updated());
  EXPECT_EQ(lock0.get_sponsor(), &sponsor);

  llfs::SlotReadLock lock1{&sponsor, llfs::SlotRange{105, 300}, {}};
  llfs::SlotReadLock lock2 = std::move(lock1);
  llfs::SlotReadLock lock3;

  lock3 = std::move(lock2);

  EXPECT_CALL(sponsor, unlock_slots(&lock0)).WillOnce(::testing::Return());

  lock0 = std::move(lock3);

  EXPECT_TRUE(lock0);
  EXPECT_EQ(lock0.slot_range().lower_bound, 105u);
  EXPECT_EQ(lock0.slot_range().upper_bound, 300u);
  EXPECT_FALSE(lock0.is_upper_bound_updated());
  EXPECT_EQ(lock0.get_sponsor(), &sponsor);

  EXPECT_FALSE(lock1);
  EXPECT_EQ(lock1.slot_range().lower_bound, 0u);
  EXPECT_EQ(lock1.slot_range().upper_bound, 0u);
  EXPECT_FALSE(lock1.is_upper_bound_updated());
  EXPECT_EQ(lock1.get_sponsor(), nullptr);

  EXPECT_FALSE(lock2);
  EXPECT_EQ(lock2.slot_range().lower_bound, 0u);
  EXPECT_EQ(lock2.slot_range().upper_bound, 0u);
  EXPECT_FALSE(lock2.is_upper_bound_updated());
  EXPECT_EQ(lock2.get_sponsor(), nullptr);

  EXPECT_FALSE(lock3);
  EXPECT_EQ(lock3.slot_range().lower_bound, 0u);
  EXPECT_EQ(lock3.slot_range().upper_bound, 0u);
  EXPECT_FALSE(lock3.is_upper_bound_updated());
  EXPECT_EQ(lock3.get_sponsor(), nullptr);

  lock2 = std::move(lock0);

  EXPECT_FALSE(lock0);
  EXPECT_EQ(lock0.slot_range().lower_bound, 0u);
  EXPECT_EQ(lock0.slot_range().upper_bound, 0u);
  EXPECT_FALSE(lock0.is_upper_bound_updated());
  EXPECT_EQ(lock0.get_sponsor(), nullptr);

  EXPECT_TRUE(lock2);
  EXPECT_EQ(lock2.slot_range().lower_bound, 105u);
  EXPECT_EQ(lock2.slot_range().upper_bound, 300u);
  EXPECT_FALSE(lock2.is_upper_bound_updated());
  EXPECT_EQ(lock2.get_sponsor(), &sponsor);

  EXPECT_FALSE(lock1);
  EXPECT_EQ(lock1.slot_range().lower_bound, 0u);
  EXPECT_EQ(lock1.slot_range().upper_bound, 0u);
  EXPECT_FALSE(lock1.is_upper_bound_updated());
  EXPECT_EQ(lock1.get_sponsor(), nullptr);

  EXPECT_FALSE(lock3);
  EXPECT_EQ(lock3.slot_range().lower_bound, 0u);
  EXPECT_EQ(lock3.slot_range().upper_bound, 0u);
  EXPECT_FALSE(lock3.is_upper_bound_updated());
  EXPECT_EQ(lock3.get_sponsor(), nullptr);

  EXPECT_CALL(sponsor, unlock_slots(&lock2)).WillOnce(::testing::Return());
}

TEST(SlotReadLock, Issue50)
{
  llfs::SlotLockManager mgr;
  llfs::SlotReadLock lock1 =
      BATT_OK_RESULT_OR_PANIC(mgr.lock_slots(llfs::SlotRange{1, 100}, "test"));

  EXPECT_TRUE(lock1);

  lock1 = BATT_OK_RESULT_OR_PANIC(mgr.lock_slots(llfs::SlotRange{1, 100}, "test2"));

  EXPECT_TRUE(lock1);
}

}  // namespace
