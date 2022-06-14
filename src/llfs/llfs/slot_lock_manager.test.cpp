#include <llfs/slot_lock_manager.hpp>
//
#include <llfs/slot_lock_manager.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using ::testing::Eq;

TEST(SlotLockManagerTest, AllInputs)
{
  llfs::SlotLockManager mgr;

  EXPECT_THAT(mgr.get_lower_bound(), Eq(0u));
}

}  // namespace
