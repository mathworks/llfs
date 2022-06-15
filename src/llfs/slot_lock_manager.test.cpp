//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

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
