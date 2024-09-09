//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/key.hpp>
//
#include <llfs/key.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

TEST(KeyTest, CompareKeys)
{
  EXPECT_EQ(llfs::compare_keys(llfs::KeyView{"a"}, llfs::KeyView{"a"}), 0);
  EXPECT_EQ(llfs::compare_keys(llfs::KeyView{"aa"}, llfs::KeyView{"a"}), 1);
  EXPECT_EQ(llfs::compare_keys(llfs::KeyView{"a"}, llfs::KeyView{"aa"}), -1);
  EXPECT_EQ(llfs::compare_keys(llfs::KeyView{"ab"}, llfs::KeyView{"aa"}), 1);
  EXPECT_EQ(llfs::compare_keys(llfs::KeyView{"aa"}, llfs::KeyView{"ab"}), -1);
}

}  // namespace
