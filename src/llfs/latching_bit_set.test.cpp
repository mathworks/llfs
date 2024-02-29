//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/latching_bit_set.hpp>
//
#include <llfs/latching_bit_set.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

namespace {

using namespace llfs::int_types;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(LatchingBitSetTest, Size0)
{
  llfs::LatchingBitSet s{0};

  EXPECT_EQ(s.upper_bound(), 0u);
  EXPECT_EQ(s.first_missing(), 0u);
  EXPECT_TRUE(s.is_full());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(LatchingBitSetTest, Size1)
{
  llfs::LatchingBitSet s{1};

  EXPECT_EQ(s.upper_bound(), 1u);
  EXPECT_EQ(s.first_missing(), 0u);
  EXPECT_FALSE(s.is_full());

  EXPECT_DEATH(s.insert(1), ".*[Aa]ssert.*fail.*i.*<.*size.*");

  EXPECT_TRUE(s.insert(0));
  EXPECT_FALSE(s.insert(0));

  EXPECT_EQ(s.first_missing(), 1u);
  EXPECT_TRUE(s.is_full());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(LatchingBitSetTest, Size40)
{
  llfs::LatchingBitSet s{40};

  EXPECT_EQ(s.upper_bound(), 40u);
  EXPECT_EQ(s.first_missing(), 0u);
  EXPECT_FALSE(s.is_full());

  EXPECT_TRUE(s.insert(0));
  EXPECT_EQ(s.first_missing(), 1u);

  EXPECT_TRUE(s.insert(10));
  EXPECT_EQ(s.first_missing(), 1u);

  for (usize i = 1; i < 9; ++i) {
    for (usize j = 0; j < i; ++j) {
      EXPECT_TRUE(s.contains(j));
      EXPECT_FALSE(s.insert(j));
    }
    EXPECT_FALSE(s.contains(i));
    EXPECT_TRUE(s.insert(i));
    EXPECT_EQ(s.first_missing(), i + 1);
    EXPECT_FALSE(s.is_full());
  }

  EXPECT_TRUE(s.insert(9));
  EXPECT_EQ(s.first_missing(), 11u);
  EXPECT_FALSE(s.is_full());

  for (usize i = 11; i < 39; ++i) {
    for (usize j = 0; j < i; ++j) {
      EXPECT_FALSE(s.insert(j));
    }
    EXPECT_TRUE(s.insert(i));
    EXPECT_EQ(s.first_missing(), i + 1);
    EXPECT_FALSE(s.is_full());
  }

  EXPECT_TRUE(s.insert(39));
  EXPECT_EQ(s.first_missing(), 40);
  EXPECT_TRUE(s.is_full());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(LatchingBitSetTest, MultiLevelTest)
{
  for (usize n : std::vector<usize>{
           64,            // one level, exactly full
           277,           // a prime number
           64 * 64,       // two full levels
           7777,          // three levels, not full
           64 * 64 * 64,  // three exactly full levels
       }) {
    llfs::LatchingBitSet s{n};

    EXPECT_EQ(s.upper_bound(), n);

    for (usize i = 0; i < s.upper_bound(); ++i) {
      for (usize j = i - std::min<usize>(i, 100); j < i; ++j) {
        EXPECT_TRUE(s.contains(j));
        EXPECT_FALSE(s.insert(j));
      }

      EXPECT_EQ(s.first_missing(), i);
      EXPECT_TRUE(s.insert(i));
      EXPECT_EQ(s.first_missing(), i + 1);
      if (i + 1 < s.upper_bound()) {
        EXPECT_FALSE(s.is_full());
      } else {
        EXPECT_TRUE(s.is_full());
      }
    }
  }
}

}  // namespace
