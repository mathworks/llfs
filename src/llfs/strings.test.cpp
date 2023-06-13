//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/strings.hpp>
//
#include <llfs/strings.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan: find_common_prefix
//
//  1. a and b are both empty - return empty
//    1. skip_len is 0
//    2. skip_len is >0
//  2. no common prefix
//    1. skip_len is 0
//    2. skip_len is >0
//  3. common prefix == a
//    1. a.length() < b.length()
//      1. skip_len is 0
//      2. skip_len is >0
//    2. a.length() == b.length()
//      1. skip_len is 0
//      2. skip_len is >0
//  4. common prefix == b, a.length() > b.length()
//    1. skip_len is 0
//    2. skip_len is >0
//      1. a == b before skip_len
//      2. a != b before skip_len (no chars in common)
//      3. a and b before skip_len have common prefix >0, <skip_len
//  5. a empty, b not empty
//    1. skip_len is 0
//    2. skip_len is >0
//  6. a not empty, b empty
//    1. skip_len is 0
//    2. skip_len is >0
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  1. a and b are both empty - return empty
//    1. skip_len is 0
//
TEST(FindCommonPrefixTest, BothEmptySkipLen0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/0, "", ""), ::testing::StrEq(""));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  1. a and b are both empty - return empty
//    2. skip_len is >0
//
TEST(FindCommonPrefixTest, BothEmptySkipLenGt0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  2. no common prefix
//    1. skip_len is 0
//
TEST(FindCommonPrefixTest, NoCommonPrefixSkipLen0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  2. no common prefix
//    2. skip_len is >0
//
TEST(FindCommonPrefixTest, NoCommonPrefixSkipLenGt0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    1. a.length() < b.length()
//      1. skip_len is 0
//
TEST(FindCommonPrefixTest, CommonPrefixAShorterSkipLen0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    1. a.length() < b.length()
//      2. skip_len is >0
//
TEST(FindCommonPrefixTest, CommonPrefixAShorterSkipLenGt0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    2. a.length() == b.length()
//      1. skip_len is 0
//
TEST(FindCommonPrefixTest, CommonPrefixAEqualLenSkipLen0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    2. a.length() == b.length()
//      2. skip_len is >0
//
TEST(FindCommonPrefixTest, CommonPrefixAEqualLenSkipLenGt0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    1. skip_len is 0
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLen0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    2. skip_len is >0
//      1. a == b before skip_len
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLenGt0a)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    2. skip_len is >0
//      2. a != b before skip_len (no chars in common)
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLenGt0b)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    2. skip_len is >0
//      3. a and b before skip_len have common prefix >0, <skip_len
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLenGt0c)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  5. a empty, b not empty
//    1. skip_len == 0
//
TEST(FindCommonPrefixTest, AEmptyBNonEmptySkipLen0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  5. a empty, b not empty
//    2. skip_len > 0
//
TEST(FindCommonPrefixTest, AEmptyBNonEmptySkipLenGt0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. a not empty, b empty
//    1. skip_len == 0
//
TEST(FindCommonPrefixTest, ANonEmptyBEmptySkipLen0)
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. a not empty, b empty
//    2. skip_len > 0
//
TEST(FindCommonPrefixTest, ANonEmptyBEmptySkipLenGt0)
{
}

}  // namespace
