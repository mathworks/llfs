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
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/4, "", ""), ::testing::StrEq(""));
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "ab", "xy"), ::testing::StrEq(""));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  2. no common prefix
//    1. skip_len is 0
//
TEST(FindCommonPrefixTest, NoCommonPrefixSkipLen0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/0, "ab", "xy"), ::testing::StrEq(""));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  2. no common prefix
//    2. skip_len is >0
//
TEST(FindCommonPrefixTest, NoCommonPrefixSkipLenGt0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "iiab", "iixy"), ::testing::StrEq(""));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    1. a.length() < b.length()
//      1. skip_len is 0
//
TEST(FindCommonPrefixTest, CommonPrefixAShorterSkipLen0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/0, "abc", "abcdef"), ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    1. a.length() < b.length()
//      2. skip_len is >0
//
TEST(FindCommonPrefixTest, CommonPrefixAShorterSkipLenGt0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "iiabc", "iiabcdef"),
              ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    2. a.length() == b.length()
//      1. skip_len is 0
//
TEST(FindCommonPrefixTest, CommonPrefixAEqualLenSkipLen0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/0, "abc", "abc"), ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. common prefix == a
//    2. a.length() == b.length()
//      2. skip_len is >0
//
TEST(FindCommonPrefixTest, CommonPrefixAEqualLenSkipLenGt0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "xyabc", "wzabc"), ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    1. skip_len is 0
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLen0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/0, "abcxyz", "abc"), ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    2. skip_len is >0
//      1. a == b before skip_len
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLenGt0a)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "iiabcxyz", "iiabc"),
              ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    2. skip_len is >0
//      2. a != b before skip_len (no chars in common)
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLenGt0b)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "iiabcxyz", "jjabc"),
              ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. common prefix == b, a.length() > b.length()
//    2. skip_len is >0
//      3. a and b before skip_len have common prefix >0, <skip_len
//
TEST(FindCommonPrefixTest, CommonPrefixBNonEqualLenSkipLenGt0c)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "iiabcxyz", "ijabc"),
              ::testing::StrEq("abc"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  5. a empty, b not empty
//    1. skip_len == 0
//
TEST(FindCommonPrefixTest, AEmptyBNonEmptySkipLen0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/0, "", "abc"), ::testing::StrEq(""));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  5. a empty, b not empty
//    2. skip_len > 0
//
TEST(FindCommonPrefixTest, AEmptyBNonEmptySkipLenGt0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/5, "", "abc"), ::testing::StrEq(""));
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "ii", "jjabc"), ::testing::StrEq(""));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. a not empty, b empty
//    1. skip_len == 0
//
TEST(FindCommonPrefixTest, ANonEmptyBEmptySkipLen0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/0, "abc", ""), ::testing::StrEq(""));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. a not empty, b empty
//    2. skip_len > 0
//
TEST(FindCommonPrefixTest, ANonEmptyBEmptySkipLenGt0)
{
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/5, "abc", ""), ::testing::StrEq(""));
  EXPECT_THAT(llfs::find_common_prefix(/*skip_len=*/2, "jjabc", "ii"), ::testing::StrEq(""));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan: CompareKthByte
//
// 1. (char, string)
//   1. k > str.size
//   2. k == str.size
//   3. k == 0
//     1. str[k] == ch
//     2. str[k] > ch
//     3. str[k] < ch
//   4. 0 < k < str.size
//     1. str[k] == ch
//     2. str[k] > ch
//     3. str[k] < ch
// 2. (string, char)
//   1. k > str.size
//   2. k == str.size
//   3. k == 0
//     1. str[k] == ch
//     2. str[k] > ch
//     3. str[k] < ch
//   4. 0 < k < str.size
//     1. str[k] == ch
//     2. str[k] > ch
//     3. str[k] < ch
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   1. k > str.size
//
TEST(CompareKthByteTest, Case1_1)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/4}('\0', std::string_view{"abc"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   2. k == str.size
//
TEST(CompareKthByteTest, Case1_2)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/3}('\0', std::string_view{"abc"})));
  EXPECT_TRUE((llfs::CompareKthByte{/*k=*/2}('\0', std::string_view{"abc"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   3. k == 0
//     1. str[k] == ch
//
TEST(CompareKthByteTest, Case1_3_1)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/0}('a', std::string_view{"abc"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   3. k == 0
//     2. str[k] > ch
//
TEST(CompareKthByteTest, Case1_3_2)
{
  EXPECT_TRUE((llfs::CompareKthByte{/*k=*/0}(' ', std::string_view{"abc"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   3. k == 0
//     3. str[k] < ch
//
TEST(CompareKthByteTest, Case1_3_3)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/0}('d', std::string_view{"abc"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   4. 0 < k < str.size
//     1. str[k] == ch
//
TEST(CompareKthByteTest, Case1_4_1)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/2}('c', std::string_view{"abcd"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   4. 0 < k < str.size
//     2. str[k] > ch
//
TEST(CompareKthByteTest, Case1_4_2)
{
  EXPECT_TRUE((llfs::CompareKthByte{/*k=*/2}('b', std::string_view{"abcd"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. (char, string)
//   4. 0 < k < str.size
//     3. str[k] < ch
//
TEST(CompareKthByteTest, Case1_4_3)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/2}('d', std::string_view{"abcd"})));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   1. k > str.size
//
TEST(CompareKthByteTest, Case2_1)
{
  EXPECT_TRUE((llfs::CompareKthByte{/*k=*/4}(std::string_view{"abc"}, '~')));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   2. k == str.size
//
TEST(CompareKthByteTest, Case2_2)
{
  EXPECT_TRUE((llfs::CompareKthByte{/*k=*/3}(std::string_view{"abc"}, '\0')));
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/2}(std::string_view{"abc"}, '\0')));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   3. k == 0
//     1. str[k] == ch
//
TEST(CompareKthByteTest, Case2_3_1)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/0}(std::string_view{"abc"}, 'a')));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   3. k == 0
//     2. str[k] > ch
//
TEST(CompareKthByteTest, Case2_3_2)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/0}(std::string_view{"abc"}, ' ')));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   3. k == 0
//     3. str[k] < ch
//
TEST(CompareKthByteTest, Case2_3_3)
{
  EXPECT_TRUE((llfs::CompareKthByte{/*k=*/0}(std::string_view{"abc"}, 'b')));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   4. 0 < k < str.size
//     1. str[k] == ch
//
TEST(CompareKthByteTest, Case2_4_1)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/2}(std::string_view{"abcd"}, 'c')));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   4. 0 < k < str.size
//     2. str[k] > ch
//
TEST(CompareKthByteTest, Case2_4_2)
{
  EXPECT_FALSE((llfs::CompareKthByte{/*k=*/2}(std::string_view{"abcd"}, 'b')));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. (string, char)
//   4. 0 < k < str.size
//     3. str[k] < ch
//
TEST(CompareKthByteTest, Case2_4_3)
{
  EXPECT_TRUE((llfs::CompareKthByte{/*k=*/2}(std::string_view{"abcd"}, 'd')));
}

}  // namespace
