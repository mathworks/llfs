//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_writer.hpp>
//
#include <llfs/slot_writer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/varint.hpp>

namespace {

using namespace llfs::int_types;

TEST(SlotWriterTest, BeginEndAtomicRangeTokens)
{
  llfs::Slice<const u8> begin_token = llfs::SlotWriter::WriterLock::begin_atomic_range_token();
  llfs::Slice<const u8> end_token = llfs::SlotWriter::WriterLock::end_atomic_range_token();

  EXPECT_EQ(begin_token.size(), llfs::SlotWriter::WriterLock::kBeginAtomicRangeTokenSize);
  EXPECT_EQ(end_token.size(), llfs::SlotWriter::WriterLock::kEndAtomicRangeTokenSize);
  EXPECT_NE(begin_token.size(), end_token.size());
  EXPECT_GT(begin_token.size(), 1u);
  EXPECT_GT(end_token.size(), 1u);

  const auto verify_parses_as_zero = [](const llfs::Slice<const u8>& token) {
    auto [parsed_n, end_of_parse] = llfs::unpack_varint_from(token.begin(), token.end());

    ASSERT_TRUE(parsed_n);
    EXPECT_EQ(*parsed_n, 0u);
    EXPECT_EQ(end_of_parse, token.end());
  };

  verify_parses_as_zero(begin_token);
  verify_parses_as_zero(end_token);
}

}  // namespace
