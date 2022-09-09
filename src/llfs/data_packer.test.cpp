//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/data_packer.hpp>
//
#include <llfs/data_packer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/stream_util.hpp>

#include <cstring>

namespace {

using namespace llfs::int_types;

TEST(DataPackerTest, Arena)
{
  std::array<u8, 64> memory;
  memory.fill(0);

  std::array<u8, 64> expected;
  expected.fill(0);

  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(24);
  ASSERT_TRUE(arena);

  EXPECT_EQ(arena->capacity(), 24u);
  EXPECT_EQ(arena->space(), 24u);
  EXPECT_FALSE(arena->full());
  EXPECT_EQ(arena->unused(), (batt::Interval<isize>{40, 64}));

  EXPECT_EQ(0, std::memcmp(memory.data(), expected.data(), memory.size()));

  const std::string_view test_str = "hello, world";

  llfs::Optional<std::string_view> packed_str = packer.pack_string(test_str, &*arena);
  ASSERT_TRUE(packed_str);

  EXPECT_EQ(*packed_str, test_str);

  // Set up the expected data.
  //
  {
    auto* packed_bytes = reinterpret_cast<llfs::PackedBytes*>(expected.data());
    packed_bytes->data_offset = 40;
    packed_bytes->data_size = test_str.size();
    std::memcpy(expected.data() + 40, test_str.data(), test_str.size());
  }

  // Compare actual to expected.
  //
  EXPECT_EQ(0, std::memcmp(memory.data(), expected.data(), memory.size()))
      << "\nmemory=\n"
      << batt::dump_hex(memory.data(), memory.size()) << "\nexpected=\n"
      << batt::dump_hex(expected.data(), expected.size());

  auto* packed_bytes1 = reinterpret_cast<const llfs::PackedBytes*>(memory.data());

  EXPECT_EQ(packed_bytes1->size(), test_str.size());
  EXPECT_EQ((void*)packed_bytes1->data(), memory.data() + 40);

  // Now pack a string in the regular way and see that it lands at the back of the available region.
  //
  const std::string_view test_str2 = "so long, farewell";
  llfs::Optional<std::string_view> packed_str2 = packer.pack_string(test_str2);
  ASSERT_TRUE(packed_str2);

  EXPECT_EQ(*packed_str2, test_str2);

  // Update the expected data.
  //
  {
    auto* packed_bytes2 = reinterpret_cast<llfs::PackedBytes*>(expected.data()) + 1;
    packed_bytes2->data_offset = 40 - (sizeof(llfs::PackedBytes) + test_str2.size());
    packed_bytes2->data_size = test_str2.size();
    std::memcpy(expected.data() + (40 - test_str2.size()), test_str2.data(), test_str2.size());
  }

  // Compare actual to expected.
  //
  EXPECT_EQ(0, std::memcmp(memory.data(), expected.data(), memory.size()))
      << "\nmemory=\n"
      << batt::dump_hex(memory.data(), memory.size()) << "\nexpected=\n"
      << batt::dump_hex(expected.data(), expected.size());
}

}  // namespace
