//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/varint.hpp>
//
#include <llfs/varint.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/logging.hpp>

#include <llfs/data_reader.hpp>

#include <bitset>
#include <random>

namespace {

using namespace llfs::int_types;

using llfs::ConstBuffer;
using llfs::MutableBuffer;
using llfs::None;
using llfs::Optional;
using llfs::pack_varint_to;
using llfs::packed_sizeof_varint;
using llfs::unpack_varint_from;

// Verify the edge-case boundaries of packed varint size (i.e., the precise integer values where the
// packed size of a varint changes).
//
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 1)) - 1), 1);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 1)) - 0), 2);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 2)) - 1), 2);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 2)) - 0), 3);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 3)) - 1), 3);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 3)) - 0), 4);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 4)) - 1), 4);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 4)) - 0), 5);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 5)) - 1), 5);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 5)) - 0), 6);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 6)) - 1), 6);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 6)) - 0), 7);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 7)) - 1), 7);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 7)) - 0), 8);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 8)) - 1), 8);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 8)) - 0), 9);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST(VarIntTest, RandomValues)
{
  std::default_random_engine rng{1};

  u64 min_value = 0;
  u64 max_value = (1ull << 7) - 1;

  for (usize nbytes = 1; nbytes <= 10; ++nbytes) {
    LLFS_VLOG(1) << "\n" << std::bitset<64>{min_value} << "..\n" << std::bitset<64>{max_value};
    std::uniform_int_distribution<u64> pick_number{min_value, max_value};
    for (usize i = 0; i < 10000; ++i) {
      u64 n = pick_number(rng);

      const usize expected_size = packed_sizeof_varint(n);
      ASSERT_EQ(expected_size, nbytes)
          << BATT_INSPECT(n) << BATT_INSPECT(min_value) << BATT_INSPECT(max_value);

      std::vector<u8> storage(nbytes);
      {
        u8* packed_end = llfs::pack_varint_to(storage.data(), storage.data() + storage.size(), n);
        EXPECT_EQ(packed_end, storage.data() + expected_size);
      }
      Optional<u64> out;
      const u8* parse_end = nullptr;
      std::tie(out, parse_end) =
          llfs::unpack_varint_from(storage.data(), storage.data() + storage.size());
      ASSERT_TRUE(out);
      EXPECT_EQ(*out, n);
    }
    min_value = max_value + 1;
    max_value = ((max_value + 1) << 7) - 1;
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST(VarIntTest, BufferApi)
{
  const u64 small_num = 7;
  const u64 large_num = 0xfedcba9876543210ull;

  constexpr usize small_num_size = 1;
  constexpr usize large_num_size = 10;

  ASSERT_EQ(packed_sizeof_varint(small_num), small_num_size);
  ASSERT_EQ(packed_sizeof_varint(large_num), large_num_size);

  std::array<char, small_num_size> small_buf;
  std::array<char, large_num_size> large_buf;

  EXPECT_FALSE(pack_varint_to(MutableBuffer{small_buf.data(), small_buf.size()}, large_num));
  EXPECT_FALSE(pack_varint_to(MutableBuffer{large_buf.data(), large_buf.size() - 1}, large_num));

  Optional<MutableBuffer> packed;

  packed = None;
  packed = pack_varint_to(MutableBuffer{small_buf.data(), small_buf.size()}, small_num);

  ASSERT_TRUE(packed);
  EXPECT_EQ((void*)packed->data(), (void*)(small_buf.data() + small_buf.size()));
  EXPECT_EQ(packed->size(), 0u);

  packed = None;
  packed = pack_varint_to(MutableBuffer{large_buf.data(), large_buf.size()}, large_num);

  ASSERT_TRUE(packed);
  EXPECT_EQ((void*)packed->data(), (void*)(large_buf.data() + large_buf.size()));
  EXPECT_EQ(packed->size(), 0u);

  {
    auto src = ConstBuffer{small_buf.data(), small_buf.size()};
    Optional<u64> unpacked_small_num = unpack_varint_from(&src);

    ASSERT_TRUE(unpacked_small_num);
    EXPECT_EQ(*unpacked_small_num, small_num);

    EXPECT_EQ((void*)src.data(), (void*)(small_buf.data() + small_buf.size()));
    EXPECT_EQ(src.size(), 0u);

    auto [n, rest] = unpack_varint_from(ConstBuffer{small_buf.data(), small_buf.size()});

    ASSERT_TRUE(n);
    EXPECT_EQ(*n, small_num);
    EXPECT_EQ(rest.size(), 0u);
    EXPECT_EQ((void*)rest.data(), (void*)(small_buf.data() + small_buf.size()));
  }
  {
    auto src = ConstBuffer{large_buf.data(), large_buf.size()};
    Optional<u64> unpacked_large_num = unpack_varint_from(&src);

    ASSERT_TRUE(unpacked_large_num);
    EXPECT_EQ(*unpacked_large_num, large_num);

    EXPECT_EQ((void*)src.data(), (void*)(large_buf.data() + large_buf.size()));
    EXPECT_EQ(src.size(), 0u);

    auto [n, rest] = unpack_varint_from(ConstBuffer{large_buf.data(), large_buf.size()});

    ASSERT_TRUE(n);
    EXPECT_EQ(*n, large_num);
    EXPECT_EQ(rest.size(), 0u);
    EXPECT_EQ((void*)rest.data(), (void*)(large_buf.data() + large_buf.size()));
  }
}

}  // namespace
