#include <llfs/data_packer.hpp>
//
#include <llfs/data_packer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/data_reader.hpp>

#include <bitset>
#include <random>

namespace {

using namespace llfs::int_types;

using llfs::ConstBuffer;
using llfs::DataPacker;
using llfs::DataReader;
using llfs::MutableBuffer;
using llfs::Optional;
using llfs::packed_sizeof_varint;

TEST(DataPackerTest, VarInt)
{
  std::default_random_engine rng{1};

  u64 min_value = 0;
  u64 max_value = (1ull << 7) - 1;

  for (usize nbytes = 1; nbytes <= 10; ++nbytes) {
    VLOG(1) << "\n" << std::bitset<64>{min_value} << "..\n" << std::bitset<64>{max_value};
    std::uniform_int_distribution<u64> pick_number{min_value, max_value};
    for (usize i = 0; i < 10000; ++i) {
      u64 n = pick_number(rng);

      ASSERT_EQ(packed_sizeof_varint(n), nbytes)
          << BATT_INSPECT(n) << BATT_INSPECT(min_value) << BATT_INSPECT(max_value);

      std::vector<u8> storage(nbytes);
      MutableBuffer buffer{storage.data(), storage.size()};
      {
        DataPacker packer{buffer};
        EXPECT_TRUE(packer.pack_varint(n));
      }
      ConstBuffer data = buffer;
      DataReader reader{data};
      Optional<u64> out = reader.read_varint();
      ASSERT_TRUE(out);
      EXPECT_EQ(*out, n);
    }
    min_value = max_value + 1;
    max_value = ((max_value + 1) << 7) - 1;
  }
}

}  // namespace
