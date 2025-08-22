//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_float.hpp>
//
#include <llfs/packed_float.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <bitset>
#include <cmath>
#include <limits>
#include <random>

namespace {

using namespace llfs::int_types;

constexpr i32 kNumTestCases = 1000 * 1000;

template <typename PackedT, typename FloatT>
void verify_case_for_packed_type(FloatT value, i32 case_i)
{
  u8 buffer[sizeof(PackedT)];

  {
    std::memset(buffer, 0xba, sizeof(buffer));

    // Construct from double
    //
    const PackedT& packed = *(new (buffer) PackedT(value));
    FloatT unpacked = packed;

    EXPECT_EQ(value, unpacked) << BATT_INSPECT(case_i) << " "
                               << std::bitset<64>(packed.int_value());
  }
  {
    std::memset(buffer, 0x73, sizeof(buffer));

    // Assign from double
    //
    PackedT& packed = reinterpret_cast<PackedT&>(buffer);
    packed = value;
    FloatT unpacked = packed;

    EXPECT_EQ(value, unpacked) << BATT_INSPECT(case_i) << " "
                               << std::bitset<64>(packed.int_value());
  }
}

void verify_case(double value, i32 case_i)
{
  verify_case_for_packed_type<llfs::little_f64, double>(value, case_i * 4);
  verify_case_for_packed_type<llfs::big_f64, double>(value, case_i * 4 + 1);

  const float narrow = value;
  verify_case_for_packed_type<llfs::little_f32, float>(narrow, case_i * 4 + 2);
  verify_case_for_packed_type<llfs::big_f32, float>(narrow, case_i * 4 + 3);
}

TEST(PackedFloat, Test)
{
  const double pos_inf = std::numeric_limits<double>::infinity();
  const double neg_inf = -std::numeric_limits<double>::infinity();

  ASSERT_TRUE(std::isinf(pos_inf));
  ASSERT_FALSE(std::signbit(pos_inf));
  ASSERT_TRUE(std::isinf(neg_inf));
  ASSERT_TRUE(std::signbit(neg_inf));

  // First verify edge cases.
  //
  verify_case(std::numeric_limits<double>::infinity(), -1);
  verify_case(-std::numeric_limits<double>::infinity(), -2);
  verify_case(0.0, -3);
  verify_case(-0.0, -4);

  // Randomly generate other test cases to verify that the impl is likely correct for all values.
  //
  std::default_random_engine rng{1};
  std::uniform_real_distribution<double> pick_value{std::numeric_limits<double>::lowest(),
                                                    std::numeric_limits<double>::max()};

  for (i32 i = 0; i < kNumTestCases; ++i) {
    double value = pick_value(rng);
    verify_case(value, i);
  }
}

}  // namespace
