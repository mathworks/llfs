//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/unpack_cast.hpp>
//
#include <llfs/unpack_cast.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

// Test Plan:
//  - For all packed endian-ordered int types {little/big_i8/u8/i16/.../u64}
//    - Correct size succeeds
//    - Zero fails
//    - >0, too small fails (if possible)
//    - too big fails
//    - nullptr with correct size fails (different code)
//
TEST(UnpackCastTest, Test)
{
  using namespace llfs::int_types;

  const auto test_case = [](const auto& value) {
    using PackedT = std::decay_t<decltype(value)>;

    {
      llfs::StatusOr<const PackedT&> result =
          llfs::unpack_cast<PackedT>(llfs::ConstBuffer{&value, sizeof(PackedT)});

      ASSERT_TRUE(result.ok()) << BATT_INSPECT(result.status());
    }
    {
      llfs::StatusOr<const PackedT&> result =
          llfs::unpack_cast<PackedT>(llfs::ConstBuffer{&value, 0});

      EXPECT_TRUE(!result.ok()) << BATT_INSPECT(result.status());
      EXPECT_EQ(result.status(), llfs::StatusCode::kUnpackCastWrongIntegerSize);
    }
    {
      llfs::StatusOr<const PackedT&> result =
          llfs::unpack_cast<PackedT>(llfs::ConstBuffer{&value, sizeof(PackedT) - 1});

      EXPECT_TRUE(!result.ok()) << BATT_INSPECT(result.status());
      EXPECT_EQ(result.status(), llfs::StatusCode::kUnpackCastWrongIntegerSize);
    }
    {
      llfs::StatusOr<const PackedT&> result =
          llfs::unpack_cast<PackedT>(llfs::ConstBuffer{&value, sizeof(PackedT) + 1});

      EXPECT_TRUE(!result.ok()) << BATT_INSPECT(result.status());
      EXPECT_EQ(result.status(), llfs::StatusCode::kUnpackCastWrongIntegerSize);
    }
    {
      llfs::StatusOr<const PackedT&> result =
          llfs::unpack_cast<PackedT>(llfs::ConstBuffer{nullptr, sizeof(PackedT)});

      EXPECT_TRUE(!result.ok()) << BATT_INSPECT(result.status());
      EXPECT_EQ(result.status(), llfs::StatusCode::kUnpackCastNullptr);
    }
  };

  test_case(little_i8{17});
  test_case(little_u8{17u});
  test_case(little_i16{17});
  test_case(little_u16{17u});
  test_case(little_i32{17});
  test_case(little_u32{17u});
  test_case(little_i64{17});
  test_case(little_u64{17u});

  test_case(big_i8{17});
  test_case(big_u8{17u});
  test_case(big_i16{17});
  test_case(big_u16{17u});
  test_case(big_i32{17});
  test_case(big_u32{17u});
  test_case(big_i64{17});
  test_case(big_u64{17u});
}

}  // namespace
