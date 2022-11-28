//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_status_or.hpp>
//
#include <llfs/packed_status_or.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/data_packer.hpp>

namespace {

using namespace llfs::int_types;

TEST(PackedStatusOrTest, PackUnPack)
{
  batt::Status not_found_status{batt::StatusCode::kNotFound};

  batt::StatusOr<little_u32> ok_int = 123u;
  const usize ok_int_packed_size = llfs::packed_sizeof(ok_int);

  EXPECT_EQ(ok_int_packed_size, 5u);

  batt::StatusOr<little_u32> not_ok_int = not_found_status;
  const usize not_ok_int_packed_size = llfs::packed_sizeof(not_ok_int);

  EXPECT_EQ(not_ok_int_packed_size, 1u + llfs::packed_sizeof(not_found_status));

  // Pack an ok StatusOr - success case
  //
  std::vector<char> ok_buffer(ok_int_packed_size);
  {
    llfs::DataPacker packer{llfs::MutableBuffer{ok_buffer.data(), ok_buffer.size()}};

    llfs::PackedStatusOr<little_u32>* packed = pack_object(ok_int, &packer);

    ASSERT_NE(packed, nullptr);
    EXPECT_THAT(batt::to_string(*packed), ::testing::StrEq("Ok{123}"));
    EXPECT_EQ(llfs::packed_sizeof(*packed), ok_int_packed_size);
    EXPECT_EQ(packed->ok, 1u);
  }

  // Unpack cast - ok StatusOr - success case
  //
  {
    batt::StatusOr<const llfs::PackedStatusOr<little_u32>&> unpacked =
        llfs::unpack_cast<llfs::PackedStatusOr<little_u32>>(ok_buffer);

    ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(unpacked.status());

    EXPECT_THAT(batt::to_string(*unpacked), ::testing::StrEq("Ok{123}"));
    EXPECT_THAT(batt::to_string(unpacked->status()), ::testing::StrEq("batt::StatusCode{0}:Ok"));
    EXPECT_THAT(unpacked->value(), ::testing::Eq(123u));
  }

  // Unpack cast - ok StatusOr - failure case
  //
  {
    batt::StatusOr<const llfs::PackedStatusOr<little_u32>&> unpacked =
        llfs::unpack_cast<llfs::PackedStatusOr<little_u32>>(
            llfs::ConstBuffer{ok_buffer.data(), ok_buffer.size() - 1});

    EXPECT_EQ(unpacked.status(), llfs::make_status(llfs::StatusCode::kUnpackCastWrongIntegerSize));
  }

  // Pack a non-ok StatusOr - success case
  //
  std::vector<char> not_ok_buffer(not_ok_int_packed_size);
  {
    llfs::DataPacker packer{llfs::MutableBuffer{not_ok_buffer.data(), not_ok_buffer.size()}};
    llfs::PackedStatusOr<little_u32>* packed = pack_object(not_ok_int, &packer);

    ASSERT_NE(packed, nullptr) << not_ok_int_packed_size;
    EXPECT_THAT(batt::to_string(*packed),
                ::testing::StrEq("Status{batt::StatusCode{5}:Not Found}"));
    EXPECT_EQ(llfs::packed_sizeof(*packed), not_ok_int_packed_size);
    EXPECT_EQ(packed->ok, 0u);
  }

  // Unpack cast - non-ok StatusOr - success case
  //
  {
    batt::StatusOr<const llfs::PackedStatusOr<little_u32>&> unpacked =
        llfs::unpack_cast<llfs::PackedStatusOr<little_u32>>(not_ok_buffer);

    ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(unpacked.status());

    EXPECT_THAT(batt::to_string(*unpacked),
                ::testing::StrEq("Status{batt::StatusCode{5}:Not Found}"));
    EXPECT_THAT(batt::to_string(unpacked->status()),
                ::testing::StrEq("batt::StatusCode{5}:Not Found"));
    EXPECT_DEATH(unpacked->value(), "Assertion failed: this->ok !=.*0");
  }

  // Unpack cast - non-ok StatusOr - failure case
  //
  {
    batt::StatusOr<const llfs::PackedStatusOr<little_u32>&> unpacked =
        llfs::unpack_cast<llfs::PackedStatusOr<little_u32>>(
            llfs::ConstBuffer{not_ok_buffer.data(), not_ok_buffer.size() - 1});

    EXPECT_EQ(unpacked.status(),
              llfs::make_status(llfs::StatusCode::kUnpackCastPackedBytesDataOver));
  }
}

}  // namespace
