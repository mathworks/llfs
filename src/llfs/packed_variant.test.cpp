//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_variant.hpp>
//
#include <llfs/packed_variant.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/data_packer.hpp>

#include <batteries/assert.hpp>

namespace {

using namespace llfs::int_types;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PackedVariantTest : public ::testing::Test
{
 public:
  using Var = llfs::PackedVariant<llfs::little_u16, llfs::little_u32, llfs::little_u64>;

  template <typename T>
  void pack_value(const T& src_value)
  {
    auto to_pack = llfs::pack_as_variant<Var>(src_value);
    this->buffer.resize(llfs::packed_sizeof(to_pack));
    llfs::DataPacker packer{llfs::MutableBuffer{this->buffer.data(), this->buffer.size()}};

    ASSERT_NE(llfs::pack_object(to_pack, &packer), nullptr);
  }

  template <typename T, typename U>
  void success_case(const T& src_value, U expected_value, u8 expected_case)
  {
    ASSERT_NO_FATAL_FAILURE(this->pack_value(src_value));

    llfs::StatusOr<const Var*> unpacked = llfs::unpack_cast<Var>(this->buffer);

    ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(unpacked.status());
    ASSERT_EQ((int)(**unpacked).which, (int)expected_case);
    ASSERT_EQ(((**unpacked).visit([](auto value) -> decltype(expected_value) {
                return value;
              })),
              expected_value);
  }

  template <typename T>
  void overflow_error_case(const T& src_value, batt::Status expected_error, usize truncate_count)
  {
    ASSERT_NO_FATAL_FAILURE(this->pack_value(src_value));

    BATT_CHECK_LE(truncate_count, this->buffer.size());

    llfs::StatusOr<const Var*> unpacked = llfs::unpack_cast<Var>(
        llfs::ConstBuffer{this->buffer.data(), this->buffer.size() - truncate_count});

    ASSERT_FALSE(unpacked.ok());
    ASSERT_EQ(unpacked.status(), expected_error);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::vector<char> buffer;
  llfs::little_u16 n16 = 111;
  llfs::little_u32 n32 = 222;
  llfs::little_u64 n64 = 333;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PackedVariantTest, UnpackCastSuccessAllCases)
{
  ASSERT_NO_FATAL_FAILURE(this->success_case(this->n16, this->n16.value(), /*which=*/0u));
  ASSERT_NO_FATAL_FAILURE(this->success_case(this->n32, this->n32.value(), /*which=*/1u));
  ASSERT_NO_FATAL_FAILURE(this->success_case(this->n64, this->n64.value(), /*which=*/2u));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PackedVariantTest, UnpackCastFailVariantStructOOB)
{
  ASSERT_NO_FATAL_FAILURE(this->overflow_error_case(
      this->n16, llfs::StatusCode::kUnpackCastVariantStructOutOfBounds, 3u));
  ASSERT_NO_FATAL_FAILURE(this->overflow_error_case(
      this->n32, llfs::StatusCode::kUnpackCastVariantStructOutOfBounds, 5u));
  ASSERT_NO_FATAL_FAILURE(this->overflow_error_case(
      this->n64, llfs::StatusCode::kUnpackCastVariantStructOutOfBounds, 9u));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PackedVariantTest, UnpackCastFailCastDataOOB)
{
  ASSERT_NO_FATAL_FAILURE(
      this->overflow_error_case(this->n16, llfs::StatusCode::kUnpackCastWrongIntegerSize, 1u));
  ASSERT_NO_FATAL_FAILURE(
      this->overflow_error_case(this->n32, llfs::StatusCode::kUnpackCastWrongIntegerSize, 1u));
  ASSERT_NO_FATAL_FAILURE(
      this->overflow_error_case(this->n64, llfs::StatusCode::kUnpackCastWrongIntegerSize, 1u));
}

}  // namespace
