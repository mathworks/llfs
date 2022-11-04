//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_bytes.hpp>
//
#include <llfs/packed_bytes.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/data_packer.hpp>

namespace {

using namespace llfs::int_types;

TEST(PackedBytesTest, UnpackCast)
{
  std::string data = "0123456789";

  for (usize len = 0; len <= data.size(); ++len) {
    auto str = data.substr(0, len);

    if (str.size() <= 4) {
      EXPECT_EQ(llfs::packed_sizeof(str), sizeof(llfs::PackedBytes)) << BATT_INSPECT(len);
    } else {
      EXPECT_EQ(llfs::packed_sizeof(str), len + sizeof(llfs::PackedBytes)) << BATT_INSPECT(len);
    }

    std::vector<char> buffer(llfs::packed_sizeof(str));
    {
      llfs::DataPacker packer{llfs::MutableBuffer{buffer.data(), buffer.size()}};

      EXPECT_TRUE(packer.pack_string(str));
    }

    // unpack_cast - success case
    {
      batt::StatusOr<const llfs::PackedBytes*> unpacked =
          llfs::unpack_cast<llfs::PackedBytes>(buffer);

      ASSERT_TRUE(unpacked.ok());
      ASSERT_NE(*unpacked, nullptr);

      EXPECT_THAT((**unpacked).as_str(), ::testing::StrEq(str));

      // buffer underflow edge cases
      {
        EXPECT_EQ(llfs::validate_packed_value(**unpacked, buffer.data() + 1, buffer.size() - 1),
                  llfs::make_status(llfs::StatusCode::kUnpackCastPackedBytesStructUnder));
      }
    }

    // unpack_cast - failure case
    {
      batt::StatusOr<const llfs::PackedBytes*> unpacked =
          llfs::unpack_cast<llfs::PackedBytes>(batt::ConstBuffer{buffer.data(), buffer.size() - 1});

      ASSERT_FALSE(unpacked.ok());

      if (str.size() <= 4) {
        EXPECT_EQ(unpacked.status(),
                  llfs::make_status(llfs::StatusCode::kUnpackCastPackedBytesStructOver));
      } else {
        EXPECT_EQ(unpacked.status(),
                  llfs::make_status(llfs::StatusCode::kUnpackCastPackedBytesDataOver));
      }
    }
  }
}

}  // namespace
