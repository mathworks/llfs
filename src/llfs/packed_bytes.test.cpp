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
      batt::StatusOr<const llfs::PackedBytes&> unpacked =
          llfs::unpack_cast<llfs::PackedBytes>(buffer);

      ASSERT_TRUE(unpacked.ok());
      EXPECT_THAT(unpacked->as_str(), ::testing::StrEq(str));

      // buffer underflow edge cases
      {
        EXPECT_EQ(llfs::validate_packed_value(*unpacked, buffer.data() + 1, buffer.size() - 1),
                  llfs::make_status(llfs::StatusCode::kUnpackCastPackedBytesStructUnder));
      }
    }

    // unpack_cast - failure case
    {
      batt::StatusOr<const llfs::PackedBytes&> unpacked =
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

TEST(PackedBytesTest, Clear)
{
  char buffer[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  static_assert(sizeof(buffer) > sizeof(llfs::PackedBytes));

  llfs::PackedBytes* p = reinterpret_cast<llfs::PackedBytes*>(buffer);

  EXPECT_EQ(p->size(), 0x070605u) << BATT_INSPECT(batt::to_string(std::hex, p->size()));

  buffer[2] = 0;

  EXPECT_EQ(p->size(), 0x070605u) << BATT_INSPECT(batt::to_string(std::hex, p->size()));

  buffer[1] = 0;

  EXPECT_EQ(p->size(), 7u) << BATT_INSPECT(batt::to_string(std::hex, p->size()));

  buffer[3] = 0;

  EXPECT_EQ(p->size(), 7u) << BATT_INSPECT(batt::to_string(std::hex, p->size()));

  buffer[0] = 0;

  EXPECT_EQ(p->size(), 8u) << BATT_INSPECT(batt::to_string(std::hex, p->size()));

  p->clear();

  EXPECT_EQ(p->size(), 0u);
  EXPECT_EQ(p->data(), (const void*)(buffer + sizeof(llfs::PackedBytes)));
  EXPECT_THAT(p->as_str(), ::testing::StrEq(""));
}

TEST(PackedBytesTest, PackAsFragments)
{
  std::array<char, 100> fragment_1;
  std::array<char, sizeof(llfs::PackedBytes)> fragment_0;

  auto* rec = reinterpret_cast<llfs::PackedBytes*>(fragment_0.data());

  std::string_view s1 = "abc";
  std::string_view s2 = "hello, big wide world!";

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack s1.
  //
  const bool s1_uses_extra_fragment = llfs::pack_as_fragments(
      s1, rec, /*offset=*/BATT_CHECKED_CAST(u32, sizeof(llfs::PackedBytes)));

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Verify packed s1.
  //
  EXPECT_FALSE(s1_uses_extra_fragment);

  std::vector s1_buffer_sequence{llfs::ConstBuffer{fragment_0.data(), fragment_0.size()}};

  std::array<char, 100 + sizeof(llfs::PackedBytes)> all_fragments_copy;

  std::memset(all_fragments_copy.data(), '\0', all_fragments_copy.size());

  boost::asio::buffer_copy(
      llfs::MutableBuffer{all_fragments_copy.data(), all_fragments_copy.size()},
      s1_buffer_sequence);

  const auto* rec_copy = reinterpret_cast<const llfs::PackedBytes*>(all_fragments_copy.data());

  EXPECT_EQ(rec_copy->size(), s1.size());
  EXPECT_THAT(rec_copy->as_str(), ::testing::StrEq(s1));

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack s2.
  //
  const bool s2_uses_extra_fragment = llfs::pack_as_fragments(
      s2, rec, /*offset=*/BATT_CHECKED_CAST(u32, sizeof(llfs::PackedBytes)));

  std::memcpy(fragment_1.data(), s2.data(), s2.size());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Verify packed s2.
  //
  EXPECT_TRUE(s2_uses_extra_fragment);

  std::vector s2_buffer_sequence{
      llfs::ConstBuffer{fragment_0.data(), fragment_0.size()},
      llfs::ConstBuffer{fragment_1.data(), fragment_1.size()},
  };
  std::memset(all_fragments_copy.data(), '\0', all_fragments_copy.size());

  boost::asio::buffer_copy(
      llfs::MutableBuffer{all_fragments_copy.data(), all_fragments_copy.size()},
      s2_buffer_sequence);

  EXPECT_EQ(rec_copy->size(), s2.size());
  EXPECT_THAT(rec_copy->as_str(), ::testing::StrEq(s2));
}

}  // namespace
