//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/pack_as_raw.hpp>
//
#include <llfs/pack_as_raw.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/data_packer.hpp>
#include <llfs/packed_bytes.hpp>

#include <string_view>
#include <vector>

#ifndef LLFS_PACKED_BYTES_HPP
#error !
#endif

namespace {

TEST(PackObjectAsRawTest, Test)
{
  static_assert(std::is_same_v<llfs::PackedBytes, llfs::PackedTypeFor<std::string_view>>);

  std::string_view to_pack = "A medium length string";

  EXPECT_EQ(llfs::packed_sizeof(to_pack), 8 /*sizeof(llfs::PackedBytes)*/ + to_pack.size());

  // First verify the normal object packing for std::string_view -> llfs::PackedBytes.
  {
    std::vector<char> storage(llfs::packed_sizeof(to_pack));
    {
      llfs::DataPacker packer{llfs::MutableBuffer{storage.data(), storage.size()}};

      llfs::PackedBytes* packed = llfs::pack_object(to_pack, &packer);

      ASSERT_NE(packed, nullptr);
      EXPECT_EQ(packed->size(), to_pack.size());
      EXPECT_THAT(packed->as_str(), ::testing::StrEq(to_pack));
      EXPECT_EQ((void*)packed, (void*)storage.data());
    }
  }

  // Now wrap the std::string_view in PackObjectAsRaw.
  //
  llfs::PackObjectAsRawData<std::string_view&> to_pack_as_raw{to_pack};

  EXPECT_EQ(llfs::packed_sizeof(to_pack), llfs::packed_sizeof(to_pack_as_raw));
  {
    std::vector<char> storage(llfs::packed_sizeof(to_pack_as_raw));
    {
      llfs::DataPacker packer{llfs::MutableBuffer{storage.data(), storage.size()}};

      llfs::PackedRawData* packed_raw = llfs::pack_object(to_pack_as_raw, &packer);

      ASSERT_NE(packed_raw, nullptr);
      EXPECT_EQ((void*)packed_raw, (void*)storage.data());

      llfs::PackedBytes* packed = reinterpret_cast<llfs::PackedBytes*>(packed_raw);

      ASSERT_NE(packed, nullptr);
      EXPECT_EQ(packed->size(), to_pack.size());
      EXPECT_THAT(packed->as_str(), ::testing::StrEq(to_pack));
      EXPECT_EQ((void*)packed, (void*)storage.data());
    }
  }
}

}  // namespace
