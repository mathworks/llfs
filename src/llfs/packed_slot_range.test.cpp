//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_slot_range.hpp>
//
#include <llfs/packed_slot_range.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace llfs::int_types;

TEST(PackedSlotRangeTest, Test)
{
  llfs::SlotRange slot_range{45, 108};

  EXPECT_EQ(llfs::packed_sizeof(slot_range), 16u);

  std::array<char, 16> buffer;
  buffer.fill(' ');
  {
    llfs::DataPacker packer{llfs::MutableBuffer{buffer.data(), buffer.size()}};

    llfs::PackedSlotRange* packed = llfs::pack_object(slot_range, &packer);

    ASSERT_NE(packed, nullptr);
    EXPECT_EQ(packed->lower_bound, 45u);
    EXPECT_EQ(packed->upper_bound, 108u);
    EXPECT_THAT(batt::to_string(*packed), ::testing::StrEq("[45, 108)"));
    EXPECT_EQ(llfs::packed_sizeof(*packed), 16u);
  }

  {
    batt::StatusOr<const llfs::PackedSlotRange*> unpacked =
        llfs::unpack_cast<llfs::PackedSlotRange>(buffer);

    ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(unpacked.status());
    ASSERT_NE(*unpacked, nullptr);
    EXPECT_EQ((**unpacked).lower_bound, 45u);
    EXPECT_EQ((**unpacked).upper_bound, 108u);
  }

  {
    batt::StatusOr<const llfs::PackedSlotRange*> unpacked =
        llfs::unpack_cast<llfs::PackedSlotRange>(
            llfs::ConstBuffer{buffer.data(), buffer.size() - 1});

    EXPECT_EQ(unpacked.status(), llfs::make_status(llfs::StatusCode::kUnpackCastStructOver));
  }
}

}  // namespace
