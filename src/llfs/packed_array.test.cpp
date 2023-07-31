//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_array.hpp>
//
#include <llfs/packed_array.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/static_assert.hpp>

#include <llfs/data_packer.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_seq.hpp>

#include <vector>

namespace {

using namespace llfs::int_types;

BATT_STATIC_ASSERT_TYPE_EQ(llfs::PackedTypeFor<i32>, little_i32);

TEST(PackedArrayTest, PackBoxedSeq)
{
  std::vector<u8> buffer;

  std::vector<i32> numbers{0, 1, 2, 3, 4, 5};

  buffer.resize(llfs::packed_sizeof(llfs::as_seq(numbers) | llfs::seq::boxed()));
  {
    llfs::DataPacker dst{llfs::MutableBuffer{buffer.data(), buffer.size()}};

    llfs::PackedArray<little_i32>* packed =
        pack_object(llfs::as_seq(numbers) | llfs::seq::boxed(), &dst);

    ASSERT_NE(packed, nullptr);

    EXPECT_THAT(*packed, ::testing::ElementsAre(0, 1, 2, 3, 4, 5));
    EXPECT_EQ(dst.space(), 0u);
  }
  {
    llfs::DataReader src{llfs::ConstBuffer{buffer.data(), buffer.size()}};

    auto out = llfs::read_object(&src, batt::StaticType<llfs::BoxedSeq<i32>>{});

    ASSERT_TRUE(out.ok()) << BATT_INSPECT(out.status());

    EXPECT_THAT(std::move(*out) | llfs::seq::collect_vec(),
                ::testing::ElementsAre(0, 1, 2, 3, 4, 5));
  }
}

TEST(PackedArrayTest, PackArrayCachedSize)
{
  std::vector<u8> buffer;

  std::vector<i32> numbers{0, 10, 20, 30, 40, 50};

  buffer.resize(llfs::packed_sizeof(llfs::as_seq(numbers) | llfs::seq::boxed()));
  {
    llfs::DataPacker dst{llfs::MutableBuffer{buffer.data(), buffer.size()}};

    llfs::PackedArray<little_i32>* packed =
        pack_object(llfs::as_seq(numbers) | llfs::seq::boxed(), &dst);

    ASSERT_NE(packed, nullptr);
    packed->initialize(numbers.size());

    // check for initial value
    //
    ASSERT_EQ(packed->flags, 0);
    ASSERT_EQ(packed->size_in_bytes, 0u);
    ASSERT_FALSE(packed->is_valid_size_in_bytes());

    packed->initialize_size_in_bytes(numbers.size() * sizeof(i32));
    ASSERT_TRUE(packed->is_valid_size_in_bytes());

    EXPECT_EQ(packed->get_size_in_bytes(), numbers.size() * sizeof(i32));

    EXPECT_THAT(*packed, ::testing::ElementsAre(0, 10, 20, 30, 40, 50));
    EXPECT_EQ(dst.space(), 0u);
  }
}

}  // namespace
