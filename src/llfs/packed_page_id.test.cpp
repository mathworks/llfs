//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_page_id.hpp>
//
#include <llfs/packed_page_id.hpp>

#include <llfs/data_packer.hpp>
#include <llfs/data_reader.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sstream>

namespace {

using namespace llfs::int_types;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, FromAndAsPageId)
{
  const llfs::PageId original{0x123456789ABCDEF0ull};
  const llfs::PackedPageId packed = llfs::PackedPageId::from(original);

  EXPECT_EQ(packed.as_page_id(), original);
  EXPECT_EQ(packed.unpack(), original);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, GetPageIdFreeFunction)
{
  const llfs::PageId original{42};
  const llfs::PackedPageId packed = llfs::PackedPageId::from(original);

  EXPECT_EQ(llfs::get_page_id(packed), original);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, EqualityOperators)
{
  const llfs::PackedPageId a = llfs::PackedPageId::from(llfs::PageId{100});
  const llfs::PackedPageId b = llfs::PackedPageId::from(llfs::PageId{100});
  const llfs::PackedPageId c = llfs::PackedPageId::from(llfs::PageId{200});

  EXPECT_TRUE(a == b);
  EXPECT_FALSE(a != b);
  EXPECT_FALSE(a == c);
  EXPECT_TRUE(a != c);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, PackedSizeof)
{
  const llfs::PackedPageId packed = llfs::PackedPageId::from(llfs::PageId{0});

  EXPECT_EQ(llfs::packed_sizeof(packed), sizeof(llfs::PackedPageId));
  EXPECT_EQ(llfs::packed_sizeof(packed), 8u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, StreamOperator)
{
  const llfs::PageId original{0xFF};
  const llfs::PackedPageId packed = llfs::PackedPageId::from(original);

  std::ostringstream oss;
  oss << packed;

  std::ostringstream expected;
  expected << original;

  EXPECT_EQ(oss.str(), expected.str());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, PackObjectToFromPackedPageId)
{
  const llfs::PackedPageId src = llfs::PackedPageId::from(llfs::PageId{999});
  llfs::PackedPageId dst{};

  llfs::PackedPageId* result = pack_object_to(src, &dst, (llfs::DataPacker*)nullptr);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result, &dst);
  EXPECT_EQ(dst.as_page_id(), llfs::PageId{999});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, PackObjectToFromPageId)
{
  const llfs::PageId src{0xBEEF};
  llfs::PackedPageId dst{};

  llfs::PackedPageId* result = pack_object_to(src, &dst, (llfs::DataPacker*)nullptr);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result, &dst);
  EXPECT_EQ(dst.as_page_id(), src);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, UnpackObject)
{
  const llfs::PackedPageId packed = llfs::PackedPageId::from(llfs::PageId{12345});

  llfs::StatusOr<llfs::PageId> result = unpack_object(packed, (llfs::DataReader*)nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, llfs::PageId{12345});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, ValidatePackedValueInBounds)
{
  alignas(8) std::array<u8, 16> buffer;
  buffer.fill(0);

  auto* packed = reinterpret_cast<llfs::PackedPageId*>(buffer.data());
  packed->id_val = 42;

  batt::Status status = validate_packed_value(*packed, buffer.data(), buffer.size());
  EXPECT_TRUE(status.ok());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, ValidatePackedValueOutOfBounds)
{
  alignas(8) std::array<u8, 16> buffer;
  buffer.fill(0);

  auto* packed = reinterpret_cast<llfs::PackedPageId*>(buffer.data());
  packed->id_val = 42;

  batt::Status status = validate_packed_value(*packed, buffer.data() + 4, 4);
  EXPECT_FALSE(status.ok());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, TraceRefsPackedPageId)
{
  const llfs::PackedPageId packed = llfs::PackedPageId::from(llfs::PageId{777});

  llfs::BoxedSeq<llfs::PageId> refs = trace_refs(packed);

  llfs::Optional<llfs::PageId> first = refs.next();
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, llfs::PageId{777});

  llfs::Optional<llfs::PageId> second = refs.next();
  EXPECT_FALSE(second);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, TraceRefsPageId)
{
  const llfs::PageId page_id{888};

  llfs::BoxedSeq<llfs::PageId> refs = trace_refs(page_id);

  llfs::Optional<llfs::PageId> first = refs.next();
  ASSERT_TRUE(first);
  EXPECT_EQ(*first, llfs::PageId{888});

  llfs::Optional<llfs::PageId> second = refs.next();
  EXPECT_FALSE(second);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, ZeroPageId)
{
  const llfs::PackedPageId packed = llfs::PackedPageId::from(llfs::PageId{0});

  EXPECT_EQ(packed.as_page_id(), llfs::PageId{0});
  EXPECT_EQ(packed.id_val, 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPageIdTest, MaxPageId)
{
  const u64 max_val = std::numeric_limits<u64>::max();
  const llfs::PackedPageId packed = llfs::PackedPageId::from(llfs::PageId{max_val});

  EXPECT_EQ(packed.as_page_id(), llfs::PageId{max_val});
}

}  // namespace
