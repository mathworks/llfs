//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_pointer.hpp>
//
#include <llfs/packed_pointer.hpp>

#include <llfs/data_packer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace llfs::int_types;

struct PackedTarget {
  llfs::little_u32 value;

  auto debug_dump(const void*) const
  {
    return [this](std::ostream& out) {
      out << "PackedTarget{" << this->value << "}";
    };
  }
};

inline batt::Status validate_packed_value(const PackedTarget& target, const void* buffer_data,
                                          usize buffer_size)
{
  return llfs::validate_packed_struct(target, buffer_data, buffer_size);
}

struct PtrAndTarget {
  llfs::PackedPointer<PackedTarget> ptr;
  PackedTarget target;
};

struct TwoPtrsOneTarget {
  llfs::PackedPointer<PackedTarget> ptr_a;
  llfs::PackedPointer<PackedTarget> ptr_b;
  PackedTarget target;
};

struct TwoPtrsAndTargets {
  llfs::PackedPointer<PackedTarget> ptr_a;
  PackedTarget target_a;
  llfs::PackedPointer<PackedTarget> ptr_b;
  PackedTarget target_b;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, ResetUnsafeAndGet)
{
  PtrAndTarget layout{};
  layout.target.value = 42;
  layout.ptr.reset_unsafe(&layout.target);

  EXPECT_TRUE(static_cast<bool>(layout.ptr));
  EXPECT_EQ(layout.ptr.get(), &layout.target);
  EXPECT_EQ(layout.ptr->value, 42u);
  EXPECT_EQ((*layout.ptr).value, 42u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, GetRawAddress)
{
  PtrAndTarget layout{};
  layout.target.value = 99;
  layout.ptr.reset_unsafe(&layout.target);

  EXPECT_EQ(layout.ptr.get_raw_address(), static_cast<const void*>(&layout.target));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, ResetWithDataPacker)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  auto* ptr_slot = packer.pack_record<llfs::PackedPointer<PackedTarget>>();
  ASSERT_NE(ptr_slot, nullptr);

  auto* target = packer.pack_record<PackedTarget>();
  ASSERT_NE(target, nullptr);
  target->value = 123;

  ptr_slot->reset(target, &packer);

  EXPECT_TRUE(static_cast<bool>(*ptr_slot));
  EXPECT_EQ(ptr_slot->get(), target);
  EXPECT_EQ(ptr_slot->get()->value, 123u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, BoolOperatorFalseWhenZero)
{
  PtrAndTarget layout{};

  EXPECT_FALSE(static_cast<bool>(layout.ptr));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, NullptrEqualityOperators)
{
  PtrAndTarget layout{};

  EXPECT_TRUE(layout.ptr == nullptr);
  EXPECT_TRUE(nullptr == layout.ptr);
  EXPECT_FALSE(layout.ptr != nullptr);
  EXPECT_FALSE(nullptr != layout.ptr);

  layout.ptr.reset_unsafe(&layout.target);

  EXPECT_FALSE(layout.ptr == nullptr);
  EXPECT_FALSE(nullptr == layout.ptr);
  EXPECT_TRUE(layout.ptr != nullptr);
  EXPECT_TRUE(nullptr != layout.ptr);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, PointerEqualityOperators)
{
  TwoPtrsOneTarget layout{};
  layout.ptr_a.reset_unsafe(&layout.target);
  layout.ptr_b.reset_unsafe(&layout.target);

  EXPECT_TRUE(layout.ptr_a == layout.ptr_b);
  EXPECT_FALSE(layout.ptr_a != layout.ptr_b);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, PointerInequalityOperators)
{
  TwoPtrsAndTargets layout{};
  layout.target_a.value = 1;
  layout.target_b.value = 2;
  layout.ptr_a.reset_unsafe(&layout.target_a);
  layout.ptr_b.reset_unsafe(&layout.target_b);

  EXPECT_FALSE(layout.ptr_a == layout.ptr_b);
  EXPECT_TRUE(layout.ptr_a != layout.ptr_b);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, ValidatePackedValueInBounds)
{
  PtrAndTarget layout{};
  layout.target.value = 7;
  layout.ptr.reset_unsafe(&layout.target);

  batt::Status status = validate_packed_value(layout.ptr, &layout, sizeof(layout));
  EXPECT_TRUE(status.ok());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedPointerTest, ValidatePackedValueOutOfBounds)
{
  PtrAndTarget layout{};
  layout.ptr.reset_unsafe(&layout.target);

  batt::Status status =
      validate_packed_value(layout.ptr, &layout, sizeof(llfs::PackedPointer<PackedTarget>));
  EXPECT_FALSE(status.ok());
}

}  // namespace
