//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_buffer_view.hpp>
//
#include <llfs/ioring_buffer_view.hpp>

#include <llfs/ioring_buffer_view.test.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

// Test Plan:
//  1. Construct with empty buffer/slice.
//  2. Construct with non-empty buffer/slice.
//  3. can_merge_with
//     a. merge empty with empty, can_merge == true
//     b. merge empty with non-empty, can_merge == true
//     c. merge non-empty with empty, can_merge == true
//     d. merge non-empty with non-empty, can_merge == true
//     e. merge empty with empty, can_merge == false
//     f. merge empty with non-empty, can_merge == false
//     g. merge non-empty with empty, can_merge == false
//     h. merge non-empty with non-empty, can_merge == false
//  4. merge_with
//     a. merge empty with empty, can_merge == true
//     b. merge empty with non-empty, can_merge == true
//     c. merge non-empty with empty, can_merge == true
//     d. merge non-empty with non-empty, can_merge == true
//     e. merge empty with empty, can_merge == false
//     f. merge empty with non-empty, can_merge == false
//     g. merge non-empty with empty, can_merge == false
//     h. merge non-empty with non-empty, can_merge == false

using namespace llfs::int_types;

using llfs::testing::IoringBufferViewTest;

//+++++++++++-+-+--+----- --- -- -  -  -   -
//  1. Construct with empty buffer/slice.
//
TEST_F(IoringBufferViewTest, ConstructEmpty)
{
  llfs::IoRingConstBufferView view;

  EXPECT_EQ(view.slice.data(), nullptr);
  EXPECT_EQ(view.slice.size(), 0u);
  EXPECT_EQ(view.data(), nullptr);
  EXPECT_EQ(view.size(), 0u);

  EXPECT_FALSE(view.buffer);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  2. Construct with non-empty buffer/slice.
//
TEST_F(IoringBufferViewTest, ConstructNonEmpty)
{
  llfs::IoRingConstBufferView view{
      *this->buffer_1,
      this->buffer_1->get(),
  };

  EXPECT_NE(view.slice.data(), nullptr);
  EXPECT_EQ(view.slice.data(), this->buffer_1->data());
  EXPECT_EQ(view.slice.size(), kTestBufferSize);
  EXPECT_EQ(view.data(), this->buffer_1->data());
  EXPECT_EQ(view.size(), kTestBufferSize);

  EXPECT_TRUE(view.buffer);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     a. merge empty with empty, can_merge == true
//  4. merge_with
//     a. merge empty with empty, can_merge == true
//
TEST_F(IoringBufferViewTest, MergeEmptyWithEmptyTrue)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 0},
  };

  llfs::IoRingConstBufferView view_2 = view_1;

  EXPECT_TRUE(view_1.can_merge_with(view_2));
  EXPECT_TRUE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.slice.size(), 0u);
  EXPECT_EQ(view_1.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.size(), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     b. merge empty with non-empty, can_merge == true
//
//  4. merge_with
//     b. merge empty with non-empty, can_merge == true
//
TEST_F(IoringBufferViewTest, MergeEmptyWithNonEmptyTrue)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 0},
  };

  llfs::IoRingConstBufferView view_2{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100},
  };

  EXPECT_EQ(view_1.slice.size(), 0u);

  EXPECT_TRUE(view_1.can_merge_with(view_2));
  EXPECT_TRUE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.slice.size(), 100u);
  EXPECT_EQ(view_1.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.size(), 100u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     c. merge non-empty with empty, can_merge == true
//  4. merge_with
//     c. merge non-empty with empty, can_merge == true
//
TEST_F(IoringBufferViewTest, MergeNonEmptyWithEmptyTrue)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100},
  };

  llfs::IoRingConstBufferView view_2{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100} + 100,
  };

  EXPECT_EQ(view_1.slice.size(), 100u);
  EXPECT_EQ(view_2.slice.size(), 0u);
  EXPECT_EQ(view_1.size(), 100u);
  EXPECT_EQ(view_2.size(), 0u);

  EXPECT_TRUE(view_1.can_merge_with(view_2));
  EXPECT_TRUE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.slice.size(), 100u);
  EXPECT_EQ(view_1.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.size(), 100u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     d. merge non-empty with non-empty, can_merge == true
//  4. merge_with
//     d. merge non-empty with non-empty, can_merge == true
//
TEST_F(IoringBufferViewTest, MergeNonEmptyWithNonEmptyTrue)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100},
  };

  llfs::IoRingConstBufferView view_2{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 200} + 100,
  };

  EXPECT_EQ(view_1.slice.size(), 100u);
  EXPECT_EQ(view_1.size(), 100u);

  EXPECT_TRUE(view_1.can_merge_with(view_2));
  EXPECT_TRUE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.slice.size(), 200u);
  EXPECT_EQ(view_1.data(), this->buffer_1->data());
  EXPECT_EQ(view_1.size(), 200u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     e. merge empty with empty, can_merge == false
//  4. merge_with
//     e. merge empty with empty, can_merge == false
//
TEST_F(IoringBufferViewTest, MergeEmptyWithEmptyFalse)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100} + 100,
  };

  llfs::IoRingConstBufferView view_2{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 200} + 200,
  };

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.slice.size(), 0u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.slice.size(), 0u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.size(), 0u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.size(), 0u);

  EXPECT_FALSE(view_1.can_merge_with(view_2));
  EXPECT_FALSE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.slice.size(), 0u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.slice.size(), 0u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.size(), 0u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.size(), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     f. merge empty with non-empty, can_merge == false
//  4. merge_with
//     f. merge empty with non-empty, can_merge == false
//
TEST_F(IoringBufferViewTest, MergeEmptyWithNonEmptyFalse)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100} + 100,
  };

  llfs::IoRingConstBufferView view_2{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 200} + 150,
  };

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.slice.size(), 0u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.slice.size(), 50u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.size(), 0u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.size(), 50u);

  EXPECT_FALSE(view_1.can_merge_with(view_2));
  EXPECT_FALSE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.slice.size(), 0u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.slice.size(), 50u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
  EXPECT_EQ(view_1.size(), 0u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.size(), 50u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     g. merge non-empty with empty, can_merge == false
//  4. merge_with
//     g. merge non-empty with empty, can_merge == false
//
TEST_F(IoringBufferViewTest, MergeNonEmptyWithEmptyFalse)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100} + 50,
  };

  llfs::IoRingConstBufferView view_2{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 200} + 200,
  };

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.slice.size(), 50u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.slice.size(), 0u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.size(), 50u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.size(), 0u);

  EXPECT_FALSE(view_1.can_merge_with(view_2));
  EXPECT_FALSE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.slice.size(), 50u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.slice.size(), 0u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.size(), 50u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 200));
  EXPECT_EQ(view_2.size(), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. can_merge_with
//     h. merge non-empty with non-empty, can_merge == false
//  4. merge_with
//     h. merge non-empty with non-empty, can_merge == false
//
TEST_F(IoringBufferViewTest, MergeNonEmptyWithNonEmptyFalse)
{
  llfs::IoRingConstBufferView view_1{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100} + 50,
  };

  llfs::IoRingConstBufferView view_2{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 200} + 150,
  };

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.slice.size(), 50u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.slice.size(), 50u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.size(), 50u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.size(), 50u);

  EXPECT_FALSE(view_1.can_merge_with(view_2));
  EXPECT_FALSE(view_1.merge_with(view_2));

  EXPECT_EQ(view_1.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.slice.size(), 50u);
  EXPECT_EQ(view_2.slice.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.slice.size(), 50u);
  EXPECT_EQ(view_1.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
  EXPECT_EQ(view_1.size(), 50u);
  EXPECT_EQ(view_2.data(), llfs::advance_pointer(this->buffer_1->data(), 150));
  EXPECT_EQ(view_2.size(), 50u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// IoRingMutableBufferView => IoRingConstBufferView.
//
TEST_F(IoringBufferViewTest, ConstFromMutable)
{
  const llfs::IoRingMutableBufferView mutable_view{
      *this->buffer_1,
      batt::MutableBuffer{this->buffer_1->data(), 100},
  };

  EXPECT_NE(mutable_view.data(), nullptr);
  EXPECT_EQ(mutable_view.data(), this->buffer_1->data());
  EXPECT_EQ(mutable_view.size(), 100u);
  EXPECT_EQ(mutable_view.slice.data(), this->buffer_1->data());
  EXPECT_EQ(mutable_view.slice.size(), 100u);

  static_assert(std::is_same_v<decltype(mutable_view.data()), void*>);

  {
    const llfs::IoRingConstBufferView const_view = mutable_view;

    static_assert(std::is_same_v<decltype(const_view.data()), const void*>);

    EXPECT_EQ(const_view.data(), this->buffer_1->data());
    EXPECT_EQ(const_view.size(), 100u);
  }
  {
    llfs::IoRingConstBufferView const_view;
    const_view = mutable_view;

    EXPECT_EQ(const_view.data(), this->buffer_1->data());
    EXPECT_EQ(const_view.size(), 100u);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringBufferViewTest, Split)
{
  const llfs::IoRingConstBufferView non_empty_view{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 100} + 50,
  };

  const llfs::IoRingConstBufferView empty_view{
      *this->buffer_1,
      batt::ConstBuffer{this->buffer_1->data(), 0},
  };

  // split: empty -> at 0
  {
    llfs::IoRingConstBufferView view = empty_view;
    llfs::IoRingConstBufferView prefix = view.split(0);

    EXPECT_EQ(prefix.data(), this->buffer_1->data());
    EXPECT_EQ(prefix.size(), 0u);
    EXPECT_EQ(view.data(), this->buffer_1->data());
    EXPECT_EQ(view.size(), 0u);
  }

  // split: empty -> past 0
  {
    llfs::IoRingConstBufferView view = empty_view;
    llfs::IoRingConstBufferView prefix = view.split(1);

    EXPECT_EQ(prefix.data(), this->buffer_1->data());
    EXPECT_EQ(prefix.size(), 0u);
    EXPECT_EQ(view.data(), this->buffer_1->data());
    EXPECT_EQ(view.size(), 0u);
  }

  // split: non-empty -> at 0
  {
    llfs::IoRingConstBufferView view = non_empty_view;
    llfs::IoRingConstBufferView prefix = view.split(0);

    EXPECT_EQ(prefix.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
    EXPECT_EQ(prefix.size(), 0u);
    EXPECT_EQ(view.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
    EXPECT_EQ(view.size(), 50u);
  }

  // split: non-empty -> >0 <end
  {
    llfs::IoRingConstBufferView view = non_empty_view;
    llfs::IoRingConstBufferView prefix = view.split(10);

    EXPECT_EQ(prefix.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
    EXPECT_EQ(prefix.size(), 10u);
    EXPECT_EQ(view.data(), llfs::advance_pointer(this->buffer_1->data(), 60));
    EXPECT_EQ(view.size(), 40u);
  }

  // split: non-empty -> at end
  {
    llfs::IoRingConstBufferView view = non_empty_view;
    llfs::IoRingConstBufferView prefix = view.split(50);

    EXPECT_EQ(prefix.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
    EXPECT_EQ(prefix.size(), 50u);
    EXPECT_EQ(view.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
    EXPECT_EQ(view.size(), 0u);
  }

  // split: non-empty -> past end
  {
    llfs::IoRingConstBufferView view = non_empty_view;
    llfs::IoRingConstBufferView prefix = view.split(51);

    EXPECT_EQ(prefix.data(), llfs::advance_pointer(this->buffer_1->data(), 50));
    EXPECT_EQ(prefix.size(), 50u);
    EXPECT_EQ(view.data(), llfs::advance_pointer(this->buffer_1->data(), 100));
    EXPECT_EQ(view.size(), 0u);
  }
}

}  // namespace
