//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_stream_buffer.hpp>
//
#include <llfs/ioring_stream_buffer.hpp>

#include <llfs/ioring_stream_buffer.test.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/asio/io_context.hpp>
#include <batteries/async/task.hpp>

namespace {

using namespace llfs::int_types;

// Test Plan:
//  Sizes:
//   1. Empty
//   2. Full
//   3. Low (contains <=1 buffer worth, >0 bytes)
//   4. High (contains >1 buffer worth, not full)
//  States:
//   a. open
//   b. closed
//   c. other task waiting on prepare
//   d. other task waiting on consume_some
//   e. other task waiting on consume(start, end)
//  Actions:
//   i. close
//   ii. prepare
//   iii. commit
//   iv. consume(start, end)
//   v. consume_some
//  Results:
//   (w. blocking until resolved into one of the following...)
//   x. success, no blocking
//   y. fail, error
//   z. fail, panic
//

// 1.a.i.w. invalid (close never blocks)
// 1.a.i.x.
// 1.a.i.y. invalid
// 1.a.i.z. invalid
// 1.a.ii.w.
// 1.a.ii.x.
// 1.a.ii.y. invalid
// 1.a.ii.z. invalid
// 1.a.iii.w. invalid (commit never blocks)
// 1.a.iii.x.
// 1.a.iii.y. invalid
// 1.a.iii.z.
// 1.a.iv.w.
// 1.a.iv.x. (end == start)
// 1.a.iv.y.
// 1.a.iv.z.
// 1.a.v.w.
// 1.a.v.x. (end == start)
// 1.a.v.y.
// 1.a.v.z. invalid

// 1.b.i.w. invalid (close never blocks)
// 1.b.i.x. invalid
// 1.b.i.y.
// 1.b.i.z. invalid
// 1.b.ii.w.
// 1.b.ii.x.
// 1.b.ii.y. invalid
// 1.b.ii.z. invalid
// 1.b.iii.w. invalid (commit never blocks)
// 1.b.iii.x.
// 1.b.iii.y. invalid
// 1.b.iii.z.
// 1.b.iv.w.
// 1.b.iv.x. (end == start)
// 1.b.iv.y.
// 1.b.iv.z.
// 1.b.v.w.
// 1.b.v.x. (end == start)
// 1.b.v.y.
// 1.b.v.z. invalid

using llfs::testing::IoringStreamBufferClosedEmptyTest;
using llfs::testing::IoringStreamBufferEmptyTest;
using llfs::testing::IoringStreamBufferFullTest;
using llfs::testing::IoringStreamBufferNotEmptyTest;
using llfs::testing::IoringStreamBufferTest;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferTest, CreateDestroy)
{
  llfs::IoRingStreamBuffer stream_buffer{*this->buffer_pool_};

  EXPECT_EQ(stream_buffer.buffer_size(), this->buffer_size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferTest, EmptyFragment)
{
  llfs::IoRingStreamBuffer::Fragment fragment;

  EXPECT_TRUE(fragment.empty());
  EXPECT_EQ(fragment.view_count(), 0u);
  EXPECT_EQ(fragment.byte_size(), 0u);

  std::variant<llfs::IoRingBufferPool::Buffer, std::unique_ptr<llfs::u8[]>> storage;
  llfs::ConstBuffer gathered = fragment.gather(storage);

  EXPECT_EQ(gathered.size(), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferNotEmptyTest, ConsumeSomeOkNoBlock)
{
  llfs::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment =
      this->stream_buffer_->consume_some();

  ASSERT_TRUE(fragment.ok()) << BATT_INSPECT(fragment.status());
  EXPECT_GT(fragment->byte_size(), 0u);
  EXPECT_EQ(fragment->byte_size(), this->unverified_data_.size());

  this->verify_data(*fragment);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferFullTest, ConsumeSomeOkNoBlockFull)
{
  llfs::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment =
      this->stream_buffer_->consume_some();

  ASSERT_TRUE(fragment.ok()) << BATT_INSPECT(fragment.status());
  EXPECT_GT(fragment->byte_size(), 0u);
  EXPECT_EQ(fragment->byte_size(), this->buffer_size_ * 2);
  EXPECT_EQ(fragment->byte_size(), this->unverified_data_.size());

  this->verify_data(*fragment);

  EXPECT_EQ(this->unverified_data_.size(), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferNotEmptyTest, ConsumeSomeOkNoBlockAfterClosed)
{
  this->stream_buffer_->close();

  llfs::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment =
      this->stream_buffer_->consume_some();

  ASSERT_TRUE(fragment.ok()) << BATT_INSPECT(fragment.status());
  EXPECT_GT(fragment->byte_size(), 0u);
  EXPECT_EQ(fragment->byte_size(), this->unverified_data_.size());

  this->verify_data(*fragment);

  llfs::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment2 =
      this->stream_buffer_->consume_some();

  EXPECT_EQ(fragment2.status(), batt::StatusCode::kClosed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferClosedEmptyTest, ConsumeSomeClosedNoBlock)
{
  llfs::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment =
      this->stream_buffer_->consume_some();

  EXPECT_EQ(fragment.status(), batt::StatusCode::kClosed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferEmptyTest, ConsumeSomeWaitClosed)
{
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment;

  this->run_blocking_test(
      //----- --- -- -  -  -   -
      /*blocked_op=*/
      [&] {
        fragment = this->stream_buffer_->consume_some();
      },
      //----- --- -- -  -  -   -
      /*unblock_op=*/
      [&] {
        EXPECT_EQ(fragment.status(), batt::StatusCode::kUnknown);

        this->stream_buffer_->close();
      });

  EXPECT_EQ(fragment.status(), batt::StatusCode::kClosed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferEmptyTest, ConsumeSomeWaitOk)
{
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment;

  this->run_blocking_test(
      //----- --- -- -  -  -   -
      /*blocked_op=*/
      [&] {
        fragment = this->stream_buffer_->consume_some();
      },
      //----- --- -- -  -  -   -
      /*unblock_op=*/
      [&] {
        EXPECT_EQ(fragment.status(), batt::StatusCode::kUnknown);

        LLFS_VLOG(1) << "committing some test data";

        this->commit_test_data(10);
      });

  ASSERT_TRUE(fragment.ok());
  EXPECT_EQ(fragment->byte_size(), 10);

  this->verify_data(*fragment);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferEmptyTest, ConsumeSomeWaitOkThenClose)
{
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment;

  this->run_blocking_test(
      //----- --- -- -  -  -   -
      /*blocked_op=*/
      [&] {
        fragment = this->stream_buffer_->consume_some();
      },
      //----- --- -- -  -  -   -
      /*unblock_op=*/
      [&] {
        EXPECT_EQ(fragment.status(), batt::StatusCode::kUnknown);

        LLFS_VLOG(1) << "committing some test data";

        this->commit_test_data(10);
        this->stream_buffer_->close();

        EXPECT_EQ(fragment.status(), batt::StatusCode::kUnknown);
      });

  ASSERT_TRUE(fragment.ok());
  EXPECT_EQ(fragment->byte_size(), 10);

  this->verify_data(*fragment);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  fragment = batt::Status{batt::StatusCode::kUnknown};
  fragment = this->stream_buffer_->consume_some();

  EXPECT_EQ(fragment.status(), batt::StatusCode::kClosed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferFullTest, ConsumeRangesInOrder)
{
  constexpr usize kSizePerConsume = 8;

  const usize n_to_consume = this->stream_buffer_->size();
  const usize n_iter = n_to_consume / kSizePerConsume;

  BATT_CHECK_EQ(n_iter * kSizePerConsume, n_to_consume);

  for (usize i = 0; i < n_iter; ++i) {
    if (i == n_iter / 2) {
      this->stream_buffer_->close();
    }

    batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment = this->stream_buffer_->consume(
        /*start=*/i * kSizePerConsume,  //
        /*end=*/(i + 1) * kSizePerConsume);

    ASSERT_TRUE(fragment.ok()) << BATT_INSPECT(fragment.status());

    this->verify_data(*fragment);
  }

  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment =
      this->stream_buffer_->consume(n_to_consume, n_to_consume + 1);

  EXPECT_EQ(fragment.status(), batt::StatusCode::kClosed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferNotEmptyTest, ConsumeRangeWaitOk)
{
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment1;
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment2;

  const i64 fragment1_start = 0;
  const i64 fragment1_end = this->stream_buffer_->size() / 2;

  const i64 fragment2_start = fragment1_end;
  const i64 fragment2_end = this->stream_buffer_->size();

  this->run_blocking_test(
      //----- --- -- -  -  -   -
      /*blocked_op=*/
      [&] {
        fragment2 = this->stream_buffer_->consume(fragment2_start, fragment2_end);
      },
      //----- --- -- -  -  -   -
      /*unblock_op=*/
      [&] {
        EXPECT_EQ(fragment1.status(), batt::StatusCode::kUnknown);
        EXPECT_EQ(fragment2.status(), batt::StatusCode::kUnknown);

        fragment1 = this->stream_buffer_->consume(fragment1_start, fragment1_end);
      });

  ASSERT_TRUE(fragment1.ok()) << BATT_INSPECT(fragment1.status());
  ASSERT_TRUE(fragment2.ok()) << BATT_INSPECT(fragment2.status());

  this->verify_data(*fragment1);
  this->verify_data(*fragment2);

  EXPECT_EQ(this->unverified_data_.size(), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferNotEmptyTest, ConsumeRangeWaitClosed)
{
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment1;
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment2;

  const i64 fragment1_start = 0;
  const i64 fragment1_end = this->stream_buffer_->size();

  const i64 fragment2_start = this->stream_buffer_->size();
  const i64 fragment2_end = this->stream_buffer_->size() + 1;

  this->run_blocking_test(
      //----- --- -- -  -  -   -
      /*blocked_op=*/
      [&] {
        fragment2 = this->stream_buffer_->consume(fragment2_start, fragment2_end);
      },
      //----- --- -- -  -  -   -
      /*unblock_op=*/
      [&] {
        EXPECT_EQ(fragment1.status(), batt::StatusCode::kUnknown);
        EXPECT_EQ(fragment2.status(), batt::StatusCode::kUnknown);

        this->stream_buffer_->close();
        fragment1 = this->stream_buffer_->consume(fragment1_start, fragment1_end);
      });

  EXPECT_EQ(fragment2.status(), batt::StatusCode::kClosed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferFullTest, PrepareWaitOk1)
{
  batt::StatusOr<llfs::IoRingStreamBuffer::PreparedView> view;

  this->run_blocking_test(
      //----- --- -- -  -  -   -
      /*blocked_op=*/
      [&] {
        view = this->stream_buffer_->prepare();
      },
      //----- --- -- -  -  -   -
      /*unblock_op=*/
      [&] {
        EXPECT_EQ(view.status(), batt::StatusCode::kUnknown);

        batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment =
            this->stream_buffer_->consume_some();

        ASSERT_TRUE(fragment.ok()) << BATT_INSPECT(fragment.status());

        this->verify_data(*fragment);

        EXPECT_EQ(this->unverified_data_.size(), 0u);
      });

  EXPECT_TRUE(view.ok()) << BATT_INSPECT(view.ok());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(IoringStreamBufferFullTest, PrepareWaitOk2)
{
  batt::StatusOr<llfs::IoRingStreamBuffer::Fragment> fragment;
  batt::StatusOr<llfs::IoRingStreamBuffer::PreparedView> view;

  this->run_blocking_test(
      //----- --- -- -  -  -   -
      /*blocked_op=*/
      [&] {
        fragment = this->stream_buffer_->consume_some();

        ASSERT_TRUE(fragment.ok()) << BATT_INSPECT(fragment.status());
        EXPECT_EQ(fragment->byte_size(), this->stream_buffer_->max_size());

        this->verify_data(*fragment);

        EXPECT_EQ(this->unverified_data_.size(), 0u);
        EXPECT_EQ(this->stream_buffer_->size(), 0u);

        LLFS_VLOG(1) << "About to call prepare (this should block)...";

        view = this->stream_buffer_->prepare();
      },
      //----- --- -- -  -  -   -
      /*unblock_op=*/
      [&] {
        EXPECT_EQ(view.status(), batt::StatusCode::kUnknown);
        EXPECT_TRUE(fragment.ok());
        EXPECT_FALSE(this->prepared_view_);

        LLFS_VLOG(1) << "Setting fragment to unknown status (this should free the Fragment)";

        fragment = batt::StatusCode::kUnknown;

        LLFS_VLOG(1) << "Leaving the unblock op...";
      });

  EXPECT_TRUE(view.ok()) << BATT_INSPECT(view.ok());
}

}  // namespace
