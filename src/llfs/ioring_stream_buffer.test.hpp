//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_STREAM_BUFFER_TEST_HPP
#define LLFS_IORING_STREAM_BUFFER_TEST_HPP

#include <llfs/ioring_stream_buffer.hpp>
#include <llfs/optional.hpp>

#include <batteries/async/dump_tasks.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>

namespace llfs {
namespace testing {

constexpr usize kTestQueueDepth = 16;
constexpr usize kTestNumThreads = 1;
constexpr usize kTestBufferCount = 4;
constexpr usize kTestBufferSize = 64;
constexpr usize kTestDataSize = 4096;
constexpr u32 kTestDataRandomSeed = 0;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoringStreamBufferTest : public ::testing::Test
{
 public:
  using Self = IoringStreamBufferTest;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static const std::array<u8, kTestDataSize>& test_data()
  {
    static std::array<u8, kTestDataSize> data;

    [[maybe_unused]] const bool initialized = [] {
      std::default_random_engine rng{kTestDataRandomSeed};
      std::uniform_int_distribution<u8> pick_byte{0x00, 0xff};

      for (u8& value : data) {
        value = pick_byte(rng);
      }

      return true;
    }();

    return data;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void SetUp() override
  {
    batt::enable_dump_tasks();

    StatusOr<ScopedIoRing> status_or_scoped_io_ring =
        ScopedIoRing::make_new(MaxQueueDepth{kTestQueueDepth}, ThreadPoolSize{kTestNumThreads});

    ASSERT_TRUE(status_or_scoped_io_ring.ok()) << BATT_INSPECT(status_or_scoped_io_ring.status());

    this->scoped_io_ring_ = std::move(*status_or_scoped_io_ring);
    this->io_ring_ = std::addressof(this->scoped_io_ring_.get_io_ring());

    StatusOr<std::unique_ptr<IoRingBufferPool>> status_or_buffer_pool =
        IoRingBufferPool::make_new(*this->io_ring_, this->buffer_count_, this->buffer_size_);

    ASSERT_TRUE(status_or_buffer_pool.ok()) << BATT_INSPECT(status_or_buffer_pool.status());

    this->buffer_pool_ = std::move(*status_or_buffer_pool);
  }

  void data_written(usize byte_count)
  {
    this->unwritten_data_ += byte_count;
    this->unverified_data_ = ConstBuffer{
        this->unverified_data_.data(),
        this->unverified_data_.size() + byte_count,
    };
  }

  void verify_data(const ConstBuffer& to_verify)
  {
    ASSERT_LE(to_verify.size(), this->unverified_data_.size());
    ASSERT_EQ(0, std::memcmp(to_verify.data(), this->unverified_data_.data(), to_verify.size()));

    this->unverified_data_ += to_verify.size();
  }

  void verify_data(const IoRingStreamBuffer::Fragment& fragment)
  {
    std::variant<llfs::IoRingBufferPool::Buffer, std::unique_ptr<llfs::u8[]>> storage;
    llfs::ConstBuffer to_verify = fragment.gather(storage);

    ASSERT_EQ(to_verify.size(), fragment.byte_size());

    this->verify_data(to_verify);
  }

  void run_blocking_test(const std::function<void()>& blocked_op,
                         const std::function<void()>& unblock_op)
  {
    boost::asio::io_context io;

    LLFS_VLOG(1) << "test entered";

    bool op_entered = false;

    batt::Task blocked_op_task{
        io.get_executor(),
        [&] {
          LLFS_VLOG(1) << "inside blocked_op_task";
          op_entered = true;
          blocked_op();

          LLFS_VLOG(1) << "leaving blocked_op_task";
        },
        "blocked_op_task",
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    LLFS_VLOG(1) << "calling poll the first time";

    io.poll();
    io.restart();

    EXPECT_TRUE(op_entered);
    EXPECT_FALSE(blocked_op_task.try_join());

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    LLFS_VLOG(1) << "unblocking the operation...";

    unblock_op();

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    LLFS_VLOG(1) << "calling poll the second time";

    io.poll();
    io.restart();

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    LLFS_VLOG(1) << "joining the consumer task";

    BATT_CHECK(blocked_op_task.try_join());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ScopedIoRing scoped_io_ring_;

  const IoRing* io_ring_ = nullptr;

  BufferCount buffer_count_{kTestBufferCount};

  BufferSize buffer_size_{kTestBufferSize};

  std::unique_ptr<IoRingBufferPool> buffer_pool_;

  ConstBuffer unwritten_data_{Self::test_data().data(), Self::test_data().size()};

  ConstBuffer unverified_data_{Self::test_data().data(), 0u};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoringStreamBufferEmptyTest : public IoringStreamBufferTest
{
 public:
  using Super = IoringStreamBufferTest;
  using Self = IoringStreamBufferEmptyTest;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void SetUp() override
  {
    ASSERT_NO_FATAL_FAILURE(Super::SetUp());
    ASSERT_NE(this->buffer_pool_, nullptr);

    this->stream_buffer_.emplace(*this->buffer_pool_);

    EXPECT_EQ(this->stream_buffer_->size(), 0u);
  }

  void commit_test_data(usize n_to_commit)
  {
    BATT_CHECK(this->stream_buffer_);
    BATT_CHECK_LE(n_to_commit, this->unwritten_data_.size());
    BATT_CHECK_LE(n_to_commit + this->stream_buffer_->size(), this->stream_buffer_->max_size());

    while (n_to_commit > 0) {
      // If there is no prepared buffer we can use, then allocate one now.
      //
      if (!this->prepared_view_) {
        StatusOr<IoRingMutableBufferView> view = this->stream_buffer_->prepare();

        ASSERT_TRUE(view.ok()) << BATT_INSPECT(view.status());

        this->prepared_view_.emplace(std::move(*view));
      }

      // Copy test data into the prepared buffer.
      //
      const usize n_to_copy = std::min(this->prepared_view_->size(), n_to_commit);
      std::memcpy(this->prepared_view_->data(), this->unwritten_data_.data(), n_to_copy);

      // Commit the data.
      //
      this->stream_buffer_->commit(this->prepared_view_->split(n_to_copy));

      // Update steam states.
      //
      n_to_commit -= n_to_copy;
      this->data_written(n_to_copy);
      if (this->prepared_view_->empty()) {
        this->prepared_view_ = None;
      }
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<IoRingStreamBuffer> stream_buffer_;

  Optional<IoRingMutableBufferView> prepared_view_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoringStreamBufferClosedEmptyTest : public IoringStreamBufferEmptyTest
{
 public:
  using Super = IoringStreamBufferEmptyTest;
  using Self = IoringStreamBufferClosedEmptyTest;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void SetUp() override
  {
    ASSERT_NO_FATAL_FAILURE(Super::SetUp());

    this->stream_buffer_->close();
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoringStreamBufferNotEmptyTest : public IoringStreamBufferEmptyTest
{
 public:
  using Super = IoringStreamBufferEmptyTest;
  using Self = IoringStreamBufferNotEmptyTest;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void SetUp() override
  {
    ASSERT_NO_FATAL_FAILURE(Super::SetUp());

    // Commit the test data in two steps, to exercise code paths that merge buffer views inside a
    // fragment.
    //
    this->commit_test_data(this->buffer_size_ - 1);
    this->commit_test_data(1);

    EXPECT_EQ(this->stream_buffer_->size(), this->buffer_size_);
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoringStreamBufferFullTest : public IoringStreamBufferEmptyTest
{
 public:
  using Super = IoringStreamBufferEmptyTest;
  using Self = IoringStreamBufferFullTest;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void SetUp() override
  {
    ASSERT_NO_FATAL_FAILURE(Super::SetUp());

    this->commit_test_data(this->stream_buffer_->max_size());

    EXPECT_EQ(this->stream_buffer_->size(), this->stream_buffer_->max_size());
  }
};

}  //namespace testing
}  //namespace llfs

#endif  // LLFS_IORING_STREAM_BUFFER_TEST_HPP
