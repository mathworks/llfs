//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_FLUSH_OP_TEST_HPP
#define LLFS_IORING_LOG_FLUSH_OP_TEST_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/ioring_log_flush_op.ipp>
#include <llfs/log_block_calculator.hpp>
#include <llfs/optional.hpp>
#include <llfs/ring_buffer.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/require.hpp>

#include <functional>
#include <string_view>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class MockIoRingLogDriver
{
 public:
  using Self = MockIoRingLogDriver;

  // The default string returned by Self::name();
  //
  static std::string_view default_name()
  {
    return "TheFakeDriver";
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MOCK_METHOD(const LogBlockCalculator&, calculate, (), (const));

  MOCK_METHOD(slot_offset_type, get_trim_pos, (), (const));

  MOCK_METHOD(slot_offset_type, get_flush_pos, (), (const));

  MOCK_METHOD(slot_offset_type, get_commit_pos, (), (const));

  MOCK_METHOD(ConstBuffer, get_data, (slot_offset_type slot_offset), (const));

  MOCK_METHOD(std::string_view, name, (), (const));

  MOCK_METHOD(usize, index_of_flush_op,
              (const BasicIoRingLogFlushOp<::testing::StrictMock<Self>>* flush_op), (const));

  MOCK_METHOD(void, async_write_some,
              (i64 log_offset, const ConstBuffer& data, i32 buf_index,
               std::function<void(StatusOr<i32>)> handler),
              ());

  MOCK_METHOD(void, wait_for_commit, (slot_offset_type least_upper_bound), ());

  MOCK_METHOD(void, poll_flush_state, (), ());

  MOCK_METHOD(void, update_durable_trim_pos, (slot_offset_type pos), ());

  MOCK_METHOD(slot_offset_type, get_durable_trim_pos, (), (const));
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Maintains a simulated on-disk log device plus in-memory ring buffer for testing.
//
class FakeLogState
{
 public:
  static const u8 kUninitializedMemoryByte = 0xfe;
  static const u8 kUninitializedDiskByte = 0xba;

  static Status verify_log_data(slot_offset_type slot_offset, ConstBuffer actual)
  {
    u64 expected_value = slot_offset / sizeof(u64);
    const little_u64* next_actual = (const little_u64*)(actual.data());
    for (usize i = 0; i < actual.size(); i += sizeof(u64), ++next_actual, ++expected_value) {
      if (i + sizeof(u64) > actual.size()) {
        little_u64 tmp_new = *next_actual;
        little_u64 tmp_old = expected_value;
        usize valid_bytes = (actual.size() - i);
        BATT_CHECK_EQ(valid_bytes, actual.size() % sizeof(u64));
        std::memcpy(&tmp_new, &tmp_old, valid_bytes);
        expected_value = tmp_new;
      }
      if (expected_value != *next_actual) {
        LLFS_LOG_ERROR() << "The data is wrong at slot_offset=" << (i + slot_offset)
                         << BATT_INSPECT(actual.size()) << BATT_INSPECT(expected_value)
                         << BATT_INSPECT(*next_actual) << std::hex << BATT_INSPECT(expected_value)
                         << std::hex << BATT_INSPECT(*next_actual);
        return batt::StatusCode::kDataLoss;
      }
    }
    return batt::OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Create a fake log with the specified configuration.
  //
  explicit FakeLogState(const llfs::LogBlockCalculator& calculate, ConstBuffer fake_data,
                        RingBuffer& ring_buffer) noexcept
      : calculate_{calculate}
      , trim_pos_{0}
      , flush_pos_{0}
      , commit_pos_{0}
      , fake_data_{fake_data}
      , ring_buffer_{ring_buffer}
      , disk_(this->calculate_.physical_size(), kUninitializedDiskByte)
  {
    BATT_CHECK_EQ(this->calculate_.begin_file_offset() % llfs::kLogAtomicWriteSize, 0);
    BATT_CHECK_EQ(this->calculate_.end_file_offset() % llfs::kLogAtomicWriteSize, 0);

    MutableBuffer dst = this->ring_buffer_.get_mut(0);
    std::memset(dst.data(), kUninitializedMemoryByte, dst.size());
  }

  u64 bytes_available() const
  {
    BATT_CHECK(!slot_less_than(this->commit_pos(), this->trim_pos()));

    return this->commit_pos() - this->trim_pos();
  }

  u64 space() const
  {
    return this->calculate_.logical_size() - this->bytes_available();
  }

  slot_offset_type trim_pos() const
  {
    return this->trim_pos_;
  }

  slot_offset_type flush_pos() const
  {
    return this->flush_pos_;
  }

  slot_offset_type commit_pos() const
  {
    return this->commit_pos_;
  }

  void trim_to_slot(slot_offset_type slot_lower_bound)
  {
    BATT_CHECK(slot_less_or_equal(slot_lower_bound, this->commit_pos_))
        << "Log trimmed beyond commit_pos!" << BATT_INSPECT(slot_lower_bound)
        << BATT_INSPECT(this->commit_pos_) << BATT_INSPECT(this->trim_pos_);

    this->trim_pos_ = slot_lower_bound;
  }

  void commit_to_slot(slot_offset_type slot_upper_bound)
  {
    BATT_CHECK_LT(this->commit_pos_ - this->trim_pos_, this->calculate_.logical_size())
        << "Log grown beyond capacity!" << BATT_INSPECT(this->trim_pos_)
        << BATT_INSPECT(this->commit_pos_) << BATT_INSPECT(slot_upper_bound);

    BATT_CHECK_GE(slot_upper_bound, this->commit_pos_) << "Commit pos must not go backwards!";
    {
      const usize n_to_commit = std::max(this->commit_pos_, slot_upper_bound) - this->commit_pos_;
      MutableBuffer dst = this->ring_buffer_.get_mut(this->commit_pos_);
      if (n_to_commit <= dst.size()) {
        std::memcpy(dst.data(), (this->fake_data_ + this->commit_pos_).data(), n_to_commit);
      } else {
        BATT_PANIC() << "gcc can't figure out that the condition above is impossible...";
        BATT_UNREACHABLE();
      }
    }
    this->commit_pos_ = slot_upper_bound;
  }

  ConstBuffer get_data(slot_offset_type offset) const
  {
    return this->ring_buffer_.get(offset);
  }

  Status write_data(i64 file_offset, ConstBuffer src, std::function<bool()> inject_block_failure,
                    usize flush_op_index)
  {
    const i64 end_file_offset = file_offset + BATT_CHECKED_CAST(i64, src.size());

    // Make sure the entire range of written data is within the range of valid disk blocks.
    //
    if (file_offset < this->calculate_.begin_file_offset() ||
        end_file_offset > this->calculate_.end_file_offset()) {
      LLFS_LOG_ERROR() << "[FakeLogState::write_data] Requested write range [" << file_offset
                       << ", " << end_file_offset
                       << ") is outside the valid log file offset range: ["
                       << this->calculate_.begin_file_offset() << ", "
                       << this->calculate_.end_file_offset() << ")";

      return batt::StatusCode::kOutOfRange;
    }

    // Check alignment requirements for direct I/O.
    //
    if ((file_offset % llfs::kLogAtomicWriteSize) != 0 ||
        (src.size() % llfs::kLogAtomicWriteSize) != 0 ||
        ((std::intptr_t)src.data() % llfs::kLogAtomicWriteSize) != 0) {
      LLFS_LOG_ERROR() << "[FakeLogState::write_data] Start/size of `src` isn't aligned to "
                       << llfs::kLogAtomicWriteSize << "-byte boundary";

      return batt::StatusCode::kInvalidArgument;
    }

    // Verify the write.
    //
    Optional<slot_offset_type> updated_flush_pos = None;
    {
      const llfs::LogBlockCalculator::PhysicalFileOffset block_file_offset =
          this->calculate_.block_start_file_offset_from(
              llfs::LogBlockCalculator::PhysicalFileOffset{file_offset});

      if (block_file_offset == file_offset) {
        auto* header = (const llfs::PackedLogPageHeader*)src.data();

        if (src.size() < sizeof(llfs::PackedLogPageHeader)) {
          LLFS_LOG_ERROR() << "Write is too small to contain log page header!";
          return batt::StatusCode::kInvalidArgument;
        }

        if (src.size() > llfs::kLogAtomicWriteSize) {
          LLFS_LOG_ERROR() << "Write is too big to be atomic (header must be atomic)!";
          return batt::StatusCode::kInvalidArgument;
        }

        if (header->magic != llfs::PackedLogPageHeader::kMagic) {
          LLFS_LOG_ERROR() << "The log page header doesn't contain the magic number!";
          return batt::StatusCode::kInvalidArgument;
        }

        if (header->trim_pos > this->trim_pos_) {
          LLFS_LOG_ERROR() << "The log page header's trim_pos is too high!";
          return batt::StatusCode::kInvalidArgument;
        }

        if (header->flush_pos > this->flush_pos_) {
          LLFS_LOG_ERROR() << "The log page header's flush_pos is too high!";
          return batt::StatusCode::kInvalidArgument;
        }

        if (header->commit_pos > this->commit_pos_) {
          LLFS_LOG_ERROR() << "The log page header's commit_pos is too high!";
          return batt::StatusCode::kInvalidArgument;
        }

        if (block_file_offset != this->calculate_.block_start_file_offset_from(
                                     llfs::SlotLowerBoundAt{header->slot_offset})) {
          LLFS_LOG_ERROR() << "Log page header is being written to the wrong block!";
          return batt::StatusCode::kInvalidArgument;
        }

        if (header->commit_size == 0) {
          // commit_size == 0 can be valid, iff we are flushing the trim_pos; check that here
          //
          if (slot_greater_than(header->trim_pos, this->durable_trim_pos.get_value())) {
            LLFS_VLOG(1) << "Detected flush trim_pos: " << BATT_INSPECT(header->trim_pos)
                         << BATT_INSPECT(this->durable_trim_pos.get_value());
          } else {
            LLFS_LOG_ERROR() << "Flush op wrote page with commit_size 0!";
            return batt::StatusCode::kInvalidArgument;
          }
        }

        //----- --- -- -  -  -   -
        // If we are writing the header, then the entire block must be correct.  Reconstruct it here
        // and verify.
        //----- --- -- -  -  -   -

        // Grab the payload section of the head page (src).
        //
        const ConstBuffer head_payload =
            resize_buffer(src, std::min(usize{header->commit_size}, llfs::kLogAtomicWriteSize)) +
            sizeof(llfs::PackedLogPageHeader);

        Status head_payload_match = verify_log_data(header->slot_offset, head_payload);
        BATT_REQUIRE_OK(head_payload_match);

        // If the committed portion of the block extends beyond the initial page, then compare the
        // on-disk data with the expected data within that range.
        //
        if (header->commit_size > llfs::kLogAtomicWriteSize - sizeof(llfs::PackedLogPageHeader)) {
          const ConstBuffer block_on_disk{
              this->disk_.data() + (block_file_offset - this->calculate_.begin_file_offset()),
              header->commit_size};
          const ConstBuffer tail_payload = block_on_disk + llfs::kLogAtomicWriteSize;
          const slot_offset_type tail_slot_offset =
              header->slot_offset + llfs::kLogAtomicWriteSize - sizeof(llfs::PackedLogPageHeader);

          Status tail_payload_match = verify_log_data(tail_slot_offset, tail_payload);
          BATT_REQUIRE_OK(tail_payload_match);
        }

        updated_flush_pos = header->slot_offset + header->commit_size;

        LLFS_VLOG(1) << "The write looks OK!" << BATT_INSPECT(updated_flush_pos);
      }
    }

    // Step through the write one atomic block at a time, and allow the caller to inject block
    // failures.
    //
    MutableBuffer dst{this->disk_.data() + (file_offset - this->calculate_.begin_file_offset()),
                      src.size()};

    bool no_failures = true;

    while (dst.size() > 0) {
      BATT_CHECK_GE(src.size(), llfs::kLogAtomicWriteSize);
      if (!inject_block_failure()) {
        std::memcpy(dst.data(), src.data(), llfs::kLogAtomicWriteSize);
      } else {
        no_failures = false;
      }
      dst += llfs::kLogAtomicWriteSize;
      src += llfs::kLogAtomicWriteSize;
    }

    //----- --- -- -  -  -   -
    BATT_SUPPRESS_IF_GCC("-Wmaybe-uninitialized")
    //
    if (no_failures && updated_flush_pos) {
      this->flush_pos_ = *updated_flush_pos;
      LLFS_VLOG(1) << BATT_INSPECT(updated_flush_pos);
    }
    //
    BATT_UNSUPPRESS_IF_GCC()
    //----- --- -- -  -  -   -

    LLFS_VLOG(1) << BATT_INSPECT((int)no_failures);

    return batt::OkStatus();
  }

  batt::Watch<slot_offset_type> durable_trim_pos{0};

 private:
  LogBlockCalculator calculate_;
  slot_offset_type trim_pos_;
  slot_offset_type flush_pos_;
  slot_offset_type commit_pos_;
  ConstBuffer fake_data_;
  RingBuffer& ring_buffer_;
  std::vector<u8> disk_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class ExpectedFlushOpState
{
 public:
  struct PendingWrite {
    i64 file_offset;
    ConstBuffer buffer;
    i32 buf_index;
    std::function<void(StatusOr<i32>)> handler;
  };

  using FlushOp = BasicIoRingLogFlushOp<::testing::StrictMock<MockIoRingLogDriver>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ::testing::StrictMock<MockIoRingLogDriver>& driver;
  usize op_index;
  slot_offset_type slot_offset;
  u64 commit_size;
  i64 file_offset;
  slot_offset_type trim_pos;
  slot_offset_type flush_pos;
  slot_offset_type commit_pos;

  Optional<SlotUpperBoundAt> waiting_for_commit;
  Optional<PendingWrite> pending_write;

  Status async_write_some_status = OkStatus();
  Status wait_for_commit_status = OkStatus();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit ExpectedFlushOpState(::testing::StrictMock<MockIoRingLogDriver>& mock_driver,
                                usize op_index) noexcept
      : driver{mock_driver}
      , op_index{op_index}
      , slot_offset{0}
      , commit_size{0}
      , file_offset{0}
      , trim_pos{0}
      , flush_pos{0}
      , commit_pos{0}
      , pending_write{None}
  {
  }

  Status verify(const FlushOp& actual) const
  {
    const PackedLogPageHeader* const header = actual.get_header();

    BATT_REQUIRE_EQ(PackedLogPageHeader::kMagic, header->magic);
    BATT_REQUIRE_EQ(this->slot_offset, header->slot_offset);
    if (this->commit_size == 0 || header->commit_size != 0) {
      BATT_REQUIRE_EQ(this->commit_size, header->commit_size);
    }
    BATT_REQUIRE_EQ(this->trim_pos, header->trim_pos);
    BATT_REQUIRE_EQ(this->flush_pos, header->flush_pos);
    BATT_REQUIRE_EQ(this->commit_pos, header->commit_pos);

    return OkStatus();
  }

  void on_initialize(FakeLogState& fake_log)
  {
    const auto block_capacity = this->driver.calculate().block_capacity();

    // Start at the beginning of the log.
    //
    this->slot_offset = this->op_index * block_capacity;

    // Calculate the slot range of the block to which data is currently being committed.
    //
    SlotRange active_range = this->driver.calculate().block_slot_range_from(
        llfs::SlotLowerBoundAt{this->driver.get_flush_pos()});

    // Calculate the total log space covered by all flush ops.
    //
    const u64 window_size = block_capacity * this->driver.calculate().queue_depth();

    // Step the slot_offset forward until we catch up to where the log is.
    //
    while (this->slot_offset + block_capacity <= active_range.lower_bound) {
      this->slot_offset += window_size;
    }
    this->commit_size = 0;

    if (slot_less_than(this->slot_offset, this->driver.get_flush_pos())) {
      LLFS_VLOG(1) << "EXPECT: get_data() - recover flushed data";

      EXPECT_CALL(this->driver, get_data(this->slot_offset))  //
          .WillOnce(::testing::Invoke([&fake_log](slot_offset_type requested_offset) {
            LLFS_VLOG(1) << "MockDriver::get_data()";
            return fake_log.get_data(requested_offset);
          }));

      this->commit_size = std::min(fake_log.flush_pos() - this->slot_offset,
                                   this->driver.calculate().block_capacity());
    }

    this->trim_pos = this->driver.get_trim_pos();
    this->flush_pos = this->driver.get_flush_pos();
    this->commit_pos = this->driver.get_commit_pos();
  }

  Status on_activate(FakeLogState& fake_log)
  {
    BATT_REQUIRE_EQ(this->waiting_for_commit, None);
    BATT_REQUIRE_EQ(this->pending_write, None);

    if (slot_less_than(this->slot_offset + this->commit_size, this->driver.get_commit_pos())) {
      this->expect_fill_buffer(fake_log);
      BATT_REQUIRE_OK(this->expect_async_write_some());
    } else {
      BATT_REQUIRE_OK(this->expect_wait_for_commit());
    }

    return OkStatus();
  }

  Status on_wait_for_commit(slot_offset_type least_upper_bound)
  {
    LLFS_VLOG(1) << "MockDriver::wait_for_commit(" << least_upper_bound << ")";

    BATT_REQUIRE_FALSE(this->waiting_for_commit);
    BATT_REQUIRE_FALSE(this->pending_write);

    this->waiting_for_commit.emplace(SlotUpperBoundAt{
        .offset = least_upper_bound,
    });

    return OkStatus();
  }

  Status on_complete_wait_for_commit(FakeLogState& fake_log)
  {
    BATT_REQUIRE_EQ(bool{this->waiting_for_commit}, true);
    BATT_REQUIRE_EQ(this->pending_write, None);
    BATT_REQUIRE_GE(this->driver.get_commit_pos(), this->waiting_for_commit->offset);

    this->waiting_for_commit = None;

    this->expect_fill_buffer(fake_log);
    BATT_REQUIRE_OK(this->expect_async_write_some());

    return OkStatus();
  }

  Status on_async_write_some(i64 file_offset, ConstBuffer buffer, i32 buf_index,
                             std::function<void(StatusOr<i32>)> handler)
  {
    LLFS_VLOG(1) << "MockDriver::async_write_some(file_offset=" << file_offset << ", buffer=["
                 << buffer.size() << "], buf_index=" << buf_index << ")";

    BATT_REQUIRE_FALSE(this->waiting_for_commit);
    BATT_REQUIRE_FALSE(this->pending_write);

    this->pending_write.emplace(PendingWrite{file_offset, buffer, buf_index, handler});

    return OkStatus();
  }

  Status on_complete_async_write(StatusOr<i32> result, FakeLogState& fake_log)
  {
    BATT_REQUIRE_TRUE(this->pending_write);
    BATT_REQUIRE_FALSE(this->waiting_for_commit);

    auto local_handler = std::move(this->pending_write->handler);
    this->pending_write = None;

    // If the result is a simulated fatal failure, then don't expect any further activity from
    // the op.
    //
    if (!result.ok() && !batt::status_is_retryable(result.status())) {
      local_handler(result);
      return OkStatus();
    }

    const bool flush_complete =
        !slot_less_than(fake_log.flush_pos(), this->slot_offset + this->commit_size);

    const bool block_was_full = this->commit_size == this->driver.calculate().block_capacity();

    if (flush_complete && block_was_full) {
      this->slot_offset +=
          (this->driver.calculate().block_capacity() * this->driver.calculate().queue_depth());
      this->commit_size = 0;
      LLFS_VLOG(1) << "EVENT: Block completely flushed; advancing window"
                   << BATT_INSPECT(this->slot_offset);
    }

    const bool data_available =
        (this->commit_size < this->driver.calculate().block_capacity()) &&
        slot_less_than(this->slot_offset + this->commit_size, fake_log.commit_pos());

    if (flush_complete) {
      this->expect_poll_flush_state();

      if (data_available) {
        this->expect_fill_buffer(fake_log);
        BATT_REQUIRE_OK(this->expect_async_write_some());
      } else {
        BATT_REQUIRE_OK(this->expect_wait_for_commit());
      }
    } else {
      if (data_available) {
        this->expect_fill_buffer(fake_log);
      }
      BATT_REQUIRE_OK(this->expect_async_write_some());
    }

    local_handler(result);

    return OkStatus();
  }

  void expect_fill_buffer(FakeLogState& fake_log)
  {
    LLFS_VLOG(1) << "EXPECT: fill_buffer()";

    const slot_offset_type old_commit_pos = this->slot_offset + this->commit_size;

    EXPECT_CALL(this->driver, get_data(old_commit_pos))  //
        .WillOnce(::testing::Invoke([&fake_log](slot_offset_type requested_offset) {
          LLFS_VLOG(1) << "MockDriver::get_data()";
          return fake_log.get_data(requested_offset);
        }));

    this->commit_size = std::min(fake_log.commit_pos() - this->slot_offset,
                                 this->driver.calculate().block_capacity());

    this->trim_pos = fake_log.trim_pos();
    this->flush_pos = fake_log.flush_pos();
    this->commit_pos = fake_log.commit_pos();
  }

  void expect_poll_flush_state()
  {
    LLFS_VLOG(1) << "EXPECT: poll_flush_state()";

    EXPECT_CALL(this->driver, poll_flush_state())  //
        .WillOnce(::testing::Invoke([] {
          LLFS_VLOG(1) << "MockDriver::poll_flush_state()";
        }));
  }

  Status expect_async_write_some()
  {
    LLFS_VLOG(1) << "EXPECT: async_write_some()";

    BATT_REQUIRE_FALSE(this->pending_write);
    BATT_REQUIRE_FALSE(slot_less_than(this->driver.get_commit_pos(), this->slot_offset))
        << "If the driver commit_pos is less than the starting slot range for this op, then "
           "the op "
           "should not be writing anything.";

    const i64 op_file_offset =
        this->driver.calculate().block_start_file_offset_from(SlotLowerBoundAt{this->slot_offset});

    // Expect the op to copy up to the commit_pos (but not more than the block capacity).
    //
    this->commit_size = std::min<u64>(this->driver.get_commit_pos() - this->slot_offset,
                                      this->driver.calculate().block_capacity());

    EXPECT_CALL(this->driver,
                async_write_some(
                    ::testing::AllOf(
                        ::testing::Ge(op_file_offset),
                        ::testing::Lt(op_file_offset +
                                      BATT_CHECKED_CAST(i64, sizeof(llfs::PackedLogPageHeader) +
                                                                 this->commit_size))),
                    ::testing::Truly([&](ConstBuffer actual_data) {
                      return actual_data.size() > 0;
                    }),
                    /*buf_index=*/op_index, ::testing::_))
        .WillOnce(::testing::Invoke([this](auto&&... args) {
          this->async_write_some_status.Update(this->on_async_write_some(BATT_FORWARD(args)...));
        }));

    return OkStatus();
  }

  Status expect_wait_for_commit()
  {
    LLFS_VLOG(1) << "EXPECT: wait_for_commit()";

    BATT_REQUIRE_FALSE(this->waiting_for_commit);

    EXPECT_CALL(this->driver, wait_for_commit(this->slot_offset + this->commit_size + 1))  //
        .WillOnce(::testing::Invoke([this](slot_offset_type least_upper_bound) {
          this->wait_for_commit_status.Update(this->on_wait_for_commit(least_upper_bound));
        }));

    return OkStatus();
  }
};  // namespace llfs

}  // namespace llfs

#endif  // LLFS_IORING_LOG_FLUSH_OP_TEST_HPP
