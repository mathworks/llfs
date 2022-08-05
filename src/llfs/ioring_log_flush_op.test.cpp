//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/ioring_log_flush_op.ipp>
//
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/ioring_log_flush_op.ipp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/config.hpp>
#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>
#include <llfs/logging.hpp>
#include <llfs/ring_buffer.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/state_machine_model.hpp>

#include <boost/functional/hash.hpp>
#include <boost/operators.hpp>

#include <vector>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

using llfs::ConstBuffer;
using llfs::MutableBuffer;
using llfs::None;
using llfs::Optional;
using llfs::slot_offset_type;
using llfs::Status;
using llfs::StatusOr;

// Test Plan:
//
//  Parameters:
//   Number of Ops: 1, 2, 4
//   Block Size (pages): 1, 2, 4
//   Driver recovery state: 0 (brand new), >0 at Op, >0 before Op, >0 after Op
//   Commit increments (bytes):
//      - primes (37, 109, 263, 317, 509, 701, 997)
//      - powers of 2 (8, 64, 256, 512)
//      - mixed (all of the above)
//   Commit size variation: true|false
//   Log size (* flush ops * block capacity): smaller (one page less), equal, 2x, 3x, 4x
//   Number of writes until device failure: 0...

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct IoRingLogFlushOpState : boost::equality_comparable<IoRingLogFlushOpState> {
  struct Hash {
    usize operator()(const IoRingLogFlushOpState& s) const
    {
      return s.done;
    }
  };

  friend bool operator==(const IoRingLogFlushOpState& l, const IoRingLogFlushOpState& r)
  {
    return l.done == r.done;
  }

  bool is_terminal() const
  {
    return this->done;
  }

  bool done = false;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class MockDriver
{
 public:
  MOCK_METHOD(const llfs::LogBlockCalculator&, calculate, (), (const));

  MOCK_METHOD(slot_offset_type, get_trim_pos, (), (const));

  MOCK_METHOD(slot_offset_type, get_flush_pos, (), (const));

  MOCK_METHOD(slot_offset_type, get_commit_pos, (), (const));

  MOCK_METHOD(ConstBuffer, get_data, (slot_offset_type slot_offset), (const));

  MOCK_METHOD(std::string_view, name, (), (const));

  MOCK_METHOD(usize, index_of_flush_op,
              (const llfs::BasicIoRingLogFlushOp<::testing::StrictMock<MockDriver>>* flush_op),
              (const));

  MOCK_METHOD(void, async_write_some,
              (i64 log_offset, const ConstBuffer& data, i32 buf_index,
               std::function<void(StatusOr<i32>)> handler),
              ());

  MOCK_METHOD(void, wait_for_commit, (slot_offset_type least_upper_bound), ());

  MOCK_METHOD(void, poll_flush_state, (), ());
};

// Returned by MockDriver::name();
//
const std::string_view kFakeDriverName = "TheFakeDriver";

const u8 kUninitializedMemoryByte = 0xfe;
const u8 kUninitializedDiskByte = 0xba;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Maintains a simulated on-disk log device plus in-memory ring buffer for testing.
//
class FakeLogState
{
 public:
  // Create a fake log with the specified configuration.
  //
  explicit FakeLogState(const llfs::LogBlockCalculator& calculate) noexcept
      : calculate_{calculate}
      , trim_pos_{0}
      , flush_pos_{0}
      , commit_pos_{0}
      , ring_buffer_{llfs::RingBuffer::TempFile{
            .byte_size = this->calculate_.logical_size(),
        }}
      , disk_(this->calculate_.physical_size(), kUninitializedDiskByte)
  {
    BATT_CHECK_EQ(this->calculate_.begin_file_offset() % llfs::kLogAtomicWriteSize, 0);
    BATT_CHECK_EQ(this->calculate_.end_file_offset() % llfs::kLogAtomicWriteSize, 0);

    MutableBuffer dst = this->ring_buffer_.get_mut(0);
    std::memset(dst.data(), kUninitializedMemoryByte, dst.size());
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
    BATT_CHECK_LE(slot_lower_bound, this->commit_pos_)
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
      const usize skew = this->commit_pos_ % sizeof(little_u64);
      std::vector<u8> src(n_to_commit + sizeof(little_u64) * 2);
      {
        u64 value = (this->commit_pos_ - skew) / sizeof(u64);
        for (usize i = 0; i < src.size(); i += sizeof(little_u64), ++value) {
          *((little_u64*)(src.data() + i)) = value;
        }
      }
      MutableBuffer dst = this->ring_buffer_.get_mut(this->commit_pos_);
      if (n_to_commit <= dst.size()) {
        std::memcpy(dst.data(), src.data() + skew, n_to_commit);
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

        // If we are writing the header, then the entire block must be correct.  Reconstruct it here
        // and verify.
        //
        std::vector<u8> expected(this->calculate_.block_size(), kUninitializedMemoryByte);

        std::memcpy(expected.data(),
                    this->disk_.data() + (block_file_offset - this->calculate_.begin_file_offset()),
                    expected.size());

        std::memcpy(expected.data() + (file_offset - block_file_offset), src.data(), src.size());

        const auto* full_block_header = (const llfs::PackedLogPageHeader*)expected.data();
        if (full_block_header->commit_size > this->calculate_.block_capacity()) {
          LLFS_LOG_ERROR() << "The commit size is too large!";
          return batt::StatusCode::kInternal;
        }

        u64 expected_value = full_block_header->slot_offset / sizeof(u64);
        const little_u64* next_actual = (const little_u64*)(full_block_header + 1);
        for (usize i = 0; i < full_block_header->commit_size;
             i += sizeof(u64), ++next_actual, ++expected_value) {
          if (i + sizeof(u64) > full_block_header->commit_size) {
            little_u64 tmp_new = *next_actual;
            little_u64 tmp_old = expected_value;
            usize valid_bytes = (full_block_header->commit_size - i);
            BATT_CHECK_EQ(valid_bytes, full_block_header->commit_size % sizeof(u64));
            std::memcpy(&tmp_new, &tmp_old, valid_bytes);
            expected_value = tmp_new;
          }
          if (expected_value != *next_actual) {
            LLFS_LOG_ERROR() << "The data is wrong at slot_offset="
                             << (i + full_block_header->slot_offset) << BATT_INSPECT(expected_value)
                             << BATT_INSPECT(*next_actual) << std::hex
                             << BATT_INSPECT(expected_value) << std::hex
                             << BATT_INSPECT(*next_actual);
            return batt::StatusCode::kDataLoss;
          }
        }

        updated_flush_pos = header->slot_offset + header->commit_size;

        LLFS_VLOG(1) << "The write looks OK!";
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
    }
    //
    BATT_UNSUPPRESS_IF_GCC()
    //----- --- -- -  -  -   -

    return batt::OkStatus();
  }

 private:
  llfs::LogBlockCalculator calculate_;
  slot_offset_type trim_pos_;
  slot_offset_type flush_pos_;
  slot_offset_type commit_pos_;
  llfs::RingBuffer ring_buffer_;
  std::vector<u8> disk_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoRingLogFlushOpModel
    : public batt::StateMachineModel<IoRingLogFlushOpState, IoRingLogFlushOpState::Hash>
{
 public:
  IoRingLogFlushOpState initialize() override
  {
    return IoRingLogFlushOpState{};
  }

  void enter_state(const IoRingLogFlushOpState& s) override
  {
    this->state_ = s;
  }

  void step() override
  {
    if (this->state_.is_terminal()) {
      return;
    }
    auto on_scope_exit = batt::finally([&] {
      this->state_.done = true;
    });

    // Driver constants (test params).
    //
    const usize queue_depth = this->pick_one_of({1, 2, 4});

    // The op-under-test is assigned to be either the first, last, or exact middle op in the
    // queue.
    //
    const usize op_index = [&]() -> usize {
      if (queue_depth == 0) {
        return 0;
      }
      if (queue_depth <= 2) {
        return this->pick_int(0, 1);
      }
      switch (this->pick_int(0, 2)) {
        case 0:
          return 0;

        case 1:
          return (queue_depth + 1) / 2;

        case 2:
          return queue_depth - 1;

        default:
          break;
      }
      BATT_PANIC() << "out of range!";
      BATT_UNREACHABLE();
    }();

    const usize pages_per_block = this->pick_one_of({1, 2, 4});

    const usize pages_per_block_log2 = batt::log2_ceil(pages_per_block);
    ASSERT_EQ(1ull << pages_per_block_log2, pages_per_block);

    const usize block_size = llfs::kLogPageSize * pages_per_block;
    const usize block_capacity = block_size - sizeof(llfs::PackedLogPageHeader);

    // Pick the log size; include the case where there are more flush ops than log capacity, so
    // long as it doesn't lead to a "negative" log size.
    //
    const usize log_size = [&] {
      if (queue_depth > 1 && block_capacity > llfs::kLogPageSize && this->pick_branch()) {
        return queue_depth * block_capacity - llfs::kLogPageSize;
      } else {
        return this->pick_one_of({
            queue_depth * block_capacity,
            queue_depth * block_capacity * 2,
            queue_depth * block_capacity * 3,
            queue_depth * block_capacity * 4,
        });
      }
    }();

    // Start the log device blocks at file_offset=0 or some irregular large-ish value.
    //
    const i64 base_file_offset = this->pick_one_of({0ull, 7777ull * llfs::kLogAtomicWriteSize});

    // We use the built-in calculator for the physical layout size.
    //
    const u64 physical_size =
        llfs::LogBlockCalculator::disk_size_required_for_log_size(log_size, block_size);

    LLFS_VLOG(1) << "\n\n==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -\n"
                 << BATT_INSPECT(queue_depth) << BATT_INSPECT(op_index) << BATT_INSPECT(block_size)
                 << BATT_INSPECT(block_capacity) << BATT_INSPECT(log_size)
                 << BATT_INSPECT(base_file_offset) << BATT_INSPECT(physical_size);

    // Now the calculator has everything it needs.
    //
    const llfs::LogBlockCalculator calculate{llfs::IoRingLogConfig{
                                                 .logical_size = log_size,
                                                 .physical_offset = base_file_offset,
                                                 .physical_size = physical_size,
                                                 .pages_per_block_log2 = pages_per_block_log2,
                                             },
                                             llfs::IoRingLogDriverOptions::with_default_values()  //
                                                 .set_queue_depth(queue_depth)};

    // Driver variables (state).
    //
    FakeLogState fake_log{calculate};

    // Create and configure the driver mock.
    //
    llfs::Optional<llfs::BasicIoRingLogFlushOp<::testing::StrictMock<MockDriver>>> op;
    ::testing::StrictMock<MockDriver> driver;

    EXPECT_CALL(driver, calculate())  //
        .WillRepeatedly(::testing::ReturnRef(calculate));

    EXPECT_CALL(driver, get_trim_pos())  //
        .WillRepeatedly(::testing::Invoke([&fake_log] {
          return fake_log.trim_pos();
        }));

    EXPECT_CALL(driver, get_flush_pos())  //
        .WillRepeatedly(::testing::Invoke([&fake_log] {
          return fake_log.flush_pos();
        }));

    EXPECT_CALL(driver, get_commit_pos())  //
        .WillRepeatedly(::testing::Invoke([&fake_log] {
          return fake_log.commit_pos();
        }));

    EXPECT_CALL(driver, name())  //
        .WillRepeatedly(::testing::Return(kFakeDriverName));

    EXPECT_CALL(driver, index_of_flush_op(&*op))  //
        .WillRepeatedly(::testing::Return(op_index));

    // Instantiate the system-under-test.
    //
    op.emplace();
    op->initialize(&driver);

    // The plan will be to simulate expected operating conditions of a real log.  We will simulate
    // committing data, expect that flush operations (writes) start (once the commit_pos is in the
    // op-under-test's active slot range), simulate the completion of the writes (full success,
    // partial (short) success, and failure), and in the case of failure, we will simulate a crash
    // by destructing the op and re-initializing as though we are in recovery.  The limit for
    // simulated log writes will be to wrap around the full capacity of the log twice.
    //
    const slot_offset_type max_flush_pos = log_size * 2 + 1;
    const usize max_failures = 2;

    usize failure_count = 0;
    slot_offset_type expected_wait_commit_pos = op_index * block_capacity + 1;
    {
      EXPECT_CALL(driver, wait_for_commit(expected_wait_commit_pos))  //
          .WillOnce(::testing::Return());

      op->activate();
    }
    Optional<slot_offset_type> actual_wait_commit_pos = expected_wait_commit_pos;
    std::function<void(StatusOr<i32>)> op_handler;

    slot_offset_type expected_slot_offset = op_index * block_capacity;
    slot_offset_type expected_commit_size = 0;
    i64 op_file_offset = base_file_offset + op_index * block_size;
    i32 last_write_size = 0;

    while (failure_count < max_failures && fake_log.flush_pos() < max_flush_pos) {
      //----- --- -- -  -  -   -
      // Save our "progress" variables so we can detect the end of simulation.
      //
      const slot_offset_type old_trim_pos = fake_log.trim_pos();
      const slot_offset_type old_flush_pos = fake_log.flush_pos();
      const slot_offset_type old_commit_pos = fake_log.commit_pos();
      const usize old_failure_count = failure_count;
      //----- --- -- -  -  -   -

      EXPECT_EQ(op->get_header()->slot_offset, expected_slot_offset);
      EXPECT_EQ(op->get_header()->commit_size, expected_commit_size);

      if (actual_wait_commit_pos != None) {
        //
        // If the op is waiting for the commit_pos to advance, there is no way to make progress
        // unless we simulate committing some data to the log.

        EXPECT_EQ(*actual_wait_commit_pos, expected_wait_commit_pos);
        actual_wait_commit_pos = None;

        EXPECT_FALSE(bool{op_handler}) << "There must only be one outstanding write op at a time!";

        // The options for simulated behavior are to commit the rest of the block (full_commit),
        // half of the remaining payload of the block (partial_commit), or more than the rest of the
        // block, overflowing into the next block (over_commit).
        //
        const slot_offset_type full_commit = expected_slot_offset + block_capacity;
        const slot_offset_type partial_commit = (*actual_wait_commit_pos + full_commit + 1) / 2;
        const slot_offset_type over_commit = full_commit + 1;

        // This prepares the driver mock to return updated values/data.
        //
        fake_log.commit_to_slot(this->pick_one_of({full_commit, partial_commit, over_commit}));
        LLFS_VLOG(1) << BATT_INSPECT(fake_log.commit_pos());

        // Set expectations for
        //
        EXPECT_CALL(driver, get_data(expected_slot_offset + expected_commit_size))  //
            .WillOnce(::testing::Invoke([&fake_log](slot_offset_type requested_offset) {
              return fake_log.get_data(requested_offset);
            }));

        const usize new_commit_size =
            std::min(block_capacity, fake_log.commit_pos() - expected_slot_offset);

        Status write_status;

        EXPECT_CALL(driver,
                    async_write_some(
                        ::testing::AllOf(
                            ::testing::Ge(op_file_offset),
                            ::testing::Lt(op_file_offset +
                                          BATT_CHECKED_CAST(i64, sizeof(llfs::PackedLogPageHeader) +
                                                                     new_commit_size))),
                        ::testing::Truly([&](ConstBuffer actual_data) {
                          return actual_data.size() > 0;  // TODO [tastolfi 2022-08-04]
                        }),
                        /*buf_index=*/op_index, ::testing::_))  //
            .WillOnce(                                          //
                ::testing::DoAll(                               //
                    ::testing::SaveArg<3>(&op_handler),         //
                    ::testing::Invoke([&](i64 file_offset, const ConstBuffer& data, int buf_index,
                                          auto&& /*handler*/) {
                      last_write_size = data.size();
                      write_status = fake_log.write_data(
                          file_offset, data, /*inject_failure=*/
                          [] {
                            return false;
                          },
                          op_index);
                    })));

        // We expect that the flush op will update its commit_size to reflect the new data.
        //
        expected_commit_size = new_commit_size;

        op->handle_commit(fake_log.commit_pos());

        ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);

        // (We're going to check this next time around the loop too, but we do it here to catch any
        // problems closer to when they occur.)
        //
        EXPECT_EQ(expected_commit_size, op->get_header()->commit_size);

        ASSERT_TRUE(bool{op_handler});
      } else {
        ASSERT_TRUE(bool{op_handler});

        op_handler(last_write_size);

        last_write_size = 0;
        op_handler = nullptr;

        // TODO [tastolfi 2022-08-04] assert ongoing write
      }

      // If we fail to make any kind of progress, end the simulation.
      //
      if (fake_log.trim_pos() == old_trim_pos         //
          && fake_log.flush_pos() == old_flush_pos    //
          && fake_log.commit_pos() == old_commit_pos  //
          && failure_count == old_failure_count) {
        break;
      }
    }
    /*
    this->do_one_of(
        [] {
          action1();
        },
        [] {
          action2();
        });
    */
  }

  IoRingLogFlushOpState leave_state() override
  {
    return this->state_;
  }

  bool check_invariants() override
  {
    return true;
  }

  IoRingLogFlushOpState normalize(const IoRingLogFlushOpState& s) override
  {
    return s;
  }

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRingLogFlushOpState state_;
};

TEST(IoRingLogFlushOpTest, StateMachineSimulation)
{
  IoRingLogFlushOpModel model;

  IoRingLogFlushOpModel::Result result = model.check_model();
  EXPECT_TRUE(result.ok);
}

}  // namespace
