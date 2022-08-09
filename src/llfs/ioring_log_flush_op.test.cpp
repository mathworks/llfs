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

#include <llfs/ioring_log_flush_op.test.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/config.hpp>
#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>
#include <llfs/logging.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/state_machine_model.hpp>

#include <boost/functional/hash.hpp>
#include <boost/operators.hpp>

#include <vector>

namespace llfs {
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

}  // namespace llfs

namespace batt {

template <>
struct StateMachineRadixQueueSize<::llfs::IoRingLogFlushOpState> {
  static constexpr usize kValue = 512;
};

}  // namespace batt

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

using llfs::ConstBuffer;
using llfs::IoRingLogFlushOpState;
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
class IoRingLogFlushOpModel
    : public batt::StateMachineModel<IoRingLogFlushOpState, IoRingLogFlushOpState::Hash>
{
 public:
  using ActionFn = std::function<void()>;

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

    // Pick the params we will be using for this test.
    //
    this->pick_test_params();

    // Driver variables (state).
    //
    this->fake_log_.emplace(*this->calculate_);

    // Create and configure the driver mock.
    //
    this->initialize_mock_driver();

    // Instantiate the system-under-test.
    //
    this->op_.emplace();

    // Initialize and verify.
    //
    this->expected_state_->on_initialize();
    this->op_->initialize(&*this->driver_);

    ASSERT_NO_FATAL_FAILURE(this->expected_state_->verify(*this->op_));

    // Activate.
    //
    this->expected_state_->on_activate(*this->fake_log_);
    this->op_->activate();

    for (;;) {
      // Verify that the expected and actual flush op state match.
      //
      ASSERT_NO_FATAL_FAILURE(this->expected_state_->verify(*this->op_));

      std::vector<ActionFn> actions;
      this->generate_actions(&actions);
      LLFS_VLOG(1) << BATT_INSPECT(actions.size());

      // If there are no actions that can be taken, we are done!
      //
      if (actions.empty()) {
        break;
      }

      const usize i = this->pick_int(0, actions.size() - 1);
      ASSERT_NO_FATAL_FAILURE(actions[i]());
    }

    if (this->failure_count_ == 0) {
      BATT_CHECK_EQ(this->fake_log_->flush_pos(), this->max_flush_pos_);
    }
  }

  IoRingLogFlushOpState leave_state() override
  {
    this->op_ = None;
    this->driver_ = None;
    this->fake_log_ = None;
    this->calculate_ = None;

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
  //
  void pick_test_params()
  {
    this->queue_depth_ = this->pick_one_of({1, 2, 4});

    // The op-under-test is assigned to be either the first, last, or exact middle op in the
    // queue.
    //
    this->op_index_ = [&]() -> usize {
      if (this->queue_depth_ == 0) {
        return 0;
      }
      if (this->queue_depth_ <= 2) {
        return this->pick_int(0, 1);
      }
      switch (this->pick_int(0, 2)) {
        case 0:
          return 0;

        case 1:
          return (this->queue_depth_ + 1) / 2;

        case 2:
          return this->queue_depth_ - 1;

        default:
          break;
      }
      BATT_PANIC() << "out of range!";
      BATT_UNREACHABLE();
    }();

    this->pages_per_block_ = this->pick_one_of({1, 2, 4});

    this->pages_per_block_log2_ = batt::log2_ceil(this->pages_per_block_);
    ASSERT_EQ(1ull << this->pages_per_block_log2_, this->pages_per_block_);

    this->block_size_ = llfs::kLogPageSize * this->pages_per_block_;
    this->block_capacity_ = this->block_size_ - sizeof(llfs::PackedLogPageHeader);

    // Pick the log size; include the case where there are more flush ops than log capacity, so
    // long as it doesn't lead to a "negative" log size.
    //
    this->log_size_ = [&] {
      if (this->queue_depth_ > 1 && this->block_capacity_ > llfs::kLogPageSize &&
          this->pick_branch()) {
        return this->queue_depth_ * this->block_capacity_ - llfs::kLogPageSize;
      } else {
        return this->pick_one_of({
            this->queue_depth_ * this->block_capacity_,
            this->queue_depth_ * this->block_capacity_ * 2,
            this->queue_depth_ * this->block_capacity_ * 3,
            this->queue_depth_ * this->block_capacity_ * 4,
        });
      }
    }();

    this->max_flush_pos_ = (this->queue_depth_ + this->op_index_ + 1) * this->block_capacity_;

    // Start the log device blocks at file_offset=0 or some irregular large-ish value.
    //
    this->base_file_offset_ = this->pick_one_of({0ull, 7777ull * llfs::kLogAtomicWriteSize});

    // We use the built-in calculator for the physical layout size.
    //
    this->physical_size_ = llfs::LogBlockCalculator::disk_size_required_for_log_size(
        this->log_size_, this->block_size_);

    LLFS_VLOG(1) << "\n\n==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -\n"
                 << BATT_INSPECT(this->queue_depth_) << BATT_INSPECT(this->op_index_)
                 << BATT_INSPECT(this->block_size_) << BATT_INSPECT(this->block_capacity_)
                 << BATT_INSPECT(this->log_size_) << BATT_INSPECT(this->base_file_offset_)
                 << BATT_INSPECT(this->physical_size_);

    // Now the calculator has everything it needs.
    //
    this->calculate_.emplace(
        llfs::IoRingLogConfig{
            .logical_size = this->log_size_,
            .physical_offset = this->base_file_offset_,
            .physical_size = this->physical_size_,
            .pages_per_block_log2 = this->pages_per_block_log2_,
        },
        llfs::IoRingLogDriverOptions::with_default_values()  //
            .set_queue_depth(this->queue_depth_));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void initialize_mock_driver()
  {
    this->driver_.emplace();

    EXPECT_CALL(*this->driver_, calculate())  //
        .WillRepeatedly(::testing::ReturnRef(*this->calculate_));

    EXPECT_CALL(*this->driver_, get_trim_pos())  //
        .WillRepeatedly(::testing::Invoke([this] {
          BATT_CHECK(this->fake_log_);
          return this->fake_log_->trim_pos();
        }));

    EXPECT_CALL(*this->driver_, get_flush_pos())  //
        .WillRepeatedly(::testing::Invoke([this] {
          BATT_CHECK(this->fake_log_);
          return this->fake_log_->flush_pos();
        }));

    EXPECT_CALL(*this->driver_, get_commit_pos())  //
        .WillRepeatedly(::testing::Invoke([this] {
          BATT_CHECK(this->fake_log_);
          return this->fake_log_->commit_pos();
        }));

    EXPECT_CALL(*this->driver_, name())  //
        .WillRepeatedly(::testing::Return(llfs::MockIoRingLogDriver::default_name()));

    EXPECT_CALL(*this->driver_, index_of_flush_op(&*this->op_))  //
        .WillRepeatedly(::testing::Return(this->op_index_));

    //----- --- -- -  -  -   -

    // Create the expected flush op state.
    //
    this->expected_state_.emplace(*this->driver_, this->op_index_);

    // Reset failure state.
    //
    this->failure_count_ = 0;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void generate_actions(std::vector<ActionFn>* actions)
  {
    BATT_CHECK(this->fake_log_);
    BATT_CHECK(this->expected_state_);
    BATT_CHECK(this->op_);

    const llfs::slot_offset_type next_trim_pos =
        llfs::slot_min(this->fake_log_->commit_pos(), this->op_->get_header()->slot_offset);

    LLFS_VLOG(1) << "generate_actions() " << BATT_INSPECT(this->fake_log_->commit_pos())
                 << BATT_INSPECT(this->op_->get_header()->slot_offset)
                 << BATT_INSPECT(this->fake_log_->trim_pos()) << BATT_INSPECT(next_trim_pos);

    //----- --- -- -  -  -   -
    // Complete a write (if there is one pending).
    //
    if (this->expected_state_->pending_write) {
      actions->emplace_back([this] {
        LLFS_VLOG(1) << "ACTION: Complete Async Write";

        // TODO [tastolfi 2022-08-08] simulate failures here
        //
        Status write_status = this->fake_log_->write_data(
            this->expected_state_->pending_write->file_offset,
            this->expected_state_->pending_write->buffer, /*inject_block_failure=*/
            [] {
              return false;
            },
            this->expected_state_->pending_write->buf_index);

        ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);

        ASSERT_NO_FATAL_FAILURE(this->expected_state_->on_complete_async_write(
            StatusOr<i32>{this->expected_state_->pending_write->buffer.size()}, *this->fake_log_));
      });
    }

    //----- --- -- -  -  -   -
    // Commit more data (if we are below the max_flush_pos).
    //
    const usize min_to_commit = (this->block_capacity_ + 1) / 2;

    if (this->fake_log_->commit_pos() < this->max_flush_pos_ && this->fake_log_->space() > 0) {
      actions->emplace_back([this, min_to_commit] {
        LLFS_VLOG(1) << "ACTION: Commit Data";

        const usize max_to_commit = std::min(this->fake_log_->space(),
                                             this->max_flush_pos_ - this->fake_log_->commit_pos());

        const usize commit_unit_range = (max_to_commit + min_to_commit - 1) / min_to_commit;

        const usize size_to_commit =
            std::min(max_to_commit, min_to_commit * this->pick_int(1, commit_unit_range));

        const slot_offset_type new_commit_pos = this->fake_log_->commit_pos() + size_to_commit;

        LLFS_VLOG(1) << BATT_INSPECT(min_to_commit) << BATT_INSPECT(max_to_commit)
                     << BATT_INSPECT(commit_unit_range) << BATT_INSPECT(size_to_commit)
                     << BATT_INSPECT(new_commit_pos)
                     << BATT_INSPECT(this->expected_state_->waiting_for_commit);

        // This prepares the driver mock to return updated values/data.
        //
        this->fake_log_->commit_to_slot(new_commit_pos);

        // If this commit should wake up the flush op, do so.
        //
        if (this->expected_state_->waiting_for_commit &&
            llfs::slot_greater_or_equal(new_commit_pos,
                                        this->expected_state_->waiting_for_commit->offset)) {
          ASSERT_NO_FATAL_FAILURE(
              this->expected_state_->on_complete_wait_for_commit(*this->fake_log_));

          this->op_->handle_commit(new_commit_pos);
        }
      });
    }

    //----- --- -- -  -  -   -
    // Trim the log.
    //
    if (llfs::slot_less_than(this->fake_log_->trim_pos(), next_trim_pos)) {
      actions->emplace_back([this, next_trim_pos] {
        LLFS_VLOG(1) << "ACTION: Trim (" << this->fake_log_->trim_pos() << " -> " << next_trim_pos
                     << ")";

        this->fake_log_->trim_to_slot(next_trim_pos);
      });
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRingLogFlushOpState state_;

  // The number of simulated flush ops for the current test run.
  //
  usize queue_depth_ = 0;

  // The index of the op-under-test for the current test run.
  //
  usize op_index_ = 0;

  usize pages_per_block_ = 0;
  usize pages_per_block_log2_ = 0;
  usize block_size_ = 0;
  usize block_capacity_ = 0;
  usize log_size_ = 0;
  i64 base_file_offset_ = 0;
  u64 physical_size_ = 0;
  slot_offset_type max_flush_pos_ = 0;

  const usize max_failures_ = 2;
  usize failure_count_ = 0;

  Optional<llfs::LogBlockCalculator> calculate_;

  Optional<llfs::FakeLogState> fake_log_;

  Optional<llfs::BasicIoRingLogFlushOp<::testing::StrictMock<llfs::MockIoRingLogDriver>>> op_;

  Optional<::testing::StrictMock<llfs::MockIoRingLogDriver>> driver_;

  Optional<llfs::ExpectedFlushOpState> expected_state_;
};

TEST(IoRingLogFlushOpTest, StateMachineSimulation)
{
  IoRingLogFlushOpModel model;

  IoRingLogFlushOpModel::Result result = model.check_model();
  EXPECT_TRUE(result.ok);
}

}  // namespace
