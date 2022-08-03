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

#include <batteries/state_machine_model.hpp>

#include <boost/functional/hash.hpp>
#include <boost/operators.hpp>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

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

class MockDriver
{
 public:
  MOCK_METHOD(const llfs::LogBlockCalculator&, calculate, (), (const));

  MOCK_METHOD(llfs::slot_offset_type, get_trim_pos, (), (const));

  MOCK_METHOD(llfs::slot_offset_type, get_flush_pos, (), (const));

  MOCK_METHOD(llfs::slot_offset_type, get_commit_pos, (), (const));

  MOCK_METHOD(llfs::ConstBuffer, get_data, (llfs::slot_offset_type slot_offset), (const));

  MOCK_METHOD(std::string_view, name, (), (const));

  MOCK_METHOD(usize, index_of_flush_op,
              (const llfs::BasicIoRingLogFlushOp<::testing::StrictMock<MockDriver>>& flush_op),
              (const));
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

    // Driver constants (test params).
    //
    const usize queue_depth = this->pick_one_of({1, 2, 4});

    const usize pages_per_block = this->pick_one_of({1, 2, 4});

    const usize block_size = llfs::kLogPageSize * pages_per_block;

    const usize block_capacity = block_size - 64;

    const usize log_size = this->pick_one_of({
        queue_depth * block_capacity - llfs::kLogPageSize,
        queue_depth * block_capacity,
        queue_depth * block_capacity * 2,
        queue_depth * block_capacity * 3,
        queue_depth * block_capacity * 4,
    });

    const i64 base_file_offset = this->pick_one_of({0, 7777 * 512});

    LLFS_LOG_INFO() << BATT_INSPECT(queue_depth) << BATT_INSPECT(block_size)
                    << BATT_INSPECT(block_capacity) << BATT_INSPECT(log_size)
                    << BATT_INSPECT(base_file_offset);

    const llfs::LogBlockCalculator calculate{
        llfs::IoRingLogConfig{
            .logical_size = log_size,
            .physical_offset = base_file_offset,
            .physical_size =
                llfs::LogBlockCalculator::disk_size_required_for_log_size(log_size, block_size)},
        llfs::IoRingLogDriverOptions::with_default_values().set_queue_depth(queue_depth)};

    // Driver variables (state).
    //
    // llfs::slot_offset_type trim_pos = 0;
    // llfs::slot_offset_type flush_pos = 0;
    llfs::slot_offset_type commit_pos = 0;

    // Create and configure the driver mock.
    //
    ::testing::StrictMock<MockDriver> driver;

    EXPECT_CALL(driver, calculate())  //
        .WillRepeatedly(::testing::ReturnRef(calculate));

    EXPECT_CALL(driver, get_commit_pos())  //
        .WillRepeatedly(::testing::ReturnPointee(&commit_pos));

    // Instantiate the system-under-test.
    //
    llfs::Optional<llfs::BasicIoRingLogFlushOp<::testing::StrictMock<MockDriver>>> op;
    op.emplace();
    op->initialize(&driver);

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
  IoRingLogFlushOpState state_;
};

TEST(IoRingLogFlushOpTest, StateMachineSimulation)
{
  IoRingLogFlushOpModel model;

  IoRingLogFlushOpModel::Result result = model.check_model();
  EXPECT_TRUE(result.ok);
}

}  // namespace
