#include <llfs/slot_sequencer.hpp>
//
#include <llfs/slot_sequencer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/async/task.hpp>

#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <numeric>
#include <vector>

namespace {

using namespace llfs::int_types;

// Test Plan:
//  1. Default construct, verify non-blocking accessor methods.
//  2. set_current/get_current
//  3. Call in order: get_next(), poll_prev(), set_current(), await_prev()/poll_prev()
//  4. await_prev()/set_current() in concurrent tasks
//  5. await_prev()/set_current() in concurrent tasks, using Fake executor
//  6. Make chain of 7 sequencers, verify expected behavior in all permutations of set_current.
//

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST(SlotSequencerTest, DefaultConstruct)
{
  llfs::SlotSequencer seq;

  EXPECT_EQ(seq.poll_prev(), llfs::SlotRange{});
  EXPECT_EQ(seq.get_current(), llfs::None);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST(SlotSequencerTest, SetGetCurrent)
{
  llfs::SlotSequencer seq;

  EXPECT_TRUE(seq.set_current(llfs::SlotRange{7, 11}));
  EXPECT_EQ(seq.get_current(), (llfs::SlotRange{7, 11}));

  EXPECT_FALSE(seq.set_current(llfs::SlotRange{37, 99}));
  EXPECT_EQ(seq.get_current(), (llfs::SlotRange{7, 11}));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST(SlotSequencerTest, SimpleSequence)
{
  llfs::SlotSequencer seq1;
  llfs::SlotSequencer seq2 = seq1.get_next();

  EXPECT_EQ(seq2.poll_prev(), llfs::None);

  EXPECT_TRUE(seq1.set_current(llfs::SlotRange{88, 999}));

  ASSERT_TRUE(seq2.await_prev().ok());
  EXPECT_EQ(*seq2.await_prev(), (llfs::SlotRange{88, 999}));
  EXPECT_EQ(seq2.poll_prev(), (llfs::Optional<llfs::SlotRange>{llfs::SlotRange{88, 999}}));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST(SlotSequencerTest, SimpleSequenceCopy)
{
  llfs::SlotSequencer seq1;
  llfs::SlotSequencer seq1_copy = seq1;
  llfs::SlotSequencer seq2 = seq1.get_next();

  EXPECT_EQ(seq2.poll_prev(), llfs::None);
  EXPECT_TRUE(seq1_copy.set_current(llfs::SlotRange{88, 999}));

  ASSERT_TRUE(seq2.await_prev().ok());
  EXPECT_EQ(*seq2.await_prev(), (llfs::SlotRange{88, 999}));
  EXPECT_EQ(seq2.poll_prev(), (llfs::Optional<llfs::SlotRange>{llfs::SlotRange{88, 999}}));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+--------------
TEST(SlotSequencerTest, Permutations)
{
  std::vector<usize> order(8);
  std::iota(order.begin(), order.end(), 0);
  do {
    std::vector<llfs::SlotSequencer> seq;
    seq.emplace_back();
    for (usize i = 0; i < 7; ++i) {
      seq.emplace_back(seq.back().get_next());
    }
    boost::asio::io_context io;
    std::vector<std::unique_ptr<batt::Task>> tasks;

    for (usize n : order) {
      tasks.emplace_back(std::make_unique<batt::Task>(io.get_executor(), [n, &seq] {
        llfs::StatusOr<llfs::SlotRange> prev = seq[n].await_prev();
        BATT_CHECK(prev.ok());
        BATT_CHECK(seq[n].set_current(llfs::SlotRange{prev->upper_bound, prev->upper_bound + 1}));
      }));
    }

    io.run();

    for (auto& p_task : tasks) {
      p_task->join();
    }

    for (usize i = 0; i < seq.size(); ++i) {
      EXPECT_EQ(seq[i].get_current(), llfs::make_optional(llfs::SlotRange{i, i + 1}));
    }
  } while (std::next_permutation(order.begin(), order.end()));
}

}  // namespace
