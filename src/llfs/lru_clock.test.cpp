//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/lru_clock.hpp>
//
#include <llfs/lru_clock.hpp>

#include <batteries/cpu_align.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <map>

namespace {

// Test Goals:
//  - local counts are independent on different threads
//  - local counts are monotonic on a given thread
//  - llfs::LRUClock::kMaxSyncDelayUsec is an upper bound on the time two threads' counters can be
//  out of sync
//
// Test Plan:
//  1. start N threads, update counts on each
//     - maintain list of which count values were seen on each thread
//     - verify local monotonicity, global independence
//  2. same as (1), but add at least one "slow" thread; verify that it jumps ahead after sleeping
//  for the max sync delay.
//
//

using namespace llfs::int_types;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1.
//
TEST(LruClockTest, PerThreadUpdate)
{
  const usize kNumThreads = std::thread::hardware_concurrency();
  const usize kUpdatesPerThread = 25 * 1000;

  std::vector<std::vector<i64>> per_thread_values(kNumThreads, std::vector<i64>(kUpdatesPerThread));

  std::atomic<bool> start{false};
  std::vector<std::thread> threads;

  for (usize i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([i, &start, &per_thread_values] {
      while (!start.load()) {
        continue;
      }
      for (usize j = 0; j < kUpdatesPerThread; ++j) {
        const i64 value = llfs::LRUClock::advance_local();
        per_thread_values[i][j] = value;
      }
    });
  }

  start.store(true);

  for (std::thread& t : threads) {
    t.join();
  }

  std::map<i64, usize> count_per_value;

  for (const std::vector<i64>& values : per_thread_values) {
    ASSERT_EQ(values.size(), kUpdatesPerThread);
    ++count_per_value[values[0]];
    for (usize i = 1; i < kUpdatesPerThread; ++i) {
      EXPECT_LT(values[i - 1], values[i]);
      ++count_per_value[values[i]];
    }
  }

  if (kNumThreads > 1) {
    usize repeated_values = 0;
    for (const auto& [value, count] : count_per_value) {
      if (count > 1) {
        ++repeated_values;
      }
    }

    EXPECT_GT(repeated_values, 0u);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2.
//
void run_sync_update_test(const usize kNumFastThreads)
{
  const usize kUpdatesPerThread = 50 * 1000 * 1000;

  // The maximum number of consecutive attempts on the slow thread to read an increasing value.
  //
  const usize kSyncDelayToleranceFactor = 100;

  // The "slot thread" sleeps for the maximum sync delay times the tolerance factor, then takes a
  // reading from its local counter.  All observed local counts are recorded for verification below.
  //
  std::atomic<bool> stop_slow_thread{false};
  std::vector<i64> slow_thread_values;
  std::thread slow_thread{[&slow_thread_values, &stop_slow_thread] {
    while (!stop_slow_thread.load()) {
      const i64 prev_value = slow_thread_values.empty() ? -1 : slow_thread_values.back();
      slow_thread_values.emplace_back();

      for (usize j = 0; j < kSyncDelayToleranceFactor; ++j) {
        std::this_thread::sleep_for(std::chrono::microseconds(llfs::LRUClock::kMaxSyncDelayUsec));

        // NOTE: we are only calling `read_local()` here, not `advance_local()`.  That means unless
        // the other (fast) threads are updating the counter via periodic global synchronization,
        // this value will not change!
        //
        slow_thread_values.back() = llfs::LRUClock::read_local();
        if (slow_thread_values.back() > prev_value) {
          break;
        }
      }
    }
  }};

  // The "fast threads" just advance their local counters as fast as possible.  For each fast
  // thread, we keep track of the maximum observed count value, which should eventually skip ahead
  // due to global sync operations.
  //
  std::vector<std::thread> fast_threads;
  std::vector<batt::CpuCacheLineIsolated<i64>> max_fast_thread_value(kNumFastThreads);

  for (usize i = 0; i < kNumFastThreads; ++i) {
    fast_threads.emplace_back([i, &max_fast_thread_value] {
      i64 last_value = -1;
      for (usize j = 0; j < kUpdatesPerThread; ++j) {
        const i64 value = llfs::LRUClock::advance_local();
        EXPECT_GT(value, last_value);
        *max_fast_thread_value[i] = std::max(*max_fast_thread_value[i], value);
      }
    });
  }

  // Wait for all threads to finish.
  //
  for (std::thread& t : fast_threads) {
    t.join();
  }

  stop_slow_thread.store(true);
  slow_thread.join();

  // Calculate the maximum count value observed from any of the fast threads.  Since all threads are
  // known to have finished, this value will not change.
  //
  i64 max_count = 0;
  for (batt::CpuCacheLineIsolated<i64>& count : max_fast_thread_value) {
    max_count = std::max(max_count, *count);
  }

  const i64 max_synced_count = llfs::LRUClock::read_global();
  EXPECT_EQ(max_synced_count, max_count + 1);

  // Verify the required properties of the slow thread's count observations...
  //
  {
    i64 prev_value = -1;

    for (usize i = 0; i < slow_thread_values.size(); ++i) {
      // Once we observe the maximum count, all future observations should be the same (since the
      // slow thread does not do any updating on its own).
      //
      if (slow_thread_values[i] >= max_synced_count) {
        for (; i < slow_thread_values.size(); ++i) {
          EXPECT_EQ(slow_thread_values[i], max_synced_count);
        }
        break;
      }

      // All other values should be strictly increasing.
      //
      EXPECT_GT(slow_thread_values[i], prev_value)
          << BATT_INSPECT(i) << BATT_INSPECT(slow_thread_values[i])
          << BATT_INSPECT(slow_thread_values[i - 1]) << BATT_INSPECT(max_synced_count)
          << BATT_INSPECT(max_count) << BATT_INSPECT_RANGE(slow_thread_values);

      prev_value = slow_thread_values[i];
    }
  }

  EXPECT_GT(slow_thread_values.back(), (kUpdatesPerThread * 95) / 100);
}

TEST(LruClockTest, SyncUpdate1)
{
  run_sync_update_test(1);
}
TEST(LruClockTest, SyncUpdate2)
{
  run_sync_update_test(2);
}
TEST(LruClockTest, SyncUpdate4)
{
  run_sync_update_test(4);
}
TEST(LruClockTest, SyncUpdate8)
{
  run_sync_update_test(8);
}
TEST(LruClockTest, SyncUpdate16)
{
  run_sync_update_test(16);
}
TEST(LruClockTest, SyncUpdate32)
{
  run_sync_update_test(32);
}
TEST(LruClockTest, SyncUpdate64)
{
  run_sync_update_test(64);
}
TEST(LruClockTest, SyncUpdate128)
{
  run_sync_update_test(128);
}

}  // namespace
