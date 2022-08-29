//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_interval_map.hpp>
//
#include <llfs/slot_interval_map.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/checked_cast.hpp>

#include <array>
#include <random>
#include <vector>

namespace {

using namespace llfs::int_types;

TEST(SlotIntervalMapTest, RandomUpdates)
{
  constexpr usize kTestLogSize = 29;
  constexpr usize kLoopCount = 10000;
  constexpr usize kUpdateCount = 100;

  for (usize seed = 13; seed < kLoopCount; ++seed) {
    llfs::SlotIntervalMap actual_values;
    actual_values.update(llfs::OffsetRange{0, isize{kTestLogSize}}, 0u);

    std::array<llfs::slot_offset_type, kTestLogSize> expected_values;
    expected_values.fill(0);

    std::default_random_engine rng{seed};

    for (usize i = 0; i < kUpdateCount; ++i) {
      std::uniform_int_distribution<usize> pick_lower_bound{usize{0}, expected_values.size() - 1};
      const usize lower_bound = pick_lower_bound(rng);

      std::uniform_int_distribution<usize> pick_upper_bound{lower_bound + 1,
                                                            expected_values.size()};
      const usize upper_bound = pick_upper_bound(rng);

      std::uniform_int_distribution<llfs::slot_offset_type> pick_slot{0ull - kTestLogSize,
                                                                      kTestLogSize};
      const llfs::slot_offset_type new_slot = pick_slot(rng);

      for (usize j = lower_bound; j < upper_bound; ++j) {
        expected_values[j] = llfs::slot_max(new_slot, expected_values[j]);
      }

      const auto update_range = llfs::OffsetRange{BATT_CHECKED_CAST(isize, lower_bound),
                                                  BATT_CHECKED_CAST(isize, upper_bound)};

      LLFS_VLOG(1) << BATT_INSPECT(actual_values) << "; " << update_range << " => " << new_slot;

      actual_values.update(update_range, new_slot);

      const std::vector<llfs::slot_offset_type> to_verify = actual_values.to_vec();

      EXPECT_THAT(to_verify, ::testing::ElementsAreArray(expected_values));

      // Verify that adjacent ranges with the same slot are always combined.
      //
      llfs::Optional<llfs::SlotIntervalMap::Entry> prev_entry;
      ASSERT_NO_FATAL_FAILURE(actual_values.to_seq() |
                              llfs::seq::for_each([&](const llfs::SlotIntervalMap::Entry& entry) {
                                auto on_scope_exit = batt::finally([&] {
                                  prev_entry = entry;
                                });

                                if (prev_entry) {
                                  const bool adjacent_offsets =
                                      prev_entry->offset_range.upper_bound ==
                                      entry.offset_range.lower_bound;

                                  const bool same_key = prev_entry->slot == entry.slot;

                                  ASSERT_FALSE(adjacent_offsets && same_key)
                                      << BATT_INSPECT(prev_entry->offset_range)
                                      << BATT_INSPECT(entry.offset_range)
                                      << BATT_INSPECT(prev_entry->slot) << BATT_INSPECT(entry.slot)
                                      << BATT_INSPECT(actual_values);
                                }
                              }))
          << BATT_INSPECT(seed) << BATT_INSPECT(i);

      // Verify query() returns correct information.
      //
      for (const llfs::SlotIntervalMap::Entry& entry : actual_values.query(update_range)) {
        EXPECT_GE(entry.offset_range.lower_bound, update_range.lower_bound);
        EXPECT_LE(entry.offset_range.upper_bound, update_range.upper_bound);
        EXPECT_TRUE(llfs::slot_at_least(entry.slot, new_slot));
      }
      EXPECT_FALSE(actual_values.query(update_range).empty());
    }
  }
}

}  // namespace
