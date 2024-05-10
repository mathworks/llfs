//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume.hpp>
//
#include <llfs/volume.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/testing/test_config.hpp>

#include <llfs/storage_simulation.hpp>

#include <random>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

using llfs::StatusOr;

TEST(VolumeMultiAppendTest, SimTest)
{
  const std::string nonterminal(197, 'a');
  const std::string terminal(193, 'b');

  llfs::testing::TestConfig test_config;

  const u32 kNumSeeds = test_config.extra_testing() ? 100 * 1000 : 400;

  for (u32 seed = 0; seed < kNumSeeds; ++seed) {
    for (usize n_before_crash : {1, 2, 3, 5, 10, 100, 500, 1000, 2000}) {
      LLFS_VLOG(1) << BATT_INSPECT(seed) << BATT_INSPECT(n_before_crash);
      std::mt19937 rng{seed};

      llfs::StorageSimulation sim{batt::StateMachineEntropySource{
          /*entropy_fn=*/[&rng](usize min_value, usize max_value) -> usize {
            std::uniform_int_distribution<usize> pick_value{min_value, max_value};
            return pick_value(rng);
          }}};

      sim.run_main_task([&] {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          //----- --- -- -  -  -   -
          // Recover the simulated Volume.
          //
          LLFS_VLOG(1) << "sim.get_volume()";

          StatusOr<std::unique_ptr<llfs::Volume>> recovered_volume = sim.get_volume(
              "TestVolume", /*slot_visitor_fn=*/
              [](auto&&...) {
                return batt::OkStatus();
              },
              /*root_log_capacity=*/64 * kKiB);

          ASSERT_TRUE(recovered_volume.ok()) << recovered_volume.status();

          llfs::Volume& volume = **recovered_volume;

          //----- --- -- -  -  -   -
          // Obtain a grant to append the entire contents of the root log.
          //
          batt::Grant grant = BATT_OK_RESULT_OR_PANIC(
              volume.reserve(volume.available_to_reserve(), batt::WaitForResource::kFalse));

          // We will commit half the slots without failure injection, then turn on failure
          // injection and allow errors to be handled.
          //
          sim.set_inject_failures_mode(false);

          batt::Optional<llfs::VolumeMultiAppend> op;
          usize pos_in_batch = 0;
          usize batch_size = 1;
          bool first_half = true;

          for (usize i = 0; i < n_before_crash; ++i) {
            // Check to see if it is time to turn on failure injection yet.
            //
            if (first_half && i >= n_before_crash / 2) {
              first_half = false;
              sim.set_inject_failures_mode(true);
            }

            // Open a multi-append if there isn't an active one.
            //
            if (!op) {
              op.emplace(volume);
            }

            // Allow the simulator to run some background tasks randomly.
            //
            batt::Task::yield();

            // We write multi-appends in batches of increasing size: 1, 2, 3, 4...
            // All multi-appends contain `nonterminal` slots followed by a single `terminal`
            // slot.
            //
            std::string_view s = (pos_in_batch + 1 < batch_size) ? nonterminal : terminal;

            if (!op->append(s, grant).ok()) {
              // This is a valid path, as we aren't checking the grant to see if we have enough
              // space in the log to add `s`.
              //
              LLFS_VLOG(1) << "Append error" << BATT_INSPECT(grant.size())
                           << BATT_INSPECT(op->is_closed());
              break;
            }

            // Increment our position in the current batch and see if the current batch is
            // completed, in which case we commit the multi-append, reset state, and increment the
            // batch_size.
            //
            ++pos_in_batch;
            if (pos_in_batch == batch_size) {
              ++batch_size;
              pos_in_batch = 0;
              if (!op->commit(grant).ok()) {
                LLFS_VLOG(1) << "Commit error";
                break;
              }
              op = batt::None;
            }
          }

          // It's possible that we exited the loop while in the middle of an open multi-append; if
          // so, cancel it now.
          //
          if (op && op->is_open()) {
            op->cancel();
            op = batt::None;
          }
        }

        LLFS_VLOG(1) << "sim.inject_failures = false; crash_and_recover";

        // Simulate a crash and recover.
        //
        sim.set_inject_failures_mode(false);
        sim.crash_and_recover();

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          std::string_view last_slot;
          usize slots_found = 0;
          i32 last_run = -1;
          i32 run_length = 0;

          StatusOr<std::unique_ptr<llfs::Volume>> recovered_volume = sim.get_volume(
              "TestVolume", /*slot_visitor_fn=*/
              [&](const llfs::SlotParse& slot, std::string_view user_data) {
                ++slots_found;
                last_slot = user_data;

                LLFS_VLOG(1) << BATT_INSPECT(slot.offset) << BATT_INSPECT_STR(user_data)
                             << BATT_INSPECT(slots_found);

                if (last_slot == nonterminal) {
                  ++run_length;

                } else {
                  // If the slot data isn't `nonterminal`, and it isn't `terminal`, um... what is
                  // it, and how did it get here??
                  //
                  BATT_CHECK_EQ(last_slot, terminal);

                  EXPECT_EQ(run_length, last_run + 1);

                  last_run = run_length;
                  run_length = 0;
                }
                return batt::OkStatus();
              });

          ASSERT_TRUE(recovered_volume.ok()) << recovered_volume.status();

          // This is the crucial test -- the last slot observed should always be on a multi-append
          // boundary.
          //
          if (slots_found != 0) {
            EXPECT_THAT(last_slot, ::testing::StrEq(terminal))
                << BATT_INSPECT(seed) << BATT_INSPECT(n_before_crash);
          }
        }
      });
    }
  }
}

}  // namespace
