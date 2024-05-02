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

#include <llfs/storage_simulation.hpp>

#include <random>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

using llfs::StatusOr;

TEST(VolumeMultiAppendTest, DISABLED_SimTest)
{
  for (u32 seed = 0; seed < 100; ++seed) {
    for (usize n_before_crash = 1; n_before_crash < 20; ++n_before_crash) {
      LLFS_LOG_INFO() << BATT_INSPECT(seed);
      std::mt19937 rng{seed};

      llfs::StorageSimulation sim{batt::StateMachineEntropySource{
          /*entropy_fn=*/[&rng](usize min_value, usize max_value) -> usize {
            std::uniform_int_distribution<usize> pick_value{min_value, max_value};
            return pick_value(rng);
          }}};

      sim.run_main_task([&] {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          StatusOr<std::unique_ptr<llfs::Volume>> recovered_volume = sim.get_volume(
              "TestVolume", /*slot_visitor_fn=*/
              [](auto&&...) {
                return batt::OkStatus();
              },
              /*root_log_capacity=*/64 * kKiB);

          ASSERT_TRUE(recovered_volume.ok()) << recovered_volume.status();

          llfs::Volume& volume = **recovered_volume;

          batt::Grant grant = BATT_OK_RESULT_OR_PANIC(
              volume.reserve(volume.available_to_reserve(), batt::WaitForResource::kFalse));

          sim.set_inject_failures_mode(true);

          batt::Optional<llfs::VolumeMultiAppend> op;
          usize pos_in_batch = 0;
          usize batch_size = 1;

          for (usize i = 0; i < n_before_crash; ++i) {
            if (!op) {
              op.emplace(volume);
            }

            batt::Task::yield();

            std::string_view s = (pos_in_batch + 1 < batch_size) ? "a" : "b";

            if (!op->append(s, grant).ok()) {
              LLFS_LOG_INFO() << "Append error";
              op->cancel();
              break;
            }

            ++pos_in_batch;
            if (pos_in_batch == batch_size) {
              ++batch_size;
              pos_in_batch = 0;
              if (!op->commit(grant).ok()) {
                LLFS_LOG_INFO() << "Commit error";
                break;
              }
              op = batt::None;
            }
          }

          if (op) {
            op->cancel();
            op = batt::None;
          }
        }

        sim.set_inject_failures_mode(false);
        sim.crash_and_recover();

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          std::string last_slot;
          usize slots_found = 0;
          i32 last_run = -1;
          i32 run_length = 0;

          StatusOr<std::unique_ptr<llfs::Volume>> recovered_volume = sim.get_volume(
              "TestVolume", /*slot_visitor_fn=*/
              [&](const llfs::SlotParse& slot, std::string_view user_data) {
                ++slots_found;
                last_slot = user_data;

                LLFS_LOG_INFO() << BATT_INSPECT(slot.offset) << BATT_INSPECT_STR(user_data)
                                << BATT_INSPECT(slots_found);

                if (last_slot == "a") {
                  ++run_length;

                } else {
                  BATT_CHECK_EQ(last_slot, std::string{"b"});

                  EXPECT_EQ(run_length, last_run + 1);

                  last_run = run_length;
                  run_length = 0;
                }
                return batt::OkStatus();
              });

          ASSERT_TRUE(recovered_volume.ok()) << recovered_volume.status();

          if (slots_found != 0) {
            EXPECT_THAT(last_slot, ::testing::StrEq("b"));
          }
        }
      });
    }
  }
}

}  // namespace
