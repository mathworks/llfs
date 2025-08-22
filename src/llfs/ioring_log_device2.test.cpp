//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_device2.hpp>
//
#include <llfs/ioring_log_device2.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/constants.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_device.test.hpp>
#include <llfs/storage_simulation.hpp>

#include <llfs/testing/scenario_runner.hpp>

namespace {

using namespace llfs::constants;
using namespace llfs::int_types;

using llfs::Interval;
using llfs::None;
using llfs::Status;
using llfs::StatusOr;

// Test Plan:
//   - Use StorageSimulation to create an IoRingLogDevice2 with SimulatedLogDeviceStorage
//   - Choose random operations: append, trim
//   - Crash the simulation at a predetermined number of steps; when log is recovered, verify that
//     the confirmed trim and flush never go backwards, and that all confirmed flushed data can be
//     read.
//
class IoRingLogDevice2SimTest : public ::testing::Test
{
 public:
  static constexpr usize kNumSeeds = 250 * 1000;
  static constexpr u64 kTestLogSize = 128 * kKiB;
  static constexpr i64 kTestLogBegin = 7 * 4 * kKiB;
  static constexpr i64 kTestLogEnd =
      kTestLogBegin + 4 * kKiB /*control block (aligned)*/ + kTestLogSize;
  static constexpr usize kTestMinSlotSize = 50;
  static constexpr usize kTestMaxSlotSize = 6000;
  static constexpr usize kNumSlots = 50;
  static constexpr usize kDevicePageSize = llfs::kDirectIOBlockSize;
  static constexpr usize kDataAlignment = llfs::kDirectIOBlockAlign;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Conditions we hope to achieve in at least one test Scenario.
   */
  struct GlobalTestGoals {
    std::atomic<bool> concurrent_writes{false};
    std::atomic<bool> burst_mode_checked{false};
    std::atomic<bool> burst_mode_applied{false};
    std::atomic<bool> split_write{false};
    std::atomic<bool> tail_collision{false};
    std::atomic<bool> tail_rewrite{false};
  };

  /** \brief A single simulation run.
   *
   * It is safe to run multiple Scenarios concurrently on separate threads.
   */
  struct Scenario {
    const std::string kTestStorageName = "test_storage";

    const llfs::IoRingLogConfig2 log_config{
        .control_block_offset = kTestLogBegin,
        .log_capacity = kTestLogSize,
        .device_page_size_log2 = batt::log2_ceil(kDevicePageSize),
        .data_alignment_log2 = batt::log2_ceil(kDataAlignment),
    };

    /** \brief Reference to single shared goals struct; used to report achievement of various goals.
     */
    GlobalTestGoals& goals;

    /** \brief The RNG seed for this scenario.
     */
    usize seed;

    /** \brief The entropy source for the simulation.
     */
    std::mt19937 rng;

    /** \brief The simulation context.
     */
    llfs::StorageSimulation sim;

    /** \brief LogDevice object-under-test; created in recover_log_device.  initialize_log_storage
     * MUST be called prior to recovering the log device for the first time.
     */
    std::unique_ptr<llfs::LogDevice> log_device;

    /** \brief The LogDevice::Writer interface for `this->log_device`.
     */
    llfs::LogDevice::Writer* log_writer = nullptr;

    /** \brief The metrics object associated with the log device under test.
     */
    const llfs::IoRingLogDevice2Metrics* log_device_metrics = nullptr;

    /** \brief The slot offset ranges of all appended slots that haven't been trimmed yet; these are
     * not necessarily flushed yet.
     */
    std::deque<llfs::SlotRange> appended_slots;

    /** \brief The slot offset ranges of all slots have been speculatively trimmed; the trim
     * operation isn't necessarily durable yet (hence 'maybe').
     */
    std::deque<llfs::SlotRange> maybe_trimmed_slots;

    /** \brief The slot offset ranges of appended_slots that aren't yet known to be flushed/durable.
     */
    std::deque<llfs::SlotRange> maybe_flushed_slots;

    /** \brief The latest observed durable trim position.
     */
    llfs::slot_offset_type observed_trim = 0;

    /** \brief The latest observed durable flush position.
     */
    llfs::slot_offset_type observed_flush = 0;

    /** \brief The total number of bytes that have been appended to the log since the beginning of
     * the scenario.
     */
    usize total_bytes_appended = 0;

    /** \brief Distribution used to select appended slot payload sizes.
     */
    std::uniform_int_distribution<usize> pick_slot_size{kTestMinSlotSize, kTestMaxSlotSize};

    /** \brief The closest slot boundary (without going over) to the observed_flush position.
     */
    llfs::slot_offset_type observed_slot_flush = 0;

    /** \brief The number of partially flushed slots discovered after simulated crash/recovery.
     */
    usize partial_slot_flush_count = 0;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief Constructs a new Scenario with the given RNG seed.
     */
    explicit Scenario(usize seed, GlobalTestGoals& goals) noexcept
        : goals{goals}
        , seed{seed}
        , rng{this->seed}
        , sim{batt::StateMachineEntropySource{
              /*entropy_fn=*/[this](usize min_value, usize max_value) -> usize {
                std::uniform_int_distribution<usize> pick_value{min_value, max_value};
                return pick_value(this->rng);
              }}}
    {
    }

    /** \brief Writes initial data (control block) to the simulated media.
     */
    void initialize_log_storage() noexcept
    {
      llfs::SimulatedLogDeviceStorage storage = sim.get_log_device_storage(
          kTestStorageName, /*capacity=*/kTestLogSize, Interval<i64>{kTestLogBegin, kTestLogEnd});

      ASSERT_FALSE(storage.is_initialized());

      Status init_status =
          llfs::initialize_log_device2(*storage.get_raw_block_file(), this->log_config,
                                       llfs::ConfirmThisWillEraseAllMyData::kYes);

      ASSERT_TRUE(init_status.ok()) << BATT_INSPECT(init_status);

      storage.set_initialized(true);
    }

    /** \brief Opens the log from simulated media and creates the LogDevice object
     * (`this->log_device`).
     *
     * Also initializes `this->log_writer`.
     */
    void recover_log_device() noexcept
    {
      llfs::SimulatedLogDeviceStorage storage =
          sim.get_log_device_storage(kTestStorageName, None, None);

      ASSERT_TRUE(storage.is_initialized());

      auto runtime_options = llfs::LogDeviceRuntimeOptions{
          .name = "test_log",
          .flush_delay_threshold = kTestMaxSlotSize * 3,
          .max_concurrent_writes = 16,
          .optimize_burst_mode = true,
      };

      auto p_device =
          std::make_unique<llfs::BasicIoRingLogDevice2<llfs::SimulatedLogDeviceStorage>>(
              this->log_config, runtime_options, std::move(storage));

      BATT_CHECK_OK(p_device->open());

      this->log_device_metrics = std::addressof(p_device->metrics());
      this->log_device = std::move(p_device);
      this->log_writer = std::addressof(this->log_device->writer());
    }

    /** \brief Shuts down the log device, deletes the object, and resets all pointers.
     */
    void shutdown_log_device() noexcept
    {
      if (this->log_device) {
        BATT_CHECK_NOT_NULLPTR(this->log_device_metrics);

        // Check to see whether this scenario accomplished any global test goals.
        //
        if (this->log_device_metrics->max_concurrent_writes > 1) {
          this->goals.concurrent_writes = true;
        }
        if (this->log_device_metrics->burst_mode_checked > 0) {
          this->goals.burst_mode_checked = true;
        }
        if (this->log_device_metrics->burst_mode_applied > 0) {
          BATT_CHECK_GE(this->log_device_metrics->burst_mode_checked,
                        this->log_device_metrics->burst_mode_applied);
          this->goals.burst_mode_applied = true;
        }
        if (this->log_device_metrics->flush_write_split_wrap_count > 0) {
          this->goals.split_write = true;
        }
        if (this->log_device_metrics->flush_write_tail_collision_count > 0) {
          this->goals.tail_collision = true;
        }
        if (this->log_device_metrics->flush_tail_rewrite_count > 0) {
          this->goals.tail_rewrite = true;
        }

        // Shut down the device.
        //
        this->log_device->halt();
        this->log_device->join();
        this->log_device.reset();
        this->log_writer = nullptr;
        this->log_device_metrics = nullptr;
      }
    }

    /** \brief Writes a single random-sized slot to the log.
     * \return false if an error was encountered; true otherwise
     */
    bool append_one_slot() noexcept
    {
      const usize payload_size = pick_slot_size(this->rng);
      const usize header_size = llfs::packed_sizeof_varint(payload_size);
      const usize slot_size = header_size + payload_size;

      if (!this->trim_to_reserve(slot_size)) {
        return false;
      }

      llfs::slot_offset_type slot_offset = this->log_writer->slot_offset();
      StatusOr<llfs::MutableBuffer> buffer = this->log_writer->prepare(slot_size);
      BATT_CHECK_OK(buffer);

      *buffer = batt::get_or_panic(llfs::pack_varint_to(*buffer, payload_size));
      std::memset(buffer->data(), 'a' + (slot_offset % (('z' - 'a') + 1)), buffer->size());

      StatusOr<llfs::slot_offset_type> end_offset = this->log_writer->commit(slot_size);
      BATT_CHECK_OK(end_offset);
      BATT_CHECK_EQ(slot_offset + slot_size, *end_offset);

      this->appended_slots.push_back(llfs::SlotRange{
          .lower_bound = slot_offset,
          .upper_bound = *end_offset,
      });
      this->maybe_flushed_slots.push_back(this->appended_slots.back());
      this->total_bytes_appended += slot_size;

      return true;
    }

    /** \brief Updates observed_trim, observed_flush, and maybe_flushed_slots based on re-reading
     * the current durable range of the log.
     */
    void update_trim_flush_pos()
    {
      llfs::SlotRange slot_range = this->log_device->slot_range(llfs::LogReadMode::kDurable);

      ASSERT_FALSE(llfs::slot_less_than(slot_range.lower_bound, this->observed_trim))
          << BATT_INSPECT(slot_range) << BATT_INSPECT(this->observed_trim);

      ASSERT_FALSE(llfs::slot_less_than(slot_range.upper_bound, this->observed_flush))
          << BATT_INSPECT(slot_range) << BATT_INSPECT(this->observed_flush);

      this->observed_trim = slot_range.lower_bound;
      this->observed_flush = slot_range.upper_bound;

      while (!this->maybe_flushed_slots.empty() &&
             !llfs::slot_less_than(this->observed_flush,
                                   this->maybe_flushed_slots.front().upper_bound)) {
        this->observed_slot_flush = this->maybe_flushed_slots.front().upper_bound;
        this->maybe_flushed_slots.pop_front();
      }
    }

    /** \brief Attempts to reserve at least n_bytes for appending in the log, by trimming old slots
     * if necessary.
     *
     * If this function returns true, all is fine; otherwise, it indicates a fatal error that should
     * signal the end of the append phase of this scenario.
     */
    bool trim_to_reserve(usize n_bytes)
    {
      u64 observed_space = this->log_writer->space();
      while (observed_space < n_bytes && !this->appended_slots.empty()) {
        const llfs::SlotRange trimmed_slot = this->appended_slots.front();

        this->appended_slots.pop_front();
        this->maybe_trimmed_slots.push_back(trimmed_slot);

        Status trim_status = this->log_device->trim(trimmed_slot.upper_bound);
        if (!trim_status.ok()) {
          break;
        }
        BATT_CHECK_EQ(this->log_device->slot_range(llfs::LogReadMode::kDurable).lower_bound,
                      trimmed_slot.upper_bound);

        Status await_status = this->log_writer->await(llfs::BytesAvailable{
            .size = trimmed_slot.size() + observed_space,
        });
        if (!await_status.ok()) {
          break;
        }

        llfs::SlotRange slot_range = this->log_device->slot_range(llfs::LogReadMode::kDurable);
        while (!this->maybe_trimmed_slots.empty() &&
               llfs::slot_less_than(this->maybe_trimmed_slots.front().lower_bound,
                                    slot_range.lower_bound)) {
          LLFS_CHECK_SLOT_LE(this->maybe_trimmed_slots.front().upper_bound, slot_range.lower_bound);
          observed_space += this->maybe_trimmed_slots.front().size();
          this->maybe_trimmed_slots.pop_front();
        }
      }

      // Update the observed trim/flush pointers.
      //
      this->update_trim_flush_pos();

      // Return true iff we succeeded in trimming enough space.
      //
      return (observed_space >= n_bytes);
    }

    /** \brief Verify that the recovered durable slot range doesn't violate our assumptions:
     *
     * - observed trim pos should never move backwards
     * - slot range should always be at most the total size of the log
     * - slot range should not have negative size (i.e., lower_bound <= upper_bound)
     * - the observed flush pos should either be the same as the last one observed, or it should be
     *   not less than the highest observed slot upper bound for all appends.
     */
    void verify_recovered_slot_range(const llfs::SlotRange& slot_range)
    {
      ASSERT_TRUE(!llfs::slot_less_than(slot_range.upper_bound, slot_range.lower_bound))
          << BATT_INSPECT(slot_range);

      ASSERT_LE(slot_range.size(), kTestLogSize) << BATT_INSPECT(slot_range);

      ASSERT_FALSE(llfs::slot_less_than(slot_range.lower_bound, observed_trim))
          << BATT_INSPECT(slot_range) << BATT_INSPECT(observed_trim) << BATT_INSPECT(seed);

      ASSERT_FALSE(llfs::slot_less_than(slot_range.upper_bound, observed_slot_flush))
          << BATT_INSPECT(slot_range) << BATT_INSPECT(observed_slot_flush);

      if (llfs::slot_less_than(slot_range.upper_bound, observed_flush)) {
        this->partial_slot_flush_count += 1;
      }
    }

    /** \brief Moves any ranges in maybe_trimmed_slots to appended_slots, based on the recovered
     * durable slot range.
     *
     * maybe_trimmed_slots contains ranges for which we aren't sure whether they were actually
     * trimmed (via a durable update of the control block).  By looking at the actual recovered slot
     * range, we know for sure which ones still belong on this list.
     *
     * By the time this function returns, maybe_trimmed_slots will be empty, since we've either
     * removed each slot range, or moved it from maybe_trimmed_slots to appended_slots.
     */
    void resolve_maybe_trimmed_slots(const llfs::SlotRange& slot_range)
    {
      // Remove any actually trimmed slots from the maybe_trimmed list.
      //
      while (!this->maybe_trimmed_slots.empty() &&
             llfs::slot_less_than(this->maybe_trimmed_slots.front().lower_bound,
                                  slot_range.lower_bound)) {
        LLFS_CHECK_SLOT_LE(this->maybe_trimmed_slots.front().upper_bound, slot_range.lower_bound);
        this->maybe_trimmed_slots.pop_front();
      }

      // Move any remaining maybe trimmed slots to the front of the appended list.
      //
      while (!this->maybe_trimmed_slots.empty()) {
        this->appended_slots.push_front(this->maybe_trimmed_slots.back());
        this->maybe_trimmed_slots.pop_back();
      }
    }

    /** \brief Verifies that the recovered log has the expected slot data, based on a prior append
     * phase.
     *
     * \param slot_range the durable slot range of the recovered LogDevice
     */
    void verify_recovered_slots(const llfs::SlotRange& slot_range)
    {
      std::unique_ptr<llfs::LogDevice::Reader> log_reader =
          this->log_device->new_reader(/*slot_lower_bound=*/None, llfs::LogReadMode::kDurable);

      llfs::SlotReader slot_reader{*log_reader};

      StatusOr<usize> n_parsed = slot_reader.run(
          batt::WaitForResource::kFalse, [&](const llfs::SlotParse& slot) -> Status {
            BATT_CHECK_EQ(slot.offset, this->appended_slots.front());
            this->appended_slots.pop_front();

            // Verify the data.
            //
            const char expected_ch = 'a' + (slot.offset.lower_bound % (('z' - 'a') + 1));
            for (char actual_ch : slot.body) {
              EXPECT_EQ(expected_ch, actual_ch);
              if (actual_ch != expected_ch) {
                LLFS_LOG_INFO() << BATT_INSPECT(slot.offset) << BATT_INSPECT(slot_range)
                                << BATT_INSPECT(this->observed_trim)
                                << BATT_INSPECT(this->observed_flush);
                break;
              }
            }

            return batt::OkStatus();
          });

      ASSERT_TRUE(n_parsed.ok()) << BATT_INSPECT(n_parsed);

      // If there are any unmatched slots, then assert they are in the unflushed range.
      //
      if (!this->appended_slots.empty()) {
        ASSERT_GE(this->appended_slots.front().lower_bound, slot_range.upper_bound);
      }
    }

    /** \brief The simulation entry point.
     *
     * Consists of two phases:
     *   1. append phase - append slots until we reach kNumSlots or an injected error stops us from
     *      making progress
     *   2. verify phase - recover the log and check flush/trim pos and slot data
     *
     * If we run out of space in (1), we trim slots one at a time until the desired amount of space
     * has been freed up.
     */
    void run()
    {
      ASSERT_NO_FATAL_FAILURE(this->initialize_log_storage());

      this->sim.run_main_task([this] {
        {
          ASSERT_NO_FATAL_FAILURE(this->recover_log_device());
          auto on_scope_exit = batt::finally([&] {
            this->shutdown_log_device();
          });

          this->sim.set_inject_failures_mode((seed % 2) == 1);

          for (usize i = 0; i < kNumSlots; ++i) {
            LLFS_VLOG(1) << "Writing slot " << i << " of " << kNumSlots << ";"
                         << BATT_INSPECT(this->total_bytes_appended) << BATT_INSPECT(seed);

            if (!this->append_one_slot()) {
              break;
            }
          }
        }

        this->sim.crash_and_recover();
        this->sim.set_inject_failures_mode(false);

        {
          ASSERT_NO_FATAL_FAILURE(this->recover_log_device());
          auto on_scope_exit = batt::finally([&] {
            this->shutdown_log_device();
          });

          llfs::SlotRange slot_range = this->log_device->slot_range(llfs::LogReadMode::kDurable);

          ASSERT_NO_FATAL_FAILURE(this->verify_recovered_slot_range(slot_range));
          ASSERT_NO_FATAL_FAILURE(this->resolve_maybe_trimmed_slots(slot_range));
          ASSERT_NO_FATAL_FAILURE(this->verify_recovered_slots(slot_range));
        }
      });
    }

  };  // struct Scenario

};  // class IoRingLogDevice2SimTest

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(IoRingLogDevice2SimTest, Simulation)
{
  llfs::testing::TestConfig test_config;

  LLFS_LOG_INFO() << BATT_INSPECT(kNumSeeds);

  GlobalTestGoals goals;

  EXPECT_FALSE(goals.concurrent_writes);
  EXPECT_FALSE(goals.burst_mode_checked);
  EXPECT_FALSE(goals.burst_mode_applied);
  EXPECT_FALSE(goals.split_write);
  EXPECT_FALSE(goals.tail_collision);
  EXPECT_FALSE(goals.tail_rewrite);

  llfs::testing::ScenarioRunner{}  //
      .n_seeds(kNumSeeds)
      .n_updates(20)
      .run(batt::StaticType<Scenario>{}, goals);

  EXPECT_TRUE(goals.concurrent_writes);
  EXPECT_TRUE(goals.burst_mode_checked);
  EXPECT_TRUE(goals.burst_mode_applied);
  EXPECT_TRUE(goals.split_write);
  EXPECT_TRUE(goals.tail_collision);
  EXPECT_TRUE(goals.tail_rewrite);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoRingLogDevice2Test, Benchmark)
{
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Set configuration and options.
  //
  llfs::LogDeviceRuntimeOptions options{
      .name = "test log",
      .flush_delay_threshold = 128 * kKiB,
      .max_concurrent_writes = 64,
      .optimize_burst_mode = llfs::read_test_var<int>("LLFS_LOG_DEVICE_BURST", /*default=*/0) != 0,
  };

  const char* file_name =  //
      std::getenv("LLFS_LOG_DEVICE_FILE");

  if (!file_name) {
    LLFS_LOG_INFO() << "LLFS_LOG_DEVICE_FILE not specified; skipping benchmark test";
    return;
  }

  std::cout << "LLFS_LOG_DEVICE_FILE=" << batt::c_str_literal(file_name) << std::endl;

  LLFS_LOG_INFO() << BATT_INSPECT(file_name);

  llfs::run_log_device_benchmark([&](usize log_size, bool create, auto&& workload_fn) {
    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Erase any existing file.
    //
    if (create) {
      {
        std::filesystem::path file_path{file_name};
        std::filesystem::remove_all(file_path);
        ASSERT_FALSE(std::filesystem::exists(file_path));
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Create a new log file and size it to the configured capacity.
    //
    llfs::IoRingLogConfig2 config{
        .control_block_offset = 0,
        .log_capacity = log_size,
        .device_page_size_log2 = llfs::kDirectIOBlockSizeLog2,
        .data_alignment_log2 = llfs::kDirectIOBlockAlignLog2,
    };

    llfs::StatusOr<int> status_or_fd = [&] {
      if (create) {
        return llfs::create_file_read_write(file_name, llfs::OpenForAppend{false});
      } else {
        return llfs::open_file_read_write(file_name, llfs::OpenForAppend{false},
                                          llfs::OpenRawIO{true});
      }
    }();

    ASSERT_TRUE(status_or_fd.ok()) << BATT_INSPECT(status_or_fd);

    const int fd = *status_or_fd;

    if (create) {
      llfs::Status enable_raw_status = llfs::enable_raw_io_fd(fd, true);

      ASSERT_TRUE(enable_raw_status.ok()) << BATT_INSPECT(enable_raw_status);

      llfs::Status truncate_status =
          llfs::truncate_fd(fd, /*size=*/config.control_block_size() + config.log_capacity);

      ASSERT_TRUE(truncate_status.ok());
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Initialize the IoRing and IoRing::File inside a storage object wrapper.
    //
    llfs::StatusOr<llfs::DefaultIoRingLogDeviceStorage> status_or_storage =
        llfs::DefaultIoRingLogDeviceStorage::make_new(llfs::MaxQueueDepth{256}, fd);

    ASSERT_TRUE(status_or_storage.ok()) << BATT_INSPECT(status_or_storage.status());

    llfs::DefaultIoRingLogDeviceStorage& storage = *status_or_storage;

    if (create) {
      llfs::DefaultIoRingLogDeviceStorage::RawBlockFileImpl file{storage};

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      // Write the initial contents of the file.
      //
      llfs::Status init_status =
          llfs::initialize_log_device2(file, config, llfs::ConfirmThisWillEraseAllMyData::kYes);

      ASSERT_TRUE(init_status.ok()) << BATT_INSPECT(init_status);
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Create LogDevice object and open.
    //
    llfs::IoRingLogDevice2 log_device{config, options, std::move(storage)};
    batt::Status open_status = log_device.driver().open();

    ASSERT_TRUE(open_status.ok()) << BATT_INSPECT(open_status);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Run the passed workload.
    //
    workload_fn(log_device);

    const double write_amplification =
        double(log_device.metrics().bytes_written) / double(log_device.metrics().bytes_flushed);

    LLFS_LOG_INFO() << log_device.metrics() << BATT_INSPECT(write_amplification);
  });
}

}  // namespace
