//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_trimmer.hpp>
//
#include <llfs/volume_trimmer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/log_device_snapshot.hpp>
#include <llfs/pack_as_raw.hpp>
#include <llfs/testing/fake_log_device.hpp>
#include <llfs/uuid.hpp>

#include <batteries/env.hpp>

#include <random>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

using llfs::testing::FakeLogDevice;

// Test plan:
//
// - Choose test action:
//   - Shut down
//   - Run a pending task continuation
//   - Write a slot to the log
//     - Attach event
//     - Detach event
//     - Opaque user data
//     - PrepareJob
//     - CommitJob
//     - RollbackJob
//   - Advance the trim position
//   - Crash/recover
//
// - State to maintain:
//   - Set of active attachments
//   - Ref counts per page
//
// - Model:
//   - 4 pages

constexpr usize kNumPages = 4;
constexpr usize kLogSize = 64 * kKiB;
constexpr usize kMinTTF = 5;
constexpr usize kMaxTTF = 500;
constexpr usize kMinOpaqueDataSize = 10;
constexpr usize kMaxOpaqueDataSize = 1000;
constexpr usize kMaxSlotOverhead = 32;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeTrimmerTest : public ::testing::Test
{
 public:
  VolumeTrimmerTest()
  {
  }

  /** \brief Resets all test state variables to their initial values.
   */
  void reset_state()
  {
    this->recovery_visitor = batt::None;
    this->page_ref_count.fill(0);
    this->pending_jobs.clear();
    this->committed_jobs.clear();
    this->job_grant = batt::None;
    this->job_grant_size = 0;
    this->leaked_job_grant_size = 0;
    this->verified_client_slot = 0;
    this->initialize_log();
  }

  /** \brief Returns a pseudo-random usize value r, where r >= min_value and r <= max_value.
   */
  usize pick_usize(usize min_value, usize max_value)
  {
    std::uniform_int_distribution<usize> pick{min_value, max_value};
    return pick(this->rng);
  }

  /** \brief Returns a pseudo-random boolean value.
   */
  bool pick_branch()
  {
    return this->pick_usize(0, 1) == 1;
  }

  /** \brief Creates a new MemoryLogDevice and initializes it with minimal llfs::Volume metadata.
   */
  void initialize_log()
  {
    this->mem_log_device.emplace(kLogSize);

    llfs::TypedSlotWriter<llfs::VolumeEventVariant> slot_writer{*this->mem_log_device};

    batt::StatusOr<batt::Grant> grant = slot_writer.reserve(
        llfs::packed_sizeof_slot(this->volume_ids), batt::WaitForResource::kFalse);

    ASSERT_TRUE(grant.ok());

    batt::StatusOr<llfs::SlotRange> ids_slot = slot_writer.append(*grant, this->volume_ids);

    ASSERT_TRUE(ids_slot.ok());
    EXPECT_EQ(ids_slot->lower_bound, 0u);

    this->metadata_state.most_recent_ids_slot = ids_slot->lower_bound;
  }

  /** \brief Creates a new FakeLogDevice that wraps this->mem_log_device, giving it a pseudo-random
   * time-to-failure.
   *
   * Also creates a new SlotLockManager (this->trim_control), SlotReadLock (this->trim_lock), and
   * slot writer to be used by the VolumeTrimmer (object-under-test).
   */
  void open_fake_log()
  {
    llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver> factory =
        llfs::testing::make_fake_log_device_factory(*this->mem_log_device);

    this->fake_log_state = factory.state();

    const llfs::slot_offset_type initial_trim_pos =
        this->mem_log_device->slot_range(llfs::LogReadMode::kDurable).lower_bound;

    this->recovery_visitor.emplace(initial_trim_pos);

    {
      batt::StatusOr<std::unique_ptr<llfs::LogDevice>> status_or_fake_log = factory.open_log_device(
          [&](llfs::LogDevice::Reader& log_reader) -> batt::StatusOr<llfs::slot_offset_type> {
            llfs::TypedSlotReader<llfs::VolumeEventVariant> slot_reader{log_reader};

            batt::StatusOr<usize> slots_read = slot_reader.run(
                batt::WaitForResource::kFalse,
                [&](const llfs::SlotParse& slot, const auto& payload) -> batt::Status {
                  BATT_REQUIRE_OK((*this->recovery_visitor)(slot, payload));
                  return batt::OkStatus();
                });

            BATT_REQUIRE_OK(slots_read);

            return log_reader.slot_offset();
          });

      ASSERT_TRUE(status_or_fake_log.ok());
      this->fake_log = std::move(*status_or_fake_log);
    }

    EXPECT_EQ(this->metadata_state.most_recent_ids_slot != batt::None,
              recovery_visitor->get_refresh_info().most_recent_ids_slot != batt::None)
        << BATT_INSPECT(this->metadata_state) << BATT_INSPECT(recovery_visitor->get_refresh_info());

    // Reset the simulated device failure time.
    //
    const auto fake_log_ttf = this->pick_usize(kMinTTF, kMaxTTF);
    LLFS_VLOG(1) << BATT_INSPECT(fake_log_ttf);
    this->fake_log_state->failure_time = this->fake_log_state->device_time + fake_log_ttf;

    // Initialize the slot lock manager.
    //
    this->trim_lock.clear();
    this->trim_control.emplace();
    this->trim_lock = BATT_OK_RESULT_OR_PANIC(
        this->trim_control->lock_slots(this->fake_log->slot_range(llfs::LogReadMode::kSpeculative),
                                       "VolumeTrimmerTest::open_fake_log"));

    // Create a slot writer for the fake log.
    //
    if (this->job_grant) {
      this->job_grant_size = this->job_grant->size() + this->leaked_job_grant_size;
      this->leaked_job_grant_size = 0;
    }
    this->job_grant = batt::None;
    this->fake_slot_writer.emplace(*this->fake_log);
  }

  /** \brief Create a VolumeTrimmer for testing.
   */
  void initialize_trimmer()
  {
    ASSERT_TRUE(this->trim_control);
    ASSERT_TRUE(this->fake_log);
    ASSERT_TRUE(this->fake_slot_writer);
    ASSERT_TRUE(this->recovery_visitor);

    if (this->job_grant_size > 0) {
      batt::StatusOr<batt::Grant> reserved =
          this->fake_slot_writer->reserve(this->job_grant_size, batt::WaitForResource::kFalse);
      if (!reserved.ok()) {
        LLFS_LOG_ERROR() << "Failed to reserve grant;" << BATT_INSPECT(reserved.status())
                         << BATT_INSPECT(this->job_grant_size)
                         << BATT_INSPECT(this->fake_slot_writer->pool_size());
      }
      ASSERT_TRUE(reserved.ok());
      this->job_grant.emplace(std::move(*reserved));
    }

    LLFS_VLOG(1) << BATT_INSPECT(this->job_grant_size)
                 << BATT_INSPECT(this->recovery_visitor->get_refresh_info())
                 << BATT_INSPECT(this->recovery_visitor->get_trim_event_info())
                 << BATT_INSPECT_RANGE(this->recovery_visitor->get_pending_jobs())
                 << BATT_INSPECT(this->recovery_visitor->get_trimmer_grant_size());

    this->trimmer = std::make_unique<llfs::VolumeTrimmer>(
        this->volume_ids.trimmer_uuid, *this->trim_control,
        this->fake_log->new_reader(/*slot_lower_bound=*/batt::None,
                                   llfs::LogReadMode::kSpeculative),
        *this->fake_slot_writer,
        [this](auto&&... args) -> decltype(auto) {
          return this->handle_drop_roots(BATT_FORWARD(args)...);
        },
        *this->recovery_visitor);

    EXPECT_EQ(this->trimmer->uuid(), this->volume_ids.trimmer_uuid);

    LLFS_VLOG(1) << "Starting trimmer task";

    this->trimmer_task.emplace(this->task_context.get_executor(), [this] {
      this->trimmer_status = this->trimmer->run();
    });
  }

  /** \brief Stop the current VolumeTrimmer.
   */
  void shutdown_trimmer()
  {
    // Tell everything to shut down.
    //
    this->shutting_down = true;
    auto on_scope_exit = batt::finally([&] {
      this->shutting_down = false;
    });

    this->fake_log->close().IgnoreError();
    this->trimmer->halt();
    this->trim_control->halt();

    // Before we close the MemoryLogDevice (to unblock the VolumeTrimmer task), take a snapshot so
    // it can be restored afterward.
    //
    auto snapshot =
        llfs::LogDeviceSnapshot::from_device(*this->mem_log_device, llfs::LogReadMode::kDurable);

    this->mem_log_device->close().IgnoreError();

    // Drain the executor's queue.
    //
    ASSERT_NO_FATAL_FAILURE(this->task_context.poll());

    // The trimmer task should be finished at this point.
    //
    ASSERT_TRUE(this->trimmer_task->try_join())
        << "n_tasks=" << batt::Task::backtrace_all(/*force=*/true);

    LLFS_VLOG(1) << "Trimmer task joined";

    // Re-open the log and verify.
    //
    this->mem_log_device.emplace(kLogSize);
    this->mem_log_device->restore_snapshot(snapshot, llfs::LogReadMode::kDurable);
    this->open_fake_log();
  }

  /** \brief Called in response to the VolumeTrimmer requesting that a set of PageId's be dropped
   * due to a log trim operation.
   *
   * Verifies that the correct set of page ids is specified, according to the trimmed region pointed
   * to by the VolumeTrimEvent record at client_slot.
   */
  batt::Status handle_drop_roots(llfs::slot_offset_type client_slot,
                                 llfs::Slice<const llfs::PageId> page_ids)
  {
    if (!llfs::slot_less_than(this->verified_client_slot, client_slot)) {
      return batt::OkStatus();
    }

    if (this->shutting_down) {
      return llfs::make_status(llfs::StatusCode::kFakeLogDeviceExpectedFailure);
    }

    std::unique_ptr<llfs::LogDevice::Reader> log_reader = this->mem_log_device->new_reader(
        /*slot_lower_bound=*/client_slot, llfs::LogReadMode::kSpeculative);
    BATT_CHECK_NOT_NULLPTR(log_reader);

    std::vector<llfs::PageId> actual_page_ids(page_ids.begin(), page_ids.end());
    std::sort(actual_page_ids.begin(), actual_page_ids.end());

    std::vector<llfs::PageId> expected_page_ids;

    LLFS_VLOG(1) << "handle_drop_roots(" << BATT_INSPECT(client_slot) << ","
                 << BATT_INSPECT_RANGE(page_ids) << ")";

    llfs::TypedSlotReader<llfs::VolumeEventVariant> slot_reader{*log_reader};
    batt::StatusOr<usize> n_read = slot_reader.run(
        batt::WaitForResource::kFalse,
        batt::make_case_of_visitor(
            [&](const llfs::SlotParse& slot, const llfs::VolumeTrimEvent& trim_event) {
              for (auto iter = this->committed_jobs.begin(); iter != this->committed_jobs.end();
                   iter = this->committed_jobs.erase(iter)) {
                const auto& [prepare_slot, page_ids] = *iter;
                if (!llfs::slot_less_than(prepare_slot, trim_event.new_trim_pos)) {
                  break;
                }
                LLFS_VLOG(1) << " -- expecting: " << BATT_INSPECT(prepare_slot)
                             << BATT_INSPECT_RANGE(page_ids);
                expected_page_ids.insert(expected_page_ids.end(), page_ids.begin(), page_ids.end());
              }

              // Just read a single slot!
              //
              return llfs::make_status(llfs::StatusCode::kBreakSlotReaderLoop);
            },
            [&](const llfs::SlotParse& slot, const auto& payload) {
              BATT_PANIC() << "There should always be a trim event slot at `client_slot`!"
                           << BATT_INSPECT(slot.offset) << ", payload type == "
                           << batt::name_of<std::decay_t<decltype(payload)>>();

              // Just read a single slot!
              //
              return llfs::make_status(llfs::StatusCode::kBreakSlotReaderLoop);
            }));

    // Sort the expected page ids so we can compare it to the actual page ids.
    //
    std::sort(expected_page_ids.begin(), expected_page_ids.end());

    this->verified_client_slot = client_slot;

    [&] {
      ASSERT_EQ(actual_page_ids, expected_page_ids);
    }();

    return batt::OkStatus();
  }

  /** \brief Returns true iff a fake log has been created and the simulated device time is at least
   * the planned failure time.
   */
  bool fake_log_has_failed() const
  {
    return this->fake_log_state &&
           this->fake_log_state->device_time >= this->fake_log_state->failure_time;
  }

  /** \brief Appends an application-level opaque data slot to the fake log and updates the
   * SlotLockManager's upper bound.
   */
  void append_opaque_data_slot()
  {
    ASSERT_TRUE(this->fake_log);
    ASSERT_TRUE(this->fake_slot_writer);
    ASSERT_TRUE(this->trim_control);

    const usize data_size = this->pick_usize(kMinOpaqueDataSize, kMaxOpaqueDataSize);
    std::vector<char> buffer(data_size, 'a');
    std::string_view data_str{buffer.data(), buffer.size()};

    auto&& payload = llfs::PackableRef{llfs::pack_as_raw(data_str)};

    const usize slot_size = llfs::packed_sizeof_slot(payload);
    batt::StatusOr<batt::Grant> slot_grant =
        this->fake_slot_writer->reserve(slot_size, batt::WaitForResource::kFalse);

    ASSERT_TRUE(slot_grant.ok() || this->fake_log_has_failed())
        << BATT_INSPECT(slot_grant.status()) << BATT_INSPECT(slot_size)
        << BATT_INSPECT(this->fake_slot_writer->pool_size());

    batt::StatusOr<llfs::SlotRange> slot_range =
        this->fake_slot_writer->append(*slot_grant, payload);

    ASSERT_TRUE(slot_range.ok() || this->fake_log_has_failed());

    this->trim_control->update_upper_bound(slot_range->upper_bound);

    LLFS_VLOG(1) << "Appended opaque data: " << batt::c_str_literal(data_str);
  }

  /** \brief Selects a pseudo-random trim point and updates the trim lock to cause the VolumeTrimmer
   * under test to initiate a trim operation.
   *
   * This function purposely does not select a trim point that aligns with a slot boundary; it is up
   * to the VolumeTrimmer to make sure that the log is trimmed at slot boundaries.
   */
  void trim_log()
  {
    ASSERT_TRUE(this->fake_log);
    ASSERT_TRUE(this->trim_control);

    const llfs::SlotRange log_range = this->fake_log->slot_range(llfs::LogReadMode::kSpeculative);
    const llfs::SlotRange lock_range = this->trim_lock.slot_range();
    const llfs::SlotRange new_range{this->pick_usize(lock_range.lower_bound, log_range.upper_bound),
                                    log_range.upper_bound};

    LLFS_VLOG(1) << "Trimming log;" << BATT_INSPECT(log_range) << BATT_INSPECT(lock_range)
                 << BATT_INSPECT(new_range);

    this->trim_lock = BATT_OK_RESULT_OR_PANIC(this->trim_control->update_lock(
        std::move(this->trim_lock), new_range, "VolumeTrimmerTest::trim_log"));
  }

  /** \brief Appends a psuedo-randomly generated PrepareJob slot containing one or more PageId
   * roots.
   *
   * Does not append the corresponding CommitJob slot.
   */
  void prepare_one_job()
  {
    ASSERT_TRUE(this->fake_slot_writer);

    LLFS_VLOG(1) << "Generating prepare job";

    std::vector<llfs::PageId> page_ids;

    const usize n_pages = this->pick_usize(1, kNumPages * 2);
    for (usize i = 0; i < n_pages; ++i) {
      page_ids.emplace_back(llfs::PageId{this->pick_usize(0, kNumPages - 1)});
      LLFS_VLOG(1) << " -- " << page_ids.back();
    }

    batt::BoxedSeq<llfs::PageId> page_ids_seq =
        batt::as_seq(page_ids) | batt::seq::decayed() | batt::seq::boxed();

    llfs::PrepareJob prepare{
        .new_page_ids = batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed(),
        .deleted_page_ids = batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed(),
        .user_data = llfs::PackableRef{page_ids_seq},
    };

    const usize n_to_reserve = (packed_sizeof_slot(prepare) +
                                packed_sizeof_slot(batt::StaticType<llfs::PackedCommitJob>{})) *
                               2;

    batt::StatusOr<batt::Grant> slot_grant =
        this->fake_slot_writer->reserve(n_to_reserve, batt::WaitForResource::kFalse);

    if (!slot_grant.ok()) {
      LLFS_VLOG(1) << "Not enough space in the log; cancelling job";
      return;
    }

    {
      batt::Grant trim_grant =
          BATT_OK_RESULT_OR_PANIC(slot_grant->spend(packed_sizeof_slot(prepare)));
      this->trimmer->push_grant(std::move(trim_grant));
    }

    batt::StatusOr<llfs::SlotRange> prepare_slot =
        this->fake_slot_writer->append(*slot_grant, std::move(prepare));

    ASSERT_TRUE(prepare_slot.ok() || this->fake_log_has_failed());

    if (!this->job_grant) {
      this->job_grant.emplace(std::move(*slot_grant));
    } else {
      this->job_grant->subsume(std::move(*slot_grant));
    }

    if (prepare_slot.ok()) {
      LLFS_VLOG(1) << "Wrote prepare slot at " << *prepare_slot
                   << BATT_INSPECT(this->job_grant->size());

      this->pending_jobs.emplace_back(prepare_slot->lower_bound, std::move(page_ids));
    }
  }

  /** \brief Appends a CommitJob slot corresponding to a pending PrepareJob selected
   * pseudo-randomly.
   */
  void commit_one_job()
  {
    ASSERT_FALSE(this->pending_jobs.empty());
    ASSERT_TRUE(this->job_grant);
    ASSERT_TRUE(this->trimmer);

    LLFS_VLOG(1) << "Committing job";

    const usize grant_size_before = this->job_grant->size();

    const usize job_i = this->pick_usize(0, this->pending_jobs.size() - 1);

    const llfs::PackedCommitJob commit{
        .reserved_ = {},
        .prepare_slot = this->pending_jobs[job_i].first,
    };

    const llfs::slot_offset_type log_upper_bound =
        this->mem_log_device->slot_range(llfs::LogReadMode::kSpeculative).upper_bound;

    ASSERT_LT(commit.prepare_slot, log_upper_bound)
        << BATT_INSPECT(job_i) << BATT_INSPECT(this->pending_jobs.size())
        << BATT_INSPECT_RANGE(this->pending_jobs);
    ASSERT_GE(this->job_grant->size(), llfs::packed_sizeof_slot(commit) * 2);

    {
      batt::Grant trim_grant = BATT_OK_RESULT_OR_PANIC(
          this->job_grant->spend(llfs::packed_sizeof_slot(commit), batt::WaitForResource::kFalse));
      this->trimmer->push_grant(std::move(trim_grant));
    }

    batt::StatusOr<llfs::SlotRange> commit_slot =
        this->fake_slot_writer->append(*this->job_grant, std::move(commit));

    const usize grant_size_after = this->job_grant->size();
    const usize grant_size_spent = grant_size_before - grant_size_after;

    ASSERT_GE(grant_size_before, grant_size_after);

    if (!commit_slot.ok() && grant_size_spent > 0) {
      this->leaked_job_grant_size += grant_size_spent;
    }

    ASSERT_TRUE(commit_slot.ok() || this->fake_log_has_failed());

    if (commit_slot.ok()) {
      LLFS_VLOG(1) << "Wrote commit slot at " << *commit_slot
                   << " (prepare_slot=" << commit.prepare_slot << ")";

      std::swap(this->pending_jobs[job_i], this->pending_jobs.back());

      LLFS_VLOG(1) << "adding {slot=" << commit_slot->lower_bound
                   << ", page_ids=" << batt::dump_range(this->pending_jobs.back().second)
                   << "} to committed_jobs";

      this->committed_jobs.emplace(commit_slot->lower_bound,
                                   std::move(this->pending_jobs.back().second));

      this->pending_jobs.pop_back();
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::default_random_engine rng{1234567};

  std::array<i64, kNumPages> page_ref_count;

  const llfs::PackedVolumeIds volume_ids{
      .main_uuid = llfs::random_uuid(),
      .recycler_uuid = llfs::random_uuid(),
      .trimmer_uuid = llfs::random_uuid(),
  };

  llfs::VolumeMetadataRefreshInfo metadata_state;

  batt::Optional<llfs::MemoryLogDevice> mem_log_device;

  std::shared_ptr<FakeLogDevice::State> fake_log_state = nullptr;

  std::unique_ptr<llfs::LogDevice> fake_log = nullptr;

  batt::Optional<llfs::TypedSlotWriter<llfs::VolumeEventVariant>> fake_slot_writer;

  batt::FakeExecutionContext task_context;

  batt::Optional<llfs::SlotLockManager> trim_control;

  llfs::SlotReadLock trim_lock;

  batt::Optional<llfs::VolumeTrimmer::RecoveryVisitor> recovery_visitor;

  std::unique_ptr<llfs::VolumeTrimmer> trimmer;

  batt::Status trimmer_status;

  std::vector<std::pair<llfs::slot_offset_type, std::vector<llfs::PageId>>> pending_jobs;

  batt::Optional<batt::Grant> job_grant;

  usize job_grant_size = 0;
  usize leaked_job_grant_size = 0;

  std::map<llfs::slot_offset_type, std::vector<llfs::PageId>> committed_jobs;

  batt::Optional<batt::Task> trimmer_task;

  llfs::slot_offset_type verified_client_slot = 0;

  bool shutting_down = false;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// seeds to investigate:
// - 1:  seed_i == 461660
//
TEST_F(VolumeTrimmerTest, RandomizedTest)
{
  const bool kExtraTesting = batt::getenv_as<int>("LLFS_EXTRA_TESTING").value_or(0);
  const usize kNumSeeds = kExtraTesting ? 10 * 1000 * 1000 : 10 * 1000;
  const int kLogEveryN = kExtraTesting ? 10 * 1000 : 1000;
  const usize kInitialSeed = 0;
  usize crash_count = 0;

  for (usize seed_i = kInitialSeed; seed_i < kInitialSeed + kNumSeeds; seed_i += 1) {
    LLFS_LOG_INFO_EVERY_N(kLogEveryN) << "Starting new test run with empty log;"
                                      << BATT_INSPECT(seed_i) << BATT_INSPECT(crash_count);

    this->rng.seed(seed_i * 741461423ull);

    this->reset_state();
    this->open_fake_log();
    this->initialize_trimmer();

    // Main test loop: for each step, randomly take some action that advances the state of the
    // log/trimmer.
    //
    LLFS_VLOG(1) << "Entering test loop";

    for (int step_i = 0; !this->fake_log_has_failed(); ++step_i) {
      LLFS_VLOG(1) << "Step " << step_i;

      // Let the VolumeTrimmer task run.
      //
      if (this->pick_branch()) {
        LLFS_VLOG(1) << "Checking for tasks ready to run";
        batt::UniqueHandler<> action = this->task_context.pop_ready_handler([this](usize n) {
          return this->pick_usize(0, n - 1);
        });
        if (action) {
          ASSERT_NO_FATAL_FAILURE(action());
        }
      }

      // Trim the log.
      //
      if (this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->trim_log());
      }

      // Write opaque user data slot, if there is enough log space.
      //
      if (this->fake_slot_writer->pool_size() > kMaxOpaqueDataSize + kMaxSlotOverhead &&
          this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->append_opaque_data_slot()) << BATT_INSPECT(seed_i);
      }

      // Write PrepareJob slot.
      //
      if (this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->prepare_one_job());
      }

      // Write CommitJob slot, if there is a pending job.
      //
      if (!this->pending_jobs.empty() && this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->commit_one_job()) << BATT_INSPECT(seed_i);
      }

      // Simulate a crash/recovery (rate=1%)
      //
      if (this->pick_usize(1, 100) <= 1) {
        LLFS_VLOG(1) << "Simulating crash/recovery...";
        ASSERT_NO_FATAL_FAILURE(this->shutdown_trimmer()) << BATT_INSPECT(seed_i);
        ASSERT_NO_FATAL_FAILURE(this->initialize_trimmer());
        crash_count += 1;
      }
    }

    LLFS_VLOG(1) << "Exited loop; joining trimmer task";

    ASSERT_NO_FATAL_FAILURE(this->shutdown_trimmer()) << BATT_INSPECT(seed_i);
  }
}

}  // namespace
