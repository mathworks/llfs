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

#include <llfs/pack_as_raw.hpp>
#include <llfs/testing/fake_log_device.hpp>
#include <llfs/uuid.hpp>

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
//   - 2 devices, 4 pages per device, max ref count 5 per page

constexpr usize kNumPages = 4;
constexpr usize kLogSize = 64 * kKiB;
constexpr usize kMinTTF = 4;
constexpr usize kMaxTTF = 100;
constexpr usize kMinOpaqueDataSize = 1;
constexpr usize kMaxOpaqueDataSize = 10;
constexpr usize kNumSeeds = 10;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeTrimmerTest : public ::testing::Test
{
 public:
  VolumeTrimmerTest()
  {
  }

  void reset_state()
  {
    this->page_ref_count.fill(0);
    this->pending_jobs.clear();
    this->job_grant = batt::None;
  }

  usize pick_usize(usize min_value, usize max_value)
  {
    std::uniform_int_distribution<usize> pick{min_value, max_value};
    return pick(this->rng);
  }

  bool pick_branch()
  {
    return this->pick_usize(0, 1) == 1;
  }

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

  void open_fake_log()
  {
    llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver> factory =
        llfs::testing::make_fake_log_device_factory(*this->mem_log_device);

    this->fake_log_state = factory.state();

    llfs::VolumeTrimmer::RecoveryVisitor recovery_visitor;

    {
      batt::StatusOr<std::unique_ptr<llfs::LogDevice>> status_or_fake_log = factory.open_log_device(
          [&](llfs::LogDevice::Reader& log_reader) -> batt::StatusOr<llfs::slot_offset_type> {
            llfs::TypedSlotReader<llfs::VolumeEventVariant> slot_reader{log_reader};

            batt::StatusOr<usize> slots_read = slot_reader.run(
                batt::WaitForResource::kFalse,
                [&](const llfs::SlotParse& slot, const auto& payload) -> batt::Status {
                  recovery_visitor(slot, payload);
                  return batt::OkStatus();
                });

            BATT_REQUIRE_OK(slots_read);

            return log_reader.slot_offset();
          });

      ASSERT_TRUE(status_or_fake_log.ok());
      this->fake_log = std::move(*status_or_fake_log);
    }

    EXPECT_EQ(this->metadata_state.most_recent_ids_slot != batt::None,
              recovery_visitor.get_refresh_info().most_recent_ids_slot != batt::None)
        << BATT_INSPECT(this->metadata_state) << BATT_INSPECT(recovery_visitor.get_refresh_info());

    // Reset the simulated device failure time.
    //
    const auto fake_log_ttf = this->pick_usize(kMinTTF, kMaxTTF);
    LOG(INFO) << BATT_INSPECT(fake_log_ttf);
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
    this->job_grant = batt::None;
    this->fake_slot_writer.emplace(*this->fake_log);
  }

  batt::Status handle_drop_roots(llfs::slot_offset_type client_slot,
                                 llfs::Slice<const llfs::PageId> page_ids)
  {
    BATT_PANIC() << "Implement me!";
    return batt::StatusCode::kUnimplemented;
  }

  bool fake_log_has_failed() const
  {
    return this->fake_log_state &&
           this->fake_log_state->device_time >= this->fake_log_state->failure_time;
  }

  void append_opaque_data_slot()
  {
    ASSERT_TRUE(this->fake_log);
    ASSERT_TRUE(this->fake_slot_writer);
    ASSERT_TRUE(this->trim_control);

    const usize data_size = this->pick_usize(kMinOpaqueDataSize, kMaxOpaqueDataSize);
    std::vector<char> buffer(data_size, 'a');
    std::string_view data_str{buffer.data(), buffer.size()};

    auto&& payload = llfs::PackableRef{llfs::pack_as_raw(data_str)};

    batt::StatusOr<batt::Grant> slot_grant = this->fake_slot_writer->reserve(
        llfs::packed_sizeof_slot(payload), batt::WaitForResource::kFalse);

    ASSERT_TRUE(slot_grant.ok() || this->fake_log_has_failed());

    batt::StatusOr<llfs::SlotRange> slot_range =
        this->fake_slot_writer->append(*slot_grant, payload);

    ASSERT_TRUE(slot_range.ok() || this->fake_log_has_failed());

    this->trim_control->update_upper_bound(slot_range->upper_bound);

    LOG(INFO) << "Appended opaque data: " << batt::c_str_literal(data_str);
  }

  void trim_log()
  {
    ASSERT_TRUE(this->fake_log);
    ASSERT_TRUE(this->trim_control);

    const llfs::SlotRange log_range = this->fake_log->slot_range(llfs::LogReadMode::kSpeculative);
    const llfs::SlotRange lock_range = this->trim_lock.slot_range();
    const llfs::SlotRange new_range{this->pick_usize(lock_range.lower_bound, log_range.upper_bound),
                                    log_range.upper_bound};

    LOG(INFO) << "Trimming log;" << BATT_INSPECT(log_range) << BATT_INSPECT(lock_range)
              << BATT_INSPECT(new_range);

    this->trim_lock = BATT_OK_RESULT_OR_PANIC(this->trim_control->update_lock(
        std::move(this->trim_lock), new_range, "VolumeTrimmerTest::trim_log"));
  }

  void prepare_one_job()
  {
    ASSERT_TRUE(this->fake_slot_writer);

    LLFS_LOG_INFO() << "Generating prepare job";

    std::vector<llfs::PageId> page_ids;

    const usize n_pages = this->pick_usize(1, kNumPages * 2);
    for (usize i = 0; i < n_pages; ++i) {
      page_ids.emplace_back(llfs::PageId{this->pick_usize(0, kNumPages - 1)});
      LLFS_LOG_INFO() << " -- " << page_ids.back();
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
      LLFS_LOG_INFO() << "Not enough space in the log; cancelling job";
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
      LLFS_LOG_INFO() << "Wrote prepare slot at " << *prepare_slot
                      << BATT_INSPECT(this->job_grant->size());
    }

    this->pending_jobs.emplace_back(prepare_slot->lower_bound, std::move(page_ids));
  }

  void commit_one_job()
  {
    ASSERT_FALSE(this->pending_jobs.empty());
    ASSERT_TRUE(this->job_grant);
    ASSERT_TRUE(this->trimmer);

    LLFS_LOG_INFO() << "Committing job";

    const usize job_i = this->pick_usize(0, this->pending_jobs.size() - 1);

    const llfs::PackedCommitJob commit{
        .reserved_ = {},
        .prepare_slot = pending_jobs[job_i].first,
    };

    ASSERT_GE(this->job_grant->size(), llfs::packed_sizeof_slot(commit) * 2);

    {
      batt::Grant trim_grant = BATT_OK_RESULT_OR_PANIC(
          this->job_grant->spend(llfs::packed_sizeof_slot(commit), batt::WaitForResource::kFalse));
      this->trimmer->push_grant(std::move(trim_grant));
    }

    batt::StatusOr<llfs::SlotRange> commit_slot =
        this->fake_slot_writer->append(*this->job_grant, std::move(commit));

    ASSERT_TRUE(commit_slot.ok() || this->fake_log_has_failed());

    if (commit_slot.ok()) {
      LLFS_LOG_INFO() << "Wrote commit slot at " << *commit_slot
                      << " (prepare_slot=" << commit.prepare_slot << ")";
    }

    std::swap(pending_jobs[job_i], pending_jobs.back());
    pending_jobs.pop_back();
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

  std::unique_ptr<llfs::VolumeTrimmer> trimmer;

  batt::Status trimmer_status;

  std::vector<std::pair<llfs::slot_offset_type, std::vector<llfs::PageId>>> pending_jobs;

  batt::Optional<batt::Grant> job_grant;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(VolumeTrimmerTest, RandomizedTest)
{
  for (usize seed_i = 0; seed_i < kNumSeeds; seed_i += 1) {
    LOG(INFO) << BATT_INSPECT(seed_i);

    this->rng.seed(seed_i * 29678352935678ull);

    this->reset_state();
    this->initialize_log();
    this->open_fake_log();

    this->trimmer = std::make_unique<llfs::VolumeTrimmer>(
        this->volume_ids.trimmer_uuid, *this->trim_control,
        this->fake_log->new_reader(/*slot_lower_bound=*/batt::None,
                                   llfs::LogReadMode::kSpeculative),
        *this->fake_slot_writer, [this](auto&&... args) -> decltype(auto) {
          return this->handle_drop_roots(BATT_FORWARD(args)...);
        });

    EXPECT_EQ(this->trimmer->uuid(), this->volume_ids.trimmer_uuid);

    LOG(INFO) << "Starting trimmer task";

    batt::Task trimmer_task{this->task_context.get_executor(), [this] {
                              this->trimmer_status = this->trimmer->run();
                            }};

    LOG(INFO) << "Entering test loop";

    for (int step_i = 0; !this->fake_log_has_failed(); ++step_i) {
      LOG(INFO) << "Step " << step_i;

      if (this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->trim_log());
      }

      if (this->fake_log->space() > kMaxOpaqueDataSize && this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->append_opaque_data_slot());
      }

      if (this->pick_branch()) {
        LOG(INFO) << "Checking for tasks ready to run";
        this->task_context.poll_one();
      }

      if (this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->prepare_one_job());
      }

      if (!this->pending_jobs.empty() && this->pick_branch()) {
        ASSERT_NO_FATAL_FAILURE(this->commit_one_job());
      }
    }

    LOG(INFO) << "Exited loop; joining trimmer task";

    // Tell everything to shut down.
    //
    this->fake_log->close().IgnoreError();
    this->trimmer->halt();
    this->trim_control->halt();

    // Drain the executor's queue.
    //
    this->task_context.poll();

    // The trimmer task should be finished at this point.
    //
    EXPECT_TRUE(trimmer_task.try_join());

    LOG(INFO) << "Trimmer task joined";

    // Re-open the log and verify.
    //
    this->open_fake_log();
  }
}

}  // namespace
