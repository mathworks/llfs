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

#include <llfs/testing/fake_log_device.hpp>

#include <llfs/log_device_snapshot.hpp>
#include <llfs/pack_as_raw.hpp>
#include <llfs/uuid.hpp>
#include <llfs/volume_metadata_recovery_visitor.hpp>

#include <batteries/async/fake_execution_context.hpp>
#include <batteries/async/fake_executor.hpp>
#include <batteries/env.hpp>

#include <fstream>
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
constexpr usize kLogSize = 4 * kKiB;
constexpr usize kMinTTF = 5;
constexpr usize kMaxTTF = 30;  //10 * kLogSize;  //500;
constexpr usize kMinOpaqueDataSize = 1;
constexpr usize kMaxOpaqueDataSize = 100;
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
    this->page_ref_count.fill(0);
    this->pending_jobs.clear();
    this->committed_jobs.clear();
    this->snapshot = batt::None;
    this->job_grant = batt::None;
    this->verified_client_slot = 0;
    this->trim_delay_byte_count = 0;
    this->last_recovered_trim_pos = 0;
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
    LLFS_VLOG(1) << "initialize_log()";

    this->mem_log_device.emplace(kLogSize);

    llfs::TypedSlotWriter<llfs::VolumeEventVariant> slot_writer{*this->mem_log_device};

    batt::StatusOr<batt::Grant> grant = slot_writer.reserve(
        llfs::packed_sizeof_slot(this->volume_ids), batt::WaitForResource::kFalse);

    ASSERT_TRUE(grant.ok());

    batt::StatusOr<llfs::SlotRange> ids_slot = slot_writer.append(*grant, this->volume_ids);

    ASSERT_TRUE(ids_slot.ok());
    EXPECT_EQ(ids_slot->lower_bound, 0u);
  }

  /** \brief Creates a new FakeLogDevice that wraps this->mem_log_device, giving it a pseudo-random
   * time-to-failure.
   *
   * Also creates a new SlotLockManager (this->trim_control), SlotReadLock (this->trim_lock), and
   * slot writer to be used by the VolumeTrimmer (object-under-test).
   */
  void open_fake_log()
  {
    bool from_snapshot = false;
    if (this->snapshot) {
      from_snapshot = true;
      this->mem_log_device.emplace(kLogSize);
      this->mem_log_device->restore_snapshot(*snapshot, llfs::LogReadMode::kDurable);
      this->snapshot = batt::None;
    }

    llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver> factory =
        llfs::testing::make_fake_log_device_factory(*this->mem_log_device);

    this->fake_log_state = factory.state();

    llfs::VolumeMetadata metadata;
    {
      llfs::VolumeMetadataRecoveryVisitor metadata_visitor{metadata};

      batt::StatusOr<std::unique_ptr<llfs::LogDevice>> status_or_fake_log = factory.open_log_device(
          [&](llfs::LogDevice::Reader& log_reader) -> batt::StatusOr<llfs::slot_offset_type> {
            llfs::TypedSlotReader<llfs::VolumeEventVariant> slot_reader{log_reader};

            batt::StatusOr<usize> slots_read = slot_reader.run(
                batt::WaitForResource::kFalse,
                [&](const llfs::SlotParse& slot, const auto& payload) -> batt::Status {
                  BATT_REQUIRE_OK(metadata_visitor(slot, payload));
                  this->handle_recovered_slot(slot, payload);
                  return batt::OkStatus();
                });

            BATT_REQUIRE_OK(slots_read);

            return log_reader.slot_offset();
          });

      ASSERT_TRUE(status_or_fake_log.ok()) << BATT_INSPECT(status_or_fake_log.status())
                                           << BATT_INSPECT(this->fake_log_state->device_time)
                                           << BATT_INSPECT(this->fake_log_state->failure_time);

      this->fake_log = std::move(*status_or_fake_log);

      ASSERT_TRUE(metadata.ids) << BATT_INSPECT(
                                       fake_log->slot_range(llfs::LogReadMode::kSpeculative))
                                << BATT_INSPECT(from_snapshot);
    }

    // Reset the simulated device failure time.
    //
    const auto fake_log_ttf = this->pick_usize(kMinTTF, kMaxTTF);
    LLFS_VLOG(1) << BATT_INSPECT(fake_log_ttf);
    this->fake_log_state->failure_time = this->fake_log_state->device_time + fake_log_ttf;

    // Initialize the slot lock manager.
    //
    this->trim_lock.clear();
    this->pending_job_slot_lock.clear();
    this->trim_control.emplace();
    this->trim_lock = BATT_OK_RESULT_OR_PANIC(
        this->trim_control->lock_slots(this->fake_log->slot_range(llfs::LogReadMode::kSpeculative),
                                       "VolumeTrimmerTest::open_fake_log"));

    // Initialize the slot writer.
    //
    this->job_grant = batt::None;
    this->metadata_refresher = nullptr;
    this->fake_slot_writer.emplace(*this->fake_log);
    this->job_grant =
        BATT_OK_RESULT_OR_PANIC(this->fake_slot_writer->reserve(0, batt::WaitForResource::kFalse));

    // Initialize the VolumeMetadataRefresher.
    //
    this->metadata_refresher = std::make_unique<llfs::VolumeMetadataRefresher>(
        *this->fake_slot_writer, batt::make_copy(metadata));

    if (this->metadata_refresher->grant_required() > 0) {
      batt::Grant metadata_grant = BATT_OK_RESULT_OR_PANIC(this->fake_slot_writer->reserve(
          this->metadata_refresher->grant_required(), batt::WaitForResource::kFalse));

      batt::Status status = this->metadata_refresher->update_grant(metadata_grant);

      ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);

      BATT_CHECK_EQ(this->metadata_refresher->grant_required(), 0u);
    }

    // Reacquire slot locks and grant for all pending jobs.
    //
    for (const auto& [slot_offset, job_info] : this->pending_jobs) {
      ASSERT_NE(this->prepare_job_ptr.count(slot_offset), 0)
          << "PackedPrepareJob slot not found for pending job at slot: " << slot_offset;

      llfs::SlotReadLock slot_lock = BATT_OK_RESULT_OR_PANIC(this->trim_control->lock_slots(
          llfs::SlotRange{slot_offset, slot_offset + 1}, "open_fake_log()"));

      this->pending_job_slot_lock[slot_offset] =
          std::make_unique<llfs::SlotReadLock>(std::move(slot_lock));

      batt::Grant single_job_grant = BATT_OK_RESULT_OR_PANIC(this->fake_slot_writer->reserve(
          job_info.commit_slot_size, batt::WaitForResource::kFalse));

      this->job_grant->subsume(std::move(single_job_grant));
    }
  }

  /** \brief Create a VolumeTrimmer for testing.
   */
  void initialize_trimmer()
  {
    // Re-open the log and verify.
    //
    this->open_fake_log();

    ASSERT_TRUE(this->trim_control);
    ASSERT_TRUE(this->fake_log);
    ASSERT_TRUE(this->fake_slot_writer);

    batt::StatusOr<std::unique_ptr<llfs::VolumeTrimmer>> new_trimmer = llfs::VolumeTrimmer::recover(
        this->volume_ids.trimmer_uuid,                          //
        /*name=*/"TestTrimmer",                                 //
        llfs::TrimDelayByteCount{this->trim_delay_byte_count},  //
        *this->fake_log,                                        //
        *this->fake_slot_writer,                                //
        [this](auto&&... args) -> decltype(auto) {
          return this->handle_drop_roots(BATT_FORWARD(args)...);
        },
        *this->trim_control,       //
        *this->metadata_refresher  //
    );

    ASSERT_TRUE(new_trimmer.ok()) << BATT_INSPECT(new_trimmer.status());

    // Set `last_recovered_trim_pos` so we don't think that trim delay is failing to
    // arrest log trimming immediately after (re-)opening the log.
    //
    this->last_recovered_trim_pos =
        this->fake_log->slot_range(llfs::LogReadMode::kDurable).lower_bound;

    LLFS_VLOG(1) << "initialize_trimmer(): " << (void*)std::addressof(**new_trimmer);
    this->trimmer = std::move(*new_trimmer);

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
    LLFS_VLOG(1) << "shutdown_trimmer()" << BATT_INSPECT(this->trimmer->grant_pool_size());

    // Tell everything to shut down.
    //
    this->shutting_down = true;
    auto on_scope_exit = batt::finally([&] {
      this->shutting_down = false;
    });

    this->prepare_job_ptr.clear();
    this->fake_log->close().IgnoreError();
    this->trimmer->halt();
    this->trim_control->halt();

    // Before we close the MemoryLogDevice (to unblock the VolumeTrimmer task), take a snapshot so
    // it can be restored afterward.
    //
    this->snapshot.emplace(
        llfs::LogDeviceSnapshot::from_device(*this->mem_log_device, llfs::LogReadMode::kDurable));

    this->mem_log_device->close().IgnoreError();

    // Drain the executor's queue.
    //
    ASSERT_NO_FATAL_FAILURE(this->task_context.poll());

    // The trimmer task should be finished at this point.
    //
    ASSERT_TRUE(this->trimmer_task->try_join())
        << "n_tasks=" << batt::Task::backtrace_all(/*force=*/true);

    LLFS_VLOG(1) << "Trimmer task joined";
  }

  /** \brief Called in response to the VolumeTrimmer requesting that a set of PageId's be dropped
   * due to a log trim operation.
   *
   * Verifies that the correct set of page ids is specified, according to the trimmed region pointed
   * to by the VolumeTrimEvent record at client_slot.
   */
  batt::Status handle_drop_roots(boost::uuids::uuid const& trimmer_uuid,
                                 llfs::slot_offset_type client_slot,
                                 llfs::Slice<const llfs::PageId> page_ids)
  {
    EXPECT_EQ(trimmer_uuid, this->volume_ids.trimmer_uuid);

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
            [&](const llfs::SlotParse& /*slot*/, const llfs::VolumeTrimEvent& trim_event) {
              // TODO [tastolfi 2023-05-22] use/verify `slot` arg

              for (auto iter = this->committed_jobs.begin(); iter != this->committed_jobs.end();
                   iter = this->committed_jobs.erase(iter)) {
                const auto& [prepare_slot, job_info] = *iter;
                if (!llfs::slot_less_than(prepare_slot, trim_event.new_trim_pos)) {
                  break;
                }
                auto& page_ids = job_info.page_ids;

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

    llfs::PackAsRawData to_pack_as_raw{data_str};
    auto&& payload = llfs::PackableRef{to_pack_as_raw};

    const usize slot_size = llfs::packed_sizeof_slot(payload);
    batt::StatusOr<batt::Grant> slot_grant =
        this->fake_slot_writer->reserve(slot_size, batt::WaitForResource::kFalse);

    ASSERT_TRUE(slot_grant.ok() || this->fake_log_has_failed())
        << BATT_INSPECT(slot_grant.status()) << BATT_INSPECT(slot_size)
        << BATT_INSPECT(this->fake_slot_writer->pool_size());

    batt::StatusOr<llfs::SlotRange> slot_range =
        this->fake_slot_writer->append(*slot_grant, payload);

    ASSERT_TRUE(slot_range.ok() || this->fake_log_has_failed())
        << BATT_INSPECT(slot_range) << BATT_INSPECT(this->fake_log_has_failed());

    if (slot_range.ok()) {
      this->trim_control->update_upper_bound(slot_range->upper_bound);
    }

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
        .page_device_ids = batt::seq::Empty<llfs::page_device_id_int>{} | batt::seq::boxed(),
        .user_data = llfs::PackableRef{page_ids_seq},
    };

    const usize prepare_slot_size = packed_sizeof_slot(prepare);
    const usize commit_slot_size = packed_sizeof_commit_slot(prepare);
    const usize n_to_reserve = (prepare_slot_size + commit_slot_size);

    batt::StatusOr<batt::Grant> slot_grant =
        this->fake_slot_writer->reserve(n_to_reserve, batt::WaitForResource::kFalse);

    if (!slot_grant.ok()) {
      LLFS_VLOG(1) << "Not enough space in the log; cancelling job";
      return;
    }

    batt::StatusOr<llfs::SlotParseWithPayload<const llfs::PackedPrepareJob*>> prepare_slot =
        this->fake_slot_writer->typed_append(*slot_grant, std::move(prepare));

    ASSERT_TRUE(prepare_slot.ok() || this->fake_log_has_failed());

    if (prepare_slot.ok()) {
      LLFS_VLOG(1) << "Wrote prepare slot at " << *prepare_slot
                   << BATT_INSPECT(this->job_grant->size());

      if (!this->job_grant) {
        this->job_grant.emplace(std::move(*slot_grant));
      } else {
        this->job_grant->subsume(std::move(*slot_grant));
      }

      this->prepare_job_ptr[prepare_slot->slot.offset.lower_bound] = prepare_slot->payload;

      this->pending_jobs.emplace(prepare_slot->slot.offset.lower_bound,
                                 JobInfo{
                                     .page_ids = std::move(page_ids),
                                     .prepare_slot_size = prepare_slot_size,
                                     .commit_slot_size = commit_slot_size,
                                 });

      this->pending_job_slot_lock.emplace(
          prepare_slot->slot.offset.lower_bound,
          std::make_unique<llfs::SlotReadLock>(BATT_OK_RESULT_OR_PANIC(
              this->trim_control->lock_slots(prepare_slot->slot.offset, "prepare_one_job"))));
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

    const auto iter = std::next(this->pending_jobs.begin(), job_i);

    llfs::slot_offset_type prepare_slot_offset = iter->first;
    JobInfo& job_info = iter->second;

    const llfs::CommitJob commit{
        .prepare_slot = prepare_slot_offset,
        .prepare_job = this->prepare_job_ptr[prepare_slot_offset],
    };

    BATT_CHECK_NOT_NULLPTR(commit.prepare_job);

    const llfs::slot_offset_type log_upper_bound =
        this->mem_log_device->slot_range(llfs::LogReadMode::kSpeculative).upper_bound;

    ASSERT_LT(commit.prepare_slot, log_upper_bound)
        << BATT_INSPECT(job_i) << BATT_INSPECT(this->pending_jobs.size());

    const usize commit_slot_size = llfs::packed_sizeof_slot(commit);

    ASSERT_GE(this->job_grant->size(), commit_slot_size);
    EXPECT_EQ(commit_slot_size, job_info.commit_slot_size);

    batt::StatusOr<llfs::SlotRange> commit_slot =
        this->fake_slot_writer->append(*this->job_grant, std::move(commit));

    const usize grant_size_after = this->job_grant->size();

    ASSERT_GE(grant_size_before, grant_size_after);
    ASSERT_TRUE(commit_slot.ok() || this->fake_log_has_failed());

    if (commit_slot.ok()) {
      LLFS_VLOG(1) << "Wrote commit slot at " << *commit_slot
                   << " (prepare_slot=" << commit.prepare_slot << ")";

      this->committed_jobs.emplace(commit_slot->lower_bound, std::move(job_info));

      this->pending_job_slot_lock.erase(prepare_slot_offset);
      this->prepare_job_ptr.erase(prepare_slot_offset);
      this->pending_jobs.erase(iter);
    }
  }

  /** \brief Checks all invariants.
   */
  void check_invariants()
  {
    // Check to make sure that not too much of the log was trimmed.
    //
    {
      BATT_CHECK(this->trim_control);
      const llfs::slot_offset_type least_locked_slot = this->trim_control->get_lower_bound();

      BATT_CHECK_NOT_NULLPTR(this->fake_log);
      const llfs::slot_offset_type trim_pos =
          this->fake_log->slot_range(llfs::LogReadMode::kDurable).lower_bound;

      if (least_locked_slot >= this->last_recovered_trim_pos + this->trim_delay_byte_count) {
        ASSERT_GE((i64)least_locked_slot - (i64)trim_pos, (i64)this->trim_delay_byte_count)
            << BATT_INSPECT(this->trim_delay_byte_count) << BATT_INSPECT(least_locked_slot)
            << BATT_INSPECT(trim_pos) << BATT_INSPECT(this->last_recovered_trim_pos);
      } else {
        ASSERT_EQ(trim_pos, this->last_recovered_trim_pos)
            << BATT_INSPECT(least_locked_slot) << BATT_INSPECT(this->trim_delay_byte_count);
      }

      // No pending jobs should be trimmed!
      //
      for (const auto& [prepare_slot_offset, job_info] : this->pending_jobs) {
        ASSERT_TRUE(!llfs::slot_less_than(prepare_slot_offset, trim_pos))
            << BATT_INSPECT(prepare_slot_offset) << BATT_INSPECT(trim_pos);
      }
    }
  }

  /** \brief Forces the trimmer task to shut down; usually because we have failed an ASSERT.
   */
  void force_shut_down()
  {
    // Only need to do something if the trimmer_task exists.
    //
    if (!this->trimmer_task) {
      return;
    }

    // Halt everything that might be blocking the trimmer_task.
    //
    if (this->trimmer) {
      this->trimmer->halt();
    }
    if (this->trim_control) {
      this->trim_control->halt();
    }
    if (this->fake_log) {
      this->fake_log->close().IgnoreError();
    }

    // Run tasks until there are no more.
    //
    for (;;) {
      batt::UniqueHandler<> action = this->task_context.pop_ready_handler([this](usize /*n*/) {
        return 0;
      });
      if (!action) {
        break;
      }
      action();
    }

    // The trimmer_task should now be joined.
    //
    BATT_CHECK_EQ(this->trimmer_task->try_join(), batt::Task::IsDone{true});
  }

  template <typename T>
  void handle_recovered_slot(const llfs::SlotParse& slot, const T& /*event*/)
  {
    LLFS_VLOG(1) << BATT_INSPECT(slot.offset) << BATT_INSPECT(batt::name_of<T>());
  }

  void handle_recovered_slot(const llfs::SlotParse& slot,
                             const llfs::Ref<const llfs::PackedPrepareJob>& prepare)
  {
    this->prepare_job_ptr[slot.offset.lower_bound] = prepare.pointer();
  }

  void handle_recovered_slot(const llfs::SlotParse& /*slot*/,
                             const llfs::Ref<const llfs::PackedCommitJob>& commit)
  {
    this->prepare_job_ptr.erase(commit.get().prepare_slot);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct JobInfo {
    std::vector<llfs::PageId> page_ids;
    usize prepare_slot_size = 0;
    usize commit_slot_size = 0;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::default_random_engine rng{1234567};

  u64 trim_delay_byte_count = 0;

  std::array<i64, kNumPages> page_ref_count;

  const llfs::PackedVolumeIds volume_ids{
      .main_uuid = llfs::random_uuid(),
      .recycler_uuid = llfs::random_uuid(),
      .trimmer_uuid = llfs::random_uuid(),
      .trim_slot_offset = 0,
  };

  batt::Optional<llfs::MemoryLogDevice> mem_log_device;

  std::shared_ptr<FakeLogDevice::State> fake_log_state = nullptr;

  std::unique_ptr<llfs::LogDevice> fake_log = nullptr;

  batt::Optional<llfs::TypedSlotWriter<llfs::VolumeEventVariant>> fake_slot_writer;

  batt::Optional<llfs::LogDeviceSnapshot> snapshot;

  batt::FakeExecutionContext task_context;

  batt::Optional<llfs::SlotLockManager> trim_control;

  llfs::SlotReadLock trim_lock;

  std::unique_ptr<llfs::VolumeTrimmer> trimmer;

  batt::Status trimmer_status;

  std::map<llfs::slot_offset_type, JobInfo> pending_jobs;

  std::map<llfs::slot_offset_type, const llfs::PackedPrepareJob*> prepare_job_ptr;

  std::map<llfs::slot_offset_type, std::unique_ptr<llfs::SlotReadLock>> pending_job_slot_lock;

  batt::Optional<batt::Grant> job_grant;

  std::unique_ptr<llfs::VolumeMetadataRefresher> metadata_refresher;

  std::map<llfs::slot_offset_type, JobInfo> committed_jobs;

  batt::Optional<batt::Task> trimmer_task;

  llfs::slot_offset_type verified_client_slot = 0;

  llfs::slot_offset_type last_recovered_trim_pos = 0;

  bool shutting_down = false;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(VolumeTrimmerTest, RandomizedTest)
{
  const bool kExtraTesting = batt::getenv_as<int>("LLFS_EXTRA_TESTING").value_or(0);
  const usize kNumSeeds = kExtraTesting ? 10 * 1000 * 1000 : 100 * 1000;
  const int kLogEveryN = kNumSeeds / 10;
  const usize kInitialSeed = 1;
  const usize kNumThreads = 1;  //std::thread::hardware_concurrency();

  batt::Watch<usize> crash_count{0};
  batt::Watch<usize> max_log_size{0};

  // Make sure there are no running tasks when we return.
  //
  auto on_scope_exit = batt::finally([&] {
    this->force_shut_down();
  });

  for (usize thread_i = 0; thread_i < kNumThreads; ++thread_i) {
    usize local_crash_count = 0;
    usize local_max_log_size = 0;
    for (usize seed_i = kInitialSeed + thread_i; seed_i < kInitialSeed + kNumSeeds;
         seed_i += kNumThreads) {
      BATT_DEBUG_INFO(BATT_INSPECT(seed_i)
                      << BATT_INSPECT(this->fake_log->slot_range(llfs::LogReadMode::kSpeculative)));

      LLFS_LOG_INFO_EVERY_N(kLogEveryN) << "Starting new test run with empty log;"
                                        << BATT_INSPECT(seed_i) << BATT_INSPECT(local_crash_count);

      if (this->fake_log) {
        local_max_log_size = std::max(local_max_log_size, this->fake_log->size());
      }

      this->rng.seed(seed_i * 741461423ull);

      this->reset_state();

      // Pick a trim delay setting for this test run.
      //
      this->trim_delay_byte_count = this->pick_usize(0, 16) * 64;

      this->initialize_trimmer();

      std::ofstream ofs{batt::to_string("/tmp/volume_trimmer_", seed_i, ".csv")};

      // Main test loop: for each step, randomly take some action that advances the state of the
      // log/trimmer.
      //
      LLFS_VLOG(1) << "Entering test loop";

      for (int step_i = 0; !this->fake_log_has_failed(); ++step_i) {
        LLFS_VLOG(1) << "Step " << step_i;
        if (false) {
          auto& metrics = llfs::VolumeTrimmer::metrics();
          ofs << (metrics.prepare_grant_reserved_byte_count.load() -
                  metrics.prepare_grant_released_byte_count.load())
              << ", "  //
              << (metrics.commit_grant_reserved_byte_count.load() -
                  metrics.commit_grant_released_byte_count.load())
              << ", "  //
              << (metrics.attach_grant_reserved_byte_count.load() -
                  metrics.attach_grant_released_byte_count.load())
              << ", "  //
              << (metrics.ids_grant_reserved_byte_count.load() -
                  metrics.ids_grant_released_byte_count.load())
              << ", "  //
              << (metrics.trim_grant_reserved_byte_count.load() -
                  metrics.trim_grant_released_byte_count.load())
              << std::endl;
        }
        ASSERT_NO_FATAL_FAILURE(this->check_invariants());

        // Let the VolumeTrimmer task run.
        //
        if (this->pick_branch()) {
          LLFS_VLOG(1) << "Checking for tasks ready to run";
          batt::UniqueHandler<> action = this->task_context.pop_ready_handler([this](usize n) {
            return this->pick_usize(0, n - 1);
          });
          if (action) {
            ASSERT_NO_FATAL_FAILURE(action());
            ASSERT_NO_FATAL_FAILURE(this->check_invariants());
          }
        }

        // Trim the log.
        //
        if (this->pick_branch()) {
          ASSERT_NO_FATAL_FAILURE(this->trim_log());
          ASSERT_NO_FATAL_FAILURE(this->check_invariants());
        }

        // Write opaque user data slot, if there is enough log space.
        //
        if (this->fake_slot_writer->pool_size() > kMaxOpaqueDataSize + kMaxSlotOverhead &&
            this->pick_branch()) {
          ASSERT_NO_FATAL_FAILURE(this->append_opaque_data_slot()) << BATT_INSPECT(seed_i);
          ASSERT_NO_FATAL_FAILURE(this->check_invariants());
        }

        // Write PrepareJob slot.
        //
        if (this->pick_branch()) {
          ASSERT_NO_FATAL_FAILURE(this->prepare_one_job());
          ASSERT_NO_FATAL_FAILURE(this->check_invariants());
        }

        // Write CommitJob slot, if there is a pending job.
        //
        if (!this->pending_jobs.empty() && this->pick_branch()) {
          ASSERT_NO_FATAL_FAILURE(this->commit_one_job()) << BATT_INSPECT(seed_i);
          ASSERT_NO_FATAL_FAILURE(this->check_invariants());
        }

        // Simulate a crash/recovery (rate=1%)
        //
        if (this->pick_usize(1, 100) <= 5) {
          BATT_DEBUG_INFO(BATT_INSPECT(this->fake_log_has_failed())
                          << BATT_INSPECT(this->fake_log_state->device_time)
                          << BATT_INSPECT(this->fake_log_state->failure_time));

          LLFS_VLOG(1) << "Simulating crash/recovery..."
                       << BATT_INSPECT(this->last_recovered_trim_pos)
                       << BATT_INSPECT(this->fake_log->slot_range(llfs::LogReadMode::kDurable))
                       << BATT_INSPECT(this->trim_control->get_lower_bound())
                       << BATT_INSPECT(this->fake_log->size())
                       << BATT_INSPECT(this->fake_log->space())
                       << BATT_INSPECT(this->fake_log->capacity());

          ASSERT_NO_FATAL_FAILURE(this->shutdown_trimmer()) << BATT_INSPECT(seed_i);
          ASSERT_NO_FATAL_FAILURE(this->check_invariants());

          ASSERT_NO_FATAL_FAILURE(this->initialize_trimmer());
          ASSERT_NO_FATAL_FAILURE(this->check_invariants());

          local_crash_count += 1;
        }
      }

      LLFS_VLOG(1) << "Exited loop; joining trimmer task";

      ASSERT_NO_FATAL_FAILURE(this->shutdown_trimmer()) << BATT_INSPECT(seed_i);
      ASSERT_NO_FATAL_FAILURE(this->check_invariants());
    }
    crash_count.fetch_add(local_crash_count);
    max_log_size.clamp_min_value(local_max_log_size);
  }

  LLFS_LOG_INFO() << BATT_INSPECT(max_log_size) << BATT_INSPECT(kLogSize);
}

}  // namespace
