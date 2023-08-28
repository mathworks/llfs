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
#include <batteries/small_vec.hpp>

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
constexpr usize kMaxTTF = 30;
constexpr usize kMinOpaqueDataSize = 1;
constexpr usize kMaxOpaqueDataSize = 100;
constexpr usize kMaxSlotOverhead = 32;
constexpr int kStepLimit = 100 * 1000;
constexpr usize kDefaultPageIdsVecSize = 16;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeTrimmerTest : public ::testing::Test
{
 public:
  struct Config {
    const llfs::PackedVolumeIds volume_ids{
        .main_uuid = llfs::random_uuid(),
        .recycler_uuid = llfs::random_uuid(),
        .trimmer_uuid = llfs::random_uuid(),
    };  // ----
  };

  struct JobInfo {
    batt::SmallVec<llfs::PageId, kDefaultPageIdsVecSize> page_ids;

    llfs::slot_offset_type prepare_slot_offset;

    usize prepare_slot_size = 0;

    batt::Optional<llfs::slot_offset_type> commit_slot_offset;

    usize commit_slot_size = 0;
  };

  struct SimRun {
    std::default_random_engine rng;

    u64 trim_delay_byte_count = 0;

    llfs::slot_offset_type highest_seen_drop_pages_client_slot = 0;

    std::map<llfs::slot_offset_type, JobInfo> pending_jobs;

    std::map<llfs::slot_offset_type, JobInfo> committed_jobs;

    llfs::LogDeviceSnapshot log_snapshot;

    //----- --- -- -  -  -   -

    /** \brief Resets all test state variables to their initial values and sets the random number
     * generator seed.
     */
    void reset(unsigned random_seed, const Config& config);

    /** \brief Returns a pseudo-random usize value r, where r >= min_value and r <= max_value.
     */
    usize pick_usize(usize min_value, usize max_value);

    /** \brief Returns a pseudo-random boolean value.
     */
    bool pick_branch();
  };

  struct LogSession {
    SimRun* sim = nullptr;

    batt::Optional<llfs::MemoryLogDevice> mem_device;

    std::shared_ptr<FakeLogDevice::State> fake_device_state = nullptr;

    std::unique_ptr<llfs::LogDevice> fake_device = nullptr;

    std::map<llfs::slot_offset_type, const llfs::PackedPrepareJob*> prepare_job_ptr;

    batt::Optional<llfs::SlotLockManager> trim_control;

    llfs::SlotReadLock trim_lock;

    batt::Optional<llfs::TypedSlotWriter<llfs::VolumeEventVariant>> slot_writer;

    batt::Optional<batt::Grant> job_grant;

    batt::Optional<llfs::VolumeMetadataRefresher> metadata_refresher;

    std::map<llfs::slot_offset_type, llfs::SlotReadLock> pending_job_slot_lock;

    bool is_initialized = false;

    //----- --- -- -  -  -   -

    void initialize(const Config& config, SimRun& sim);

    /** \brief Called inside this->initialize to recover session state (this->prepare_job_ptr) from
     * the log.
     */
    template <typename T>
    void handle_recovered_slot(const llfs::SlotParse& slot, const T& /*event*/);

    void handle_recovered_slot(const llfs::SlotParse& slot, const llfs::VolumeTrimEvent& trim);

    void handle_recovered_slot(const llfs::SlotParse& slot,
                               const llfs::Ref<const llfs::PackedPrepareJob>& prepare);

    void handle_recovered_slot(const llfs::SlotParse& /*slot*/,
                               const llfs::Ref<const llfs::PackedCommitJob>& commit);

    /** \brief Returns true iff a fake log has been created and the simulated device time is at
     * least the planned failure time.
     */
    bool fake_log_has_failed() const;

    /** \brief Appends an application-level opaque data slot to the fake log and updates the
     * SlotLockManager's upper bound.
     */
    void append_opaque_data_slot();

    /** \brief Selects a pseudo-random trim point and updates the trim lock to cause the
     * VolumeTrimmer under test to initiate a trim operation.
     *
     * This function purposely does not select a trim point that aligns with a slot boundary; it is
     * up to the VolumeTrimmer to make sure that the log is trimmed at slot boundaries.
     */
    void trim_log();

    /** \brief Appends a psuedo-randomly generated PrepareJob slot containing one or more PageId
     * roots.
     *
     * Does not append the corresponding CommitJob slot.
     *
     * If there isn't enough space in the log to reserve Grant for both prepare and commit slots,
     * this function returns without changing anything.
     */
    void prepare_one_job();

    /** \brief Appends a CommitJob slot corresponding to a pending PrepareJob selected
     * pseudo-randomly.
     *
     * If there are no pending jobs, this function has no effect.
     */
    void commit_one_job();

    /** \brief Sets the log and associated objects in a "closed"/"halted" state so that the
     * trimmer task will be unblocked; doesn't tear down everything yet (that happens in
     * this->terminate()).
     */
    void halt();

    void terminate();
  };

  struct TrimmerSession {
    const Config* config = nullptr;

    SimRun* sim = nullptr;

    LogSession* log = nullptr;

    bool shutting_down = false;

    std::unique_ptr<llfs::VolumeTrimmer> trimmer;

    llfs::slot_offset_type last_recovered_trim_pos = 0;

    batt::FakeExecutionContext task_context;

    batt::Optional<batt::Task> trimmer_task;

    batt::Status trimmer_task_exit_status;

    //----- --- -- -  -  -   -

    /** \brief Create a VolumeTrimmer for testing.
     */
    void initialize(const Config& config, SimRun& sim, LogSession& log);

    /** \brief Checks all invariants.  Uses GTEST ASSERT* statements to verify.
     */
    void check_invariants();

    /** \brief Called in response to the VolumeTrimmer requesting that a set of PageId's be dropped
     * due to a log trim operation.
     *
     * Verifies that the correct set of page ids is specified, according to the trimmed region
     * pointed to by the VolumeTrimEvent record at client_slot.
     */
    batt::Status handle_drop_roots(boost::uuids::uuid const& trimmer_uuid,
                                   llfs::slot_offset_type client_slot,
                                   llfs::Slice<const llfs::PageId> page_ids);

    /** \brief Runs a random unit of work from the task_context.
     */
    void run_one();

    /** \brief Stop the current VolumeTrimmer.
     */
    void terminate();
  };
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::SimRun::reset(unsigned random_seed, const Config& config)
{
  this->rng.seed(random_seed);

  // Prime the rng.
  //
  for (usize i = 0; i < 11; ++i) {
    (void)this->rng();
  }

  this->trim_delay_byte_count = this->pick_usize(0, 16) * 64;
  this->highest_seen_drop_pages_client_slot = 0;
  this->pending_jobs.clear();
  this->committed_jobs.clear();

  // Create the initial log snapshot by creating a new MemoryLogDevice and initializing it with
  // minimal llfs::Volume metadata.
  //
  llfs::MemoryLogDevice mem_log_device{kLogSize};

  llfs::TypedSlotWriter<llfs::VolumeEventVariant> slot_writer{mem_log_device};

  batt::StatusOr<batt::Grant> grant = slot_writer.reserve(
      llfs::packed_sizeof_slot(config.volume_ids), batt::WaitForResource::kFalse);

  ASSERT_TRUE(grant.ok());

  batt::StatusOr<llfs::SlotRange> ids_slot = slot_writer.append(*grant, config.volume_ids);

  ASSERT_TRUE(ids_slot.ok());
  EXPECT_EQ(ids_slot->lower_bound, 0u);

  this->log_snapshot =
      llfs::LogDeviceSnapshot::from_device(mem_log_device, llfs::LogReadMode::kDurable);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize VolumeTrimmerTest::SimRun::pick_usize(usize min_value, usize max_value)
{
  std::uniform_int_distribution<usize> pick{min_value, max_value};
  return pick(this->rng);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool VolumeTrimmerTest::SimRun::pick_branch()
{
  return this->pick_usize(0, 1) == 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::initialize(const Config& config, SimRun& sim)
{
  BATT_CHECK_EQ(this->sim, nullptr);

  this->sim = std::addressof(sim);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Initialize the MemoryLogDevice from the current snapshot.
  //
  {
    BATT_CHECK_EQ(this->mem_device, batt::None);
    BATT_CHECK(sim.log_snapshot);

    this->mem_device.emplace(kLogSize, sim.log_snapshot, llfs::LogReadMode::kDurable);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Initialize the FakeLogDevice.
  //
  llfs::VolumeMetadata metadata;
  {
    llfs::VolumeMetadataRecoveryVisitor metadata_visitor{metadata};

    llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver> factory =
        llfs::testing::make_fake_log_device_factory(*this->mem_device);

    this->fake_device_state = factory.state();

    BATT_CHECK(this->prepare_job_ptr.empty());

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
                                         << BATT_INSPECT(this->fake_device_state->device_time)
                                         << BATT_INSPECT(this->fake_device_state->failure_time);

    this->fake_device = std::move(*status_or_fake_log);

    ASSERT_TRUE(metadata.ids) << BATT_INSPECT(
        fake_device->slot_range(llfs::LogReadMode::kSpeculative));

    EXPECT_EQ(metadata.ids->main_uuid, config.volume_ids.main_uuid);
    EXPECT_EQ(metadata.ids->recycler_uuid, config.volume_ids.recycler_uuid);
    EXPECT_EQ(metadata.ids->trimmer_uuid, config.volume_ids.trimmer_uuid);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Reset the simulated device failure time.
  //
  {
    const auto fake_log_ttf = this->sim->pick_usize(kMinTTF, kMaxTTF);
    LLFS_VLOG(1) << BATT_INSPECT(fake_log_ttf);
    this->fake_device_state->failure_time = this->fake_device_state->device_time + fake_log_ttf;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Create a new SlotLockManager.
  //
  {
    BATT_CHECK_EQ(this->trim_control, batt::None);
    this->trim_control.emplace();

    BATT_CHECK(!this->trim_lock);
    this->trim_lock = BATT_OK_RESULT_OR_PANIC(this->trim_control->lock_slots(
        this->fake_device->slot_range(llfs::LogReadMode::kSpeculative),
        "VolumeTrimmerTest::LogSession::initialize"));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Create a SlotWriter and initialize the job_grant.
  //
  {
    BATT_CHECK_EQ(this->slot_writer, batt::None);
    BATT_CHECK_NOT_NULLPTR(this->fake_device);

    this->slot_writer.emplace(*this->fake_device);

    BATT_CHECK_EQ(this->job_grant, batt::None);

    this->job_grant.emplace(
        BATT_OK_RESULT_OR_PANIC(this->slot_writer->reserve(0, batt::WaitForResource::kFalse)));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Reacquire slot locks and grant for all pending jobs.
  //
  {
    BATT_CHECK(this->pending_job_slot_lock.empty());

    for (const auto& [slot_offset, job_info] : this->sim->pending_jobs) {
      ASSERT_NE(this->prepare_job_ptr.count(slot_offset), 0)
          << "PackedPrepareJob slot not found for pending job at slot: " << slot_offset;

      llfs::SlotReadLock slot_lock = BATT_OK_RESULT_OR_PANIC(this->trim_control->lock_slots(
          llfs::SlotRange{slot_offset, slot_offset + 1}, "open_fake_log()"));

      this->pending_job_slot_lock[slot_offset] = std::move(slot_lock);

      batt::Grant single_job_grant = BATT_OK_RESULT_OR_PANIC(
          this->slot_writer->reserve(job_info.commit_slot_size, batt::WaitForResource::kFalse));

      this->job_grant->subsume(std::move(single_job_grant));
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Initialize the VolumeMetadataRefresher.
  //
  {
    BATT_CHECK_EQ(this->metadata_refresher, batt::None);
    BATT_CHECK_NE(this->slot_writer, batt::None);

    this->metadata_refresher.emplace(*this->slot_writer, batt::make_copy(metadata));

    // Reserve some grant for refreshing metadata.
    //
    if (this->metadata_refresher->grant_required() > 0) {
      batt::StatusOr<batt::Grant> metadata_grant =
          this->slot_writer->reserve(std::min<usize>(this->slot_writer->pool_size(),
                                                     this->metadata_refresher->grant_required()),
                                     batt::WaitForResource::kFalse);

      ASSERT_TRUE(metadata_grant.ok()) << BATT_INSPECT(metadata_grant.status());

      batt::Status update_status = this->metadata_refresher->update_grant_partial(*metadata_grant);

      ASSERT_TRUE(update_status.ok()) << BATT_INSPECT(update_status);
    }
  }

  // Done!
  //
  this->is_initialized = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
void VolumeTrimmerTest::LogSession::handle_recovered_slot(const llfs::SlotParse& slot,
                                                          const T& /*event*/)
{
  LLFS_VLOG(1) << BATT_INSPECT(slot.offset) << BATT_INSPECT(batt::name_of<T>());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::handle_recovered_slot(const llfs::SlotParse& slot,
                                                          const llfs::VolumeTrimEvent& trim)
{
  using T = std::decay_t<decltype(trim)>;

  LLFS_VLOG(1) << BATT_INSPECT(slot.offset) << BATT_INSPECT(batt::name_of<T>()) << "; " << trim;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::handle_recovered_slot(
    const llfs::SlotParse& slot, const llfs::Ref<const llfs::PackedPrepareJob>& prepare)
{
  using T = std::decay_t<decltype(prepare)>;

  LLFS_VLOG(1) << BATT_INSPECT(slot.offset) << BATT_INSPECT(batt::name_of<T>());

  this->prepare_job_ptr[slot.offset.lower_bound] = prepare.pointer();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::handle_recovered_slot(
    const llfs::SlotParse& slot, const llfs::Ref<const llfs::PackedCommitJob>& commit)
{
  using T = std::decay_t<decltype(commit)>;

  LLFS_VLOG(1) << BATT_INSPECT(slot.offset) << BATT_INSPECT(batt::name_of<T>());

  this->prepare_job_ptr.erase(commit.get().prepare_slot_offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool VolumeTrimmerTest::LogSession::fake_log_has_failed() const
{
  return this->fake_device_state &&
         this->fake_device_state->device_time >= this->fake_device_state->failure_time;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::append_opaque_data_slot()
{
  ASSERT_TRUE(this->sim);
  ASSERT_TRUE(this->fake_device);
  ASSERT_TRUE(this->slot_writer);
  ASSERT_TRUE(this->trim_control);

  const usize data_size = this->sim->pick_usize(kMinOpaqueDataSize, kMaxOpaqueDataSize);
  batt::SmallVec<char, kMaxOpaqueDataSize> buffer(data_size, 'a');
  std::string_view data_str{buffer.data(), buffer.size()};

  llfs::PackAsRawData to_pack_as_raw{data_str};
  auto&& payload = llfs::PackableRef{to_pack_as_raw};

  const usize slot_size = llfs::packed_sizeof_slot(payload);
  batt::StatusOr<batt::Grant> slot_grant =
      this->slot_writer->reserve(slot_size, batt::WaitForResource::kFalse);

  ASSERT_TRUE(slot_grant.ok() || this->fake_log_has_failed())
      << BATT_INSPECT(slot_grant.status()) << BATT_INSPECT(slot_size)
      << BATT_INSPECT(this->slot_writer->pool_size());

  batt::StatusOr<llfs::SlotRange> slot_range = this->slot_writer->append(*slot_grant, payload);

  ASSERT_TRUE(slot_range.ok() || this->fake_log_has_failed())
      << BATT_INSPECT(slot_range) << BATT_INSPECT(this->fake_log_has_failed());

  if (slot_range.ok()) {
    this->trim_control->update_upper_bound(slot_range->upper_bound);
  }

  LLFS_VLOG(1) << "Appended opaque data: " << batt::c_str_literal(data_str);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::trim_log()
{
  BATT_CHECK_NOT_NULLPTR(this->sim);

  ASSERT_TRUE(this->fake_device);
  ASSERT_TRUE(this->trim_control);

  const llfs::SlotRange log_range = this->fake_device->slot_range(llfs::LogReadMode::kSpeculative);
  const llfs::SlotRange lock_range = this->trim_lock.slot_range();
  const llfs::SlotRange new_range{
      this->sim->pick_usize(lock_range.lower_bound, log_range.upper_bound), log_range.upper_bound};

  LLFS_VLOG(1) << "Trimming log;" << BATT_INSPECT(log_range) << BATT_INSPECT(lock_range)
               << BATT_INSPECT(new_range);

  this->trim_lock = BATT_OK_RESULT_OR_PANIC(this->trim_control->update_lock(
      std::move(this->trim_lock), new_range, "VolumeTrimmerTest::trim_log"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::prepare_one_job()
{
  ASSERT_TRUE(this->slot_writer);

  LLFS_VLOG(1) << "Generating prepare job";

  batt::SmallVec<llfs::PageId, kDefaultPageIdsVecSize> page_ids;

  const usize n_pages = this->sim->pick_usize(1, kNumPages * 2);
  for (usize i = 0; i < n_pages; ++i) {
    page_ids.emplace_back(llfs::PageId{this->sim->pick_usize(0, kNumPages - 1)});
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
      this->slot_writer->reserve(n_to_reserve, batt::WaitForResource::kFalse);

  if (!slot_grant.ok()) {
    LLFS_VLOG(1) << "Not enough space in the log; cancelling job";
    return;
  }

  batt::StatusOr<llfs::SlotParseWithPayload<const llfs::PackedPrepareJob*>> prepare_slot =
      this->slot_writer->typed_append(*slot_grant, std::move(prepare));

  ASSERT_TRUE(prepare_slot.ok() || this->fake_log_has_failed());

  if (prepare_slot.ok()) {
    LLFS_VLOG(1) << "Wrote prepare slot at " << *prepare_slot
                 << BATT_INSPECT(this->job_grant->size())
                 << BATT_INSPECT(this->slot_writer->pool_size());

    BATT_CHECK(this->job_grant);
    this->job_grant->subsume(std::move(*slot_grant));

    this->prepare_job_ptr[prepare_slot->slot.offset.lower_bound] = prepare_slot->payload;

    this->sim->pending_jobs.emplace(
        prepare_slot->slot.offset.lower_bound,
        JobInfo{
            .page_ids = std::move(page_ids),
            .prepare_slot_offset = prepare_slot->slot.offset.lower_bound,
            .prepare_slot_size = prepare_slot_size,
            .commit_slot_offset = batt::None,
            .commit_slot_size = commit_slot_size,
        });

    this->pending_job_slot_lock.emplace(prepare_slot->slot.offset.lower_bound,
                                        BATT_OK_RESULT_OR_PANIC(this->trim_control->lock_slots(
                                            prepare_slot->slot.offset, "prepare_one_job")));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::commit_one_job()
{
  BATT_CHECK_NOT_NULLPTR(this->sim);

  if (this->sim->pending_jobs.empty()) {
    return;
  }

  ASSERT_TRUE(this->job_grant);

  LLFS_VLOG(1) << "Committing job";

  const usize grant_size_before = this->job_grant->size();

  const usize job_i = this->sim->pick_usize(0, this->sim->pending_jobs.size() - 1);

  const auto iter = std::next(this->sim->pending_jobs.begin(), job_i);

  llfs::slot_offset_type prepare_slot_offset = iter->first;
  JobInfo& job_info = iter->second;

  const llfs::CommitJob commit{
      .prepare_slot_offset = prepare_slot_offset,
      .packed_prepare = this->prepare_job_ptr[prepare_slot_offset],
  };

  BATT_CHECK_NOT_NULLPTR(commit.packed_prepare);

  const llfs::slot_offset_type log_upper_bound =
      this->mem_device->slot_range(llfs::LogReadMode::kSpeculative).upper_bound;

  ASSERT_LT(commit.prepare_slot_offset, log_upper_bound)
      << BATT_INSPECT(job_i) << BATT_INSPECT(this->sim->pending_jobs.size());

  const usize commit_slot_size = llfs::packed_sizeof_slot(commit);

  ASSERT_GE(this->job_grant->size(), commit_slot_size);
  EXPECT_EQ(commit_slot_size, job_info.commit_slot_size);

  batt::StatusOr<llfs::SlotRange> commit_slot =
      this->slot_writer->append(*this->job_grant, std::move(commit));

  const usize grant_size_after = this->job_grant->size();

  ASSERT_GE(grant_size_before, grant_size_after);
  ASSERT_TRUE(commit_slot.ok() || this->fake_log_has_failed());

  if (commit_slot.ok()) {
    LLFS_VLOG(1) << "Wrote commit slot at " << *commit_slot
                 << " (prepare_slot=" << commit.prepare_slot_offset << ")";

    job_info.commit_slot_offset = commit_slot->lower_bound;

    this->sim->committed_jobs.emplace(commit_slot->lower_bound, std::move(job_info));

    this->pending_job_slot_lock.erase(prepare_slot_offset);
    this->prepare_job_ptr.erase(prepare_slot_offset);
    this->sim->pending_jobs.erase(iter);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::halt()
{
  this->fake_device->close().IgnoreError();
  this->trim_control->halt();

  // Before we close the MemoryLogDevice (to unblock the VolumeTrimmer task), take a snapshot so
  // it can be restored afterward.
  //
  this->sim->log_snapshot =
      llfs::LogDeviceSnapshot::from_device(*this->mem_device, llfs::LogReadMode::kDurable);

  this->mem_device->close().IgnoreError();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::LogSession::terminate()
{
  this->is_initialized = false;

  // Tear down the slot lock state.
  //
  this->pending_job_slot_lock.clear();
  this->trim_lock.clear();
  this->trim_control = batt::None;

  // Tear down the slot writer.
  //
  this->metadata_refresher = batt::None;
  this->job_grant = batt::None;
  this->slot_writer = batt::None;

  // Release all pointers into the log buffer.
  //
  this->prepare_job_ptr.clear();

  // Tear down the log device.
  //
  this->fake_device = nullptr;
  this->fake_device_state = nullptr;
  this->mem_device = batt::None;

  // Reset pointers.
  //
  this->sim = nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::TrimmerSession::initialize(const Config& config, SimRun& sim,
                                                   LogSession& log)
{
  this->shutting_down = false;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Set config, sim run, log session pointers.
  //
  {
    BATT_CHECK_EQ(this->config, nullptr);

    this->config = std::addressof(config);

    BATT_CHECK_EQ(this->sim, nullptr);

    this->sim = std::addressof(sim);

    BATT_CHECK(log.is_initialized);
    BATT_CHECK_EQ(this->log, nullptr);

    this->log = std::addressof(log);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Create a new VolumeTrimmer.
  //
  {
    BATT_CHECK_NOT_NULLPTR(log.fake_device);
    BATT_CHECK_NE(log.slot_writer, batt::None);
    BATT_CHECK_EQ(this->trimmer, nullptr);

    batt::StatusOr<std::unique_ptr<llfs::VolumeTrimmer>> new_trimmer = llfs::VolumeTrimmer::recover(
        this->config->volume_ids.trimmer_uuid,                       //
        /*name=*/"TestTrimmer",                                      //
        llfs::TrimDelayByteCount{this->sim->trim_delay_byte_count},  //
        *log.fake_device,                                            //
        *log.slot_writer,                                            //
        [this](auto&&... args) -> decltype(auto) {
          return this->handle_drop_roots(BATT_FORWARD(args)...);
        },
        *this->log->trim_control,       //
        *this->log->metadata_refresher  //
    );

    ASSERT_TRUE(new_trimmer.ok()) << BATT_INSPECT(new_trimmer.status());

    LLFS_VLOG(1) << "initialize_trimmer(): " << (void*)std::addressof(**new_trimmer);
    this->trimmer = std::move(*new_trimmer);

    EXPECT_EQ(this->trimmer->uuid(), this->config->volume_ids.trimmer_uuid);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Set the recovered trim pos now; IMPORTANT: this must be done _after_ calling
  // VolumeTrimmer::recover, as this function may finish a partially completed trim, causing the
  // trim pos to move forward.
  //
  {
    this->last_recovered_trim_pos =
        log.fake_device->slot_range(llfs::LogReadMode::kDurable).lower_bound;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Start the trimmer task.
  //
  LLFS_VLOG(1) << "Starting trimmer task";

  this->trimmer_task.emplace(this->task_context.get_executor(), [this] {
    this->trimmer_task_exit_status = this->trimmer->run();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::TrimmerSession::check_invariants()
{
  // Check to make sure that not too much of the log was trimmed.
  //
  if (this->sim && this->log && this->log->fake_device) {
    const llfs::slot_offset_type trim_pos =
        this->log->fake_device->slot_range(llfs::LogReadMode::kDurable).lower_bound;

    if (this->log->trim_control) {
      const llfs::slot_offset_type least_locked_slot = this->log->trim_control->get_lower_bound();

      if (least_locked_slot >= this->last_recovered_trim_pos + this->sim->trim_delay_byte_count) {
        ASSERT_GE((i64)least_locked_slot - (i64)trim_pos, (i64)this->sim->trim_delay_byte_count)
            << BATT_INSPECT(this->sim->trim_delay_byte_count) << BATT_INSPECT(least_locked_slot)
            << BATT_INSPECT(trim_pos) << BATT_INSPECT(this->last_recovered_trim_pos);
      } else {
        ASSERT_EQ(trim_pos, this->last_recovered_trim_pos)
            << BATT_INSPECT(least_locked_slot) << BATT_INSPECT(this->sim->trim_delay_byte_count);
      }
    }

    // No pending jobs should be trimmed!
    //
    for (const auto& [prepare_slot_offset, job_info] : this->sim->pending_jobs) {
      ASSERT_TRUE(!llfs::slot_less_than(prepare_slot_offset, trim_pos))
          << BATT_INSPECT(prepare_slot_offset) << BATT_INSPECT(trim_pos);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status VolumeTrimmerTest::TrimmerSession::handle_drop_roots(
    boost::uuids::uuid const& trimmer_uuid, llfs::slot_offset_type client_slot,
    llfs::Slice<const llfs::PageId> page_ids)
{
  BATT_CHECK_NOT_NULLPTR(this->config);
  BATT_CHECK_NOT_NULLPTR(this->log);

  EXPECT_EQ(trimmer_uuid, this->config->volume_ids.trimmer_uuid);

  // Ignore calls to drop roots for client_slot values that have already been seen, just like
  // PageAllocator does.
  //
  if (!llfs::slot_less_than(this->sim->highest_seen_drop_pages_client_slot, client_slot)) {
    return batt::OkStatus();
  }

  // If we have already encountered a simulated fatal error, then return error status immediately.
  //
  if (this->shutting_down) {
    return llfs::make_status(llfs::StatusCode::kFakeLogDeviceExpectedFailure);
  }

  batt::SmallVec<llfs::PageId, kDefaultPageIdsVecSize> actual_page_ids(page_ids.begin(),
                                                                       page_ids.end());
  std::sort(actual_page_ids.begin(), actual_page_ids.end());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Read the log, starting at `client_slot` in order to:
  //  1. Verify there is a VolumeTrimEvent at the specified slot
  //  2. Get the trim position so we can select the "in-scope" jobs to build the expected page_ids
  //     list.
  //
  batt::Optional<llfs::SlotParseWithPayload<llfs::VolumeTrimEvent>> next_trim_event;
  {
    BATT_CHECK_NE(this->log->mem_device, batt::None);

    std::unique_ptr<llfs::LogDevice::Reader> log_reader = this->log->mem_device->new_reader(
        /*slot_lower_bound=*/client_slot, llfs::LogReadMode::kSpeculative);

    BATT_CHECK_NOT_NULLPTR(log_reader);

    llfs::TypedSlotReader<llfs::VolumeEventVariant> slot_reader{*log_reader};
    batt::StatusOr<usize> n_read = slot_reader.run(
        batt::WaitForResource::kFalse,
        batt::make_case_of_visitor(
            [&](const llfs::SlotParse& slot, const llfs::VolumeTrimEvent& trim_event) {
              next_trim_event.emplace(slot, trim_event);
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
    [&] {
      ASSERT_TRUE(n_read.ok() || n_read.status() == llfs::StatusCode::kBreakSlotReaderLoop);
    }();
    [&] {
      ASSERT_TRUE(next_trim_event) << "No trim event found at the specified offset!";
    }();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Build the list of expected page ids and compare it to actual.
  //
  batt::SmallVec<llfs::PageId, kDefaultPageIdsVecSize> expected_page_ids;
  if (next_trim_event) {
    const llfs::VolumeTrimEvent& trim_event = next_trim_event->payload;

    for (auto iter = this->sim->committed_jobs.begin(); iter != this->sim->committed_jobs.end();
         iter = this->sim->committed_jobs.erase(iter)) {
      const auto& [commit_slot, job_info] = *iter;

      [&] {
        ASSERT_EQ(this->sim->pending_jobs.count(job_info.prepare_slot_offset), 0u)
            << "Trimmed region contains pending jobs!";
      }();

      if (!llfs::slot_less_than(commit_slot, trim_event.new_trim_pos)) {
        break;
      }
      const auto& page_ids = job_info.page_ids;

      LLFS_VLOG(1) << " -- expecting: " << BATT_INSPECT(commit_slot)
                   << BATT_INSPECT_RANGE(page_ids);

      expected_page_ids.insert(expected_page_ids.end(), page_ids.begin(), page_ids.end());
    }

    // Sort the expected page ids so we can compare it to the actual page ids.
    //
    std::sort(expected_page_ids.begin(), expected_page_ids.end());
  }
  [&] {
    ASSERT_EQ(actual_page_ids, expected_page_ids);
  }();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Remember that we processed this update.
  //
  this->sim->highest_seen_drop_pages_client_slot = client_slot;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::TrimmerSession::run_one()
{
  BATT_CHECK_NOT_NULLPTR(this->sim);
  BATT_CHECK_NOT_NULLPTR(this->log);

  LLFS_VLOG(1) << "Checking for tasks ready to run";
  batt::UniqueHandler<> action = this->task_context.pop_ready_handler([this](usize n) {
    return this->sim->pick_usize(0, n - 1);
  });
  if (action) {
    ASSERT_NO_FATAL_FAILURE(action());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeTrimmerTest::TrimmerSession::terminate()
{
  LLFS_VLOG(1) << "TrimmerSession::terminate()" << BATT_INSPECT(this->trimmer->grant_pool_size());

  // Tell everything to shut down.
  //
  this->shutting_down = true;
  auto on_scope_exit = batt::finally([&] {
    this->shutting_down = false;
    this->trimmer = nullptr;
    this->last_recovered_trim_pos = 0;
    this->trimmer_task = batt::None;
    this->config = nullptr;
    this->sim = nullptr;
    this->log = nullptr;
  });

  if (this->log) {
    this->log->halt();
  }
  if (this->trimmer) {
    this->trimmer->halt();
  }

  // Drain the executor's queue.
  //
  ASSERT_NO_FATAL_FAILURE(this->task_context.poll());

  // The trimmer task should be finished at this point.
  //
  if (this->trimmer_task) {
    ASSERT_TRUE(this->trimmer_task->try_join())
        << "n_tasks=" << batt::Task::backtrace_all(/*force=*/true);

    LLFS_VLOG(1) << "Trimmer task joined:" << BATT_INSPECT(this->trimmer_task_exit_status);
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(VolumeTrimmerTest, RandomizedTest)
{
  static const bool kExtraTesting =               //
      batt::getenv_as<int>("LLFS_EXTRA_TESTING")  //
          .value_or(0);

  static const usize kNumSeeds =                   //
      batt::getenv_as<int>("LLFS_TEST_NUM_SEEDS")  //
          .value_or(kExtraTesting ? (1000 * 1000 * 1000) : (1000 * 1000));

  static const int kLogEveryN =                      //
      batt::getenv_as<int>("LLFS_TEST_LOG_EVERY_N")  //
          .value_or(kNumSeeds / 10);

  static const usize kInitialSeed =                   //
      batt::getenv_as<int>("LLFS_TEST_INITIAL_SEED")  //
          .value_or(0);

  static const usize kNumThreads = batt::getenv_as<int>("LLFS_TEST_NUM_THREADS")  //
                                       .value_or(std::thread::hardware_concurrency());

  LLFS_LOG_INFO() << BATT_INSPECT(kExtraTesting) << BATT_INSPECT(kNumSeeds)
                  << BATT_INSPECT(kLogEveryN) << BATT_INSPECT(kInitialSeed)
                  << BATT_INSPECT(kNumThreads);

  batt::Watch<usize> crash_count{0};
  batt::Watch<usize> max_log_size{0};

  Config config;

  std::vector<std::thread> threads;
  std::atomic<usize> running_count{0};

  for (usize thread_i = 0; thread_i < kNumThreads; ++thread_i) {
    running_count.fetch_add(1);
    threads.emplace_back([&running_count, &crash_count, &config, thread_i] {
      usize local_crash_count = 0;
      for (usize seed_i = kInitialSeed + thread_i; seed_i < kInitialSeed + kNumSeeds;
           seed_i += kNumThreads) {
        //----- --- -- -  -  -   -
        LLFS_LOG_INFO_EVERY_N(kLogEveryN)
            << "Starting new test run with empty log;" << BATT_INSPECT(seed_i)
            << BATT_INSPECT(local_crash_count) << " (est. " << (local_crash_count * kNumThreads)
            << " total)";

        SimRun sim;
        ASSERT_NO_FATAL_FAILURE(sim.reset(seed_i, config));

        LogSession log;
        ASSERT_NO_FATAL_FAILURE(log.initialize(config, sim));
        auto terminate_log = batt::finally([&] {
          log.terminate();
        });

        BATT_DEBUG_INFO(BATT_INSPECT(seed_i) << BATT_INSPECT(
                            log.fake_device->slot_range(llfs::LogReadMode::kSpeculative)));

        TrimmerSession trimmer;
        ASSERT_NO_FATAL_FAILURE(trimmer.initialize(config, sim, log));
        auto terminate_trimmer = batt::finally([&] {
          trimmer.terminate();
        });

        // Main test loop: for each step, randomly take some action that advances the state of the
        // log/trimmer.
        //
        LLFS_VLOG(1) << "Entering test loop";

        for (int step_i = 0; !log.fake_log_has_failed() && step_i < kStepLimit; ++step_i) {
          LLFS_VLOG(1) << "Step " << step_i;
          ASSERT_NO_FATAL_FAILURE(trimmer.check_invariants());

          // Let the VolumeTrimmer task run.
          //
          if (sim.pick_branch()) {
            ASSERT_NO_FATAL_FAILURE(trimmer.run_one());
            ASSERT_NO_FATAL_FAILURE(trimmer.check_invariants());
          }

          // Trim the log.
          //
          if (sim.pick_branch()) {
            ASSERT_NO_FATAL_FAILURE(log.trim_log());
            ASSERT_NO_FATAL_FAILURE(trimmer.check_invariants());
          }

          // Write opaque user data slot, if there is enough log space.
          //
          if (log.slot_writer->pool_size() > kMaxOpaqueDataSize + kMaxSlotOverhead &&
              sim.pick_branch()) {
            ASSERT_NO_FATAL_FAILURE(log.append_opaque_data_slot()) << BATT_INSPECT(seed_i);
            ASSERT_NO_FATAL_FAILURE(trimmer.check_invariants());
          }

          // Write PrepareJob slot.
          //
          if (sim.pick_branch()) {
            ASSERT_NO_FATAL_FAILURE(log.prepare_one_job());
            ASSERT_NO_FATAL_FAILURE(trimmer.check_invariants());
          }

          // Write CommitJob slot, if there is a pending job.
          //
          if (!sim.pending_jobs.empty() && sim.pick_branch()) {
            ASSERT_NO_FATAL_FAILURE(log.commit_one_job()) << BATT_INSPECT(seed_i);
            ASSERT_NO_FATAL_FAILURE(trimmer.check_invariants());
          }

          // Simulate a crash/recovery (rate=5%)
          //
          if (sim.pick_usize(1, 100) <= 5) {
            BATT_DEBUG_INFO(BATT_INSPECT(log.fake_log_has_failed())
                            << BATT_INSPECT(log.fake_device_state->device_time)
                            << BATT_INSPECT(log.fake_device_state->failure_time));

            LLFS_VLOG(1) << "Simulating crash/recovery..."
                         << BATT_INSPECT(trimmer.last_recovered_trim_pos)
                         << BATT_INSPECT(log.fake_device->slot_range(llfs::LogReadMode::kDurable))
                         << BATT_INSPECT(log.trim_control->get_lower_bound())
                         << BATT_INSPECT(log.fake_device->size())
                         << BATT_INSPECT(log.fake_device->space())
                         << BATT_INSPECT(log.fake_device->capacity());

            trimmer.terminate();
            log.terminate();

            ASSERT_NO_FATAL_FAILURE(log.initialize(config, sim));

            BATT_DEBUG_INFO(BATT_INSPECT(log.job_grant->size()));

            ASSERT_NO_FATAL_FAILURE(trimmer.initialize(config, sim, log));
            ASSERT_NO_FATAL_FAILURE(trimmer.check_invariants());

            local_crash_count += 1;
          }
        }

        LLFS_VLOG(1) << "Exited loop; joining trimmer task";
      }

      crash_count.fetch_add(local_crash_count);

      const usize n_threads_remaining = running_count.fetch_sub(1);
      LLFS_LOG_INFO() << "Test thread " << thread_i << " finished (" << n_threads_remaining
                      << " still running)";
    });
  }

  for (std::thread& t : threads) {
    t.join();
  }
}

}  // namespace
