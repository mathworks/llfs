//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator.hpp>
//
#include <llfs/page_allocator.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/log_device_snapshot.hpp>
#include <llfs/memory_log_device.hpp>
#include <llfs/testing/fake_log_device.hpp>

#include <batteries/async/fake_task_scheduler.hpp>
#include <batteries/env.hpp>
#include <batteries/state_machine_model.hpp>

#include <boost/uuid/uuid_generators.hpp>

#include <functional>
#include <random>
#include <unordered_set>

namespace {

using namespace batt::int_types;

using batt::None;

using llfs::LogDeviceSnapshot;
using llfs::LogReadMode;
using llfs::MemoryLogDeviceFactory;
using llfs::OkStatus;
using llfs::Optional;
using llfs::PackedPageAllocatorAttach;
using llfs::page_id_int;
using llfs::PageAllocator;
using llfs::PageAllocatorAttachmentStatus;
using llfs::PageId;
using llfs::PageIdFactory;
using llfs::PageRefCount;
using llfs::slot_offset_type;
using llfs::Status;
using llfs::StatusOr;

using llfs::MemoryLogDevice;
using llfs::MemoryLogStorageDriver;
using llfs::testing::FakeLogDevice;
using llfs::testing::FakeLogDeviceFactory;
using llfs::testing::FakeLogDeviceReader;
using llfs::testing::FakeLogDeviceWriter;

TEST(PageAllocatorTest, UpdateRefCounts)
{
  constexpr u64 kNumPages = 100;
  constexpr u64 kMaxAttachments = 1;
  constexpr int kRepeatCount = 10;
  constexpr u64 kCoverageFactor = 10;

  StatusOr<std::unique_ptr<PageAllocator>> page_allocator =
      PageAllocator::recover(batt::Runtime::instance().default_scheduler(), "Test",
                             PageIdFactory{/*device_capacity=*/kNumPages, /*page_device_id=*/0},
                             *std::make_unique<MemoryLogDeviceFactory>(
                                 PageAllocator::calculate_log_size(kNumPages, kMaxAttachments)));

  ASSERT_TRUE(page_allocator.ok()) << BATT_INSPECT(page_allocator.status());
  ASSERT_NE(*page_allocator, nullptr);

  PageAllocator& index = **page_allocator;

  for (page_id_int page_id = 0; page_id < kNumPages; ++page_id) {
    auto ans = index.get_ref_count(PageId{page_id});
    EXPECT_EQ(0, ans.first);
  }

  auto fake_user_id = boost::uuids::random_generator{}();

  ASSERT_TRUE(index
                  .update_sync(PackedPageAllocatorAttach{
                      .user_slot =
                          {
                              .user_id = fake_user_id,
                              .slot_offset = 0,
                          },
                  })
                  .ok());

  for (page_id_int update_count = 1; update_count < kNumPages * kCoverageFactor; ++update_count) {
    if (update_count % 100 == 0) {
      VLOG(1) << "update_count=" << update_count;
    }
    std::vector<PageRefCount> updates;
    for (page_id_int page_index = 0; page_index < std::min(kNumPages, update_count); ++page_index) {
      updates.emplace_back(PageRefCount{
          .page_id = page_index,
          .ref_count = +2,
      });
    }
    for (int repeat_count = 0; repeat_count < kRepeatCount; ++repeat_count) {
      StatusOr<slot_offset_type> updated =
          index.update_page_ref_counts(fake_user_id, /*slot=*/update_count, llfs::as_seq(updates));
      ASSERT_TRUE(updated.ok()) << updated.status();

      Status sync_status = index.sync(*updated);
      ASSERT_TRUE(sync_status.ok());

      for (page_id_int page_id = 0; page_id < kNumPages; ++page_id) {
        if (page_id >= update_count) {
          auto ans = index.get_ref_count(PageId{page_id});
          EXPECT_EQ(0, ans.first) << "update_count=" << update_count << " page_id=" << page_id
                                  << " repeat_count=" << repeat_count << " ts=" << ans.second
                                  << " updated=" << *updated;
        } else {
          auto ans = index.get_ref_count(PageId{page_id});
          EXPECT_EQ(int(update_count - page_id) * 2, ans.first)
              << "update_count=" << update_count << " page_id=" << page_id
              << " repeat_count=" << repeat_count << " ts=" << ans.second
              << " updated=" << *updated;
        }
      }
    }
  }
}

TEST(PageAllocatorTest, LogCrashRecovery)
{
  llfs::suppress_log_output_for_test() = true;
  auto undo_suppress = batt::finally([] {
    llfs::suppress_log_output_for_test() = false;
  });

  constexpr u64 kNumPages = 8;
  constexpr u64 kMaxAttachments = 64;
  constexpr u64 kNumSeeds = 1000;

  std::uniform_int_distribution<usize> pick_page_count(1u, kNumPages);
  std::uniform_int_distribution<usize> pick_client(0u, kMaxAttachments - 1u);
  std::uniform_int_distribution<usize> pick_physical_page(0u, kNumPages - 1u);
  std::uniform_int_distribution<i32> pick_ref_count_delta(-5, +5);
  std::uniform_int_distribution<u64> pick_failure_time(1u, 4096u);

  const bool extra_testing = batt::getenv_as<int>("LLFS_EXTRA_TESTING").value_or(0);

  constexpr u64 kStartSeed = 0;
  const u64 kEndSeed = (extra_testing ? (10000 * kNumSeeds) : kNumSeeds) + kStartSeed;

  for (u64 seed = kStartSeed; seed < kEndSeed; ++seed) {
    VLOG_EVERY_N(1, 1000) << BATT_INSPECT(seed);
    std::default_random_engine rng{seed};
    for (usize i = 0; i < 19; ++i) {
      (void)rng();
    }

    std::array<Optional<boost::uuids::uuid>, kMaxAttachments> client_uuids;

    std::array<slot_offset_type, kMaxAttachments> client_slot;
    client_slot.fill(0u);

    std::array<std::function<Status(PageAllocator&)>, kMaxAttachments> last_client_update;
    last_client_update.fill([](PageAllocator&) -> Status {
      return OkStatus();
    });

    std::unordered_map<boost::uuids::uuid, usize, boost::hash<boost::uuids::uuid>>
        client_index_for_uuid;

    MemoryLogDevice mem_log{
        /*size=*/PageAllocator::calculate_log_size(/*physical_page_count=*/kNumPages,
                                                   /*max_attachments=*/kMaxAttachments)};

    LOG_FIRST_N(INFO, 1) << BATT_INSPECT(mem_log.space());

    auto fake_log_state = std::make_shared<FakeLogDevice::State>();

    FakeLogDeviceFactory<MemoryLogStorageDriver> fake_log_factory{mem_log, mem_log.driver().impl(),
                                                                  batt::make_copy(fake_log_state)};

    StatusOr<std::unique_ptr<PageAllocator>> status_or_page_allocator = PageAllocator::recover(
        /*TODO [tastolfi 2022-01-21] use fake*/ batt::Runtime::instance().default_scheduler(),
        "Test", PageIdFactory{/*device_capacity=*/kNumPages, /*page_device_id=*/0},
        fake_log_factory);

    ASSERT_TRUE(status_or_page_allocator.ok());
    PageAllocator& page_allocator = **status_or_page_allocator;
    {
      u64 delta = pick_failure_time(rng);
      fake_log_state->failure_time = fake_log_state->device_time + delta;
      VLOG(1) << BATT_INSPECT(delta);
    }

    std::array<bool, kMaxAttachments> expect_attached;
    expect_attached.fill(false);

    std::array<i32, kNumPages> expect_ref_count;
    expect_ref_count.fill(0);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Run the simulation until the fake log device has reached its failure time.
    //
    while (!fake_log_state->is_failed()) {
      std::vector<usize> pages_to_update(kNumPages);
      std::iota(pages_to_update.begin(), pages_to_update.end(), 0);
      std::shuffle(pages_to_update.begin(), pages_to_update.end(), rng);

      const usize page_count = pick_page_count(rng);
      pages_to_update.resize(page_count);

      std::vector<PageRefCount> prcs;
      for (const usize page_i : pages_to_update) {
        prcs.emplace_back();
        PageRefCount& prc = prcs.back();
        const i32 delta = pick_ref_count_delta(rng);

        prc.page_id = page_i;

        if (expect_ref_count[page_i] == 0) {
          prc.ref_count = +2;
        } else if (expect_ref_count[page_i] == 1 && delta < 0) {
          prc.ref_count = llfs::kRefCount_1_to_0;
        } else {
          prc.ref_count = std::clamp<i32>(delta, 1 - expect_ref_count[page_i], +5);
        }

        VLOG(2) << "page " << page_i << ": " << expect_ref_count[page_i] << " -> "
                << expect_ref_count[page_i] + prc.ref_count;

        if (prc.ref_count == llfs::kRefCount_1_to_0) {
          expect_ref_count[page_i] = 0;
        } else {
          expect_ref_count[page_i] += prc.ref_count;
        }
      }

      const usize client_i = pick_client(rng);
      VLOG(2) << BATT_INSPECT(client_i);

      // If this is the first time we picked this client, then generate a UUID and attach.
      //
      bool need_attach = false;
      slot_offset_type attach_user_slot = 0;
      if (client_uuids[client_i] == None) {
        client_uuids[client_i].emplace(boost::uuids::random_generator{}());
        client_index_for_uuid[*client_uuids[client_i]] = client_i;
        client_slot[client_i] += 1;
        attach_user_slot = client_slot[client_i];
        need_attach = true;
        expect_attached[client_i] = true;
      }

      client_slot[client_i] += 1;
      const slot_offset_type update_user_slot = client_slot[client_i];

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      last_client_update[client_i] = [need_attach, user_id = *client_uuids[client_i],
                                      attach_user_slot, update_user_slot,
                                      prcs](PageAllocator& page_allocator) -> Status {
        if (need_attach) {
          BATT_CHECK_LT(attach_user_slot, update_user_slot);

          StatusOr<slot_offset_type> sync_point =
              page_allocator.attach_user(user_id, attach_user_slot);
          BATT_REQUIRE_OK(sync_point);

          Status sync_status = page_allocator.sync(*sync_point);
          BATT_REQUIRE_OK(sync_status);
        }

        StatusOr<slot_offset_type> update_sync_point =
            page_allocator.update_page_ref_counts(user_id, update_user_slot, as_seq(prcs));

        BATT_REQUIRE_OK(update_sync_point);

        return page_allocator.sync(*update_sync_point);
      };
      //+++++++++++-+-+--+----- --- -- -  -  -   -

      Status update_status = last_client_update[client_i](page_allocator);
      if (!update_status.ok()) {
        break;
      }
      VLOG(1) << BATT_INSPECT(mem_log.slot_range(LogReadMode::kDurable));
    }
    EXPECT_TRUE(fake_log_state->is_failed());
    //
    //+++++++++++-+-+--+----- --- -- -  -  -   -

    // Quiesce any activity on the first/"crashed" PageAllocator
    //
    page_allocator.halt();
    page_allocator.join();

    VLOG(1) << "RECOVERING +++++++++++-+-+--+----- --- -- -  -  - "
            << BATT_INSPECT(mem_log.slot_range(LogReadMode::kDurable))
            << BATT_INSPECT(fake_log_state->device_time)
            << BATT_INSPECT(fake_log_state->failure_time);

    auto fake_recovered_log_state = std::make_shared<FakeLogDevice::State>();

    FakeLogDeviceFactory<MemoryLogStorageDriver> fake_recovered_log_factory{
        mem_log, mem_log.driver().impl(), batt::make_copy(fake_recovered_log_state)};

    StatusOr<std::unique_ptr<PageAllocator>> status_or_recovered_page_allocator =
        PageAllocator::recover(
            /*TODO [tastolfi 2022-01-21] use fake*/ batt::Runtime::instance().default_scheduler(),
            "Test", PageIdFactory{/*device_capacity=*/kNumPages, /*page_device_id=*/0},
            fake_recovered_log_factory);

    ASSERT_TRUE(status_or_recovered_page_allocator.ok());
    PageAllocator& recovered_page_allocator = **status_or_recovered_page_allocator;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Retry the last update from each client.  The ones that were successfully committed prior to
    // the simulated crash will have no effect.
    //
    for (const auto& fn : last_client_update) {
      Status status = fn(recovered_page_allocator);
      ASSERT_TRUE(status.ok());
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Verify that we are completely caught up to the expected state.
    //
    std::vector<PageAllocatorAttachmentStatus> recovered_attachments =
        recovered_page_allocator.get_all_clients_attachment_status();

    std::unordered_set<usize> clients_to_check;
    for (usize client_i = 0; client_i < kMaxAttachments; ++client_i) {
      clients_to_check.emplace(client_i);
    }

    for (const auto& attach_status : recovered_attachments) {
      auto iter = client_index_for_uuid.find(attach_status.user_id);
      ASSERT_NE(iter, client_index_for_uuid.end());

      const usize client_i = iter->second;
      ASSERT_LT(client_i, kMaxAttachments);

      EXPECT_TRUE(expect_attached[client_i]);
      EXPECT_EQ(client_slot[client_i], attach_status.user_slot);

      clients_to_check.erase(client_i);
    }

    for (usize client_i : clients_to_check) {
      EXPECT_FALSE(expect_attached[client_i]);
    }

    for (usize page_i = 0; page_i < kNumPages; ++page_i) {
      const i32 recovered_ref_count = recovered_page_allocator.get_ref_count(PageId{page_i}).first;
      EXPECT_EQ(recovered_ref_count, expect_ref_count[page_i]) << BATT_INSPECT(page_i);
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//

constexpr usize kNumPages = 8;
constexpr usize kMaxAttachments = 4;
constexpr i32 kMaxCrashCount = 3;
const usize kSimLogSize =
    /*size=*/PageAllocator::calculate_log_size(/*physical_page_count=*/kNumPages,
                                               /*max_attachments=*/kMaxAttachments);

std::array<boost::uuids::uuid, kMaxAttachments> kSimUuids = [] {
  std::array<boost::uuids::uuid, kMaxAttachments> uuids;

  for (auto& user_id : uuids) {
    user_id = boost::uuids::random_generator{}();
  }

  return uuids;
}();

enum struct AttachState {
  kDetached,
  kPending,
  kConfirmed,
};

struct PageAllocatorState : boost::equality_comparable<PageAllocatorState> {
  struct Hash {
    usize operator()(const PageAllocatorState& s) const
    {
      return batt::hash(s.crash_count,   //
                        s.ref_counts,    //
                        s.attach_state,  //
                        s.finished_ok,   //
                        s.log_snapshot   //
      );
    }
  };

  friend bool operator==(const PageAllocatorState& l, const PageAllocatorState& r)
  {
    return l.crash_count == r.crash_count       //
           && l.ref_counts == r.ref_counts      //
           && l.attach_state == r.attach_state  //
           && l.finished_ok == r.finished_ok    //
           && l.log_snapshot == r.log_snapshot  //
        ;
  }

  bool is_terminal() const
  {
    return this->crash_count >= kMaxCrashCount ||
           (llfs::as_seq(this->finished_ok) | llfs::seq::all_true());
  }

  PageAllocatorState() noexcept
  {
    this->ref_counts.fill(0);
    this->attach_state.fill(AttachState::kDetached);
    this->finished_ok.fill(false);
  }

  i32 crash_count = 0;
  std::array<i32, kNumPages> ref_counts;
  std::array<AttachState, kMaxAttachments> attach_state;
  std::array<bool, kMaxAttachments> finished_ok;
  LogDeviceSnapshot log_snapshot;
};

class PageAllocatorModel
    : public batt::StateMachineModel<PageAllocatorState, PageAllocatorState::Hash>
{
 public:
  PageAllocatorModel() = default;

  PageAllocatorState initialize() override
  {
    return PageAllocatorState{};
  }

  void enter_state(const PageAllocatorState& s) override
  {
    this->state_ = s;

    if (this->state_.is_terminal()) {
      return;
    }

    this->mem_log_.restore_snapshot(s.log_snapshot, LogReadMode::kDurable);

    this->fake_log_state_ = std::make_shared<FakeLogDevice::State>();
    this->fake_log_factory_.emplace(this->mem_log_, this->mem_log_.driver().impl(),
                                    batt::make_copy(fake_log_state_));
  }

  void step() override
  {
    if (this->state_.is_terminal()) {
      return;
    }

    VLOG(2) << "Entered PageAllocatorModel::step()";

    this->state_.crash_count += 1;
    if (this->state_.crash_count < kMaxCrashCount) {
      this->fake_log_state_->failure_time = this->pick_int(0, 64);
    }

    VLOG(2) << "Launching recovery task";

    batt::Task recovery_task{this->context_.get_executor(), [&] {
                               Status recover_status = this->recover();
                               if (!recover_status.ok()) {
                                 return;
                               }

                               this->check_ref_counts();
                             }};

    this->await_task(&recovery_task);

    if (this->fake_log_state_->is_failed()) {
      return;
    }

    VLOG(2) << "Launching client tasks";

    std::array<std::unique_ptr<batt::Task>, kMaxAttachments> client_tasks;
    {
      usize client_i = 0;
      for (auto& task : client_tasks) {
        task = std::make_unique<batt::Task>(this->context_.get_executor(), [this, client_i] {
          this->client_task_main(client_i);
        });
        client_i += 1;
      }
    }
    for (auto& task : client_tasks) {
      this->await_task(task.get());
    }
  }

  PageAllocatorState leave_state() override
  {
    if (this->page_allocator_ != nullptr) {
      this->page_allocator_->halt();
      this->context_.poll();
      this->page_allocator_->join();
      this->page_allocator_ = nullptr;
    }

    // Save a little effort; if the state is terminal, don't bother snapshotting the device.
    //
    if (!this->state_.is_terminal()) {
      this->state_.log_snapshot =
          LogDeviceSnapshot::from_device(this->mem_log_, LogReadMode::kDurable);
    }

    return this->state_;
  }

  bool check_invariants() override
  {
    return true;
  }

  PageAllocatorState normalize(const PageAllocatorState& s) override
  {
    if (s.is_terminal()) {
      PageAllocatorState s_norm;
      s_norm.crash_count = kMaxCrashCount;
      return s_norm;
    }
    return s;
  }

  void report_progress(const batt::StateMachineResult& r) override
  {
    static std::atomic<usize> counter{0};

    VLOG(1) << r;

    if ((counter.fetch_add(1) + 1) % this->max_concurrency() == 0) {
      VLOG(1) << "=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------";
    }
  }

  usize max_concurrency() const override
  {
    static const int n = batt::getenv_as<int>("MODEL_CHECK_THREADS").value_or(4);
    return n;
  }

  AdvancedOptions advanced_options() const override
  {
    auto options = AdvancedOptions::with_default_values();

    options.max_loop_iterations_between_flush = 128;

    return options;
  }

  std::unique_ptr<batt::StateMachineModel<PageAllocatorState, PageAllocatorState::Hash>> clone()
      const override
  {
    return std::make_unique<PageAllocatorModel>();
  }

 private:
  void await_task(batt::Task* task)
  {
    BATT_CHECK_NOT_NULLPTR(task);

    while (!task->try_join()) {
      VLOG(2) << BATT_INSPECT(this->context_.work_count().get_value())
              << BATT_INSPECT(this->fake_log_state_->is_failed());
      BATT_CHECK(this->run_one(this->context_));
    }
  }

  Status recover()
  {
    VLOG(2) << "entered recover() " << BATT_INSPECT(this->context_.work_count().get_value());

    StatusOr<std::unique_ptr<PageAllocator>> status_or_page_allocator =
        PageAllocator::recover(this->fake_scheduler_, "Test",
                               PageIdFactory{/*device_capacity=*/kNumPages, /*page_device_id=*/0},
                               *this->fake_log_factory_);

    VLOG(2) << "after PageAllocator::recover() "
            << BATT_INSPECT(this->context_.work_count().get_value())
            << BATT_INSPECT(status_or_page_allocator.status());

    BATT_REQUIRE_OK(status_or_page_allocator);

    this->page_allocator_ = std::move(*status_or_page_allocator);

    return OkStatus();
  }

  void check_ref_counts()
  {
    BATT_CHECK_NOT_NULLPTR(this->page_allocator_);

    for (usize i = 0; i < kNumPages; ++i) {
      BATT_CHECK_EQ(this->state_.ref_counts[i],
                    this->page_allocator_->get_ref_count(PageId{i}).first);
    }
  }

  void client_task_main(usize client_i)
  {
    ([&]() -> Status {
      std::default_random_engine rng{client_i};

      const boost::uuids::uuid user_id = kSimUuids[client_i];

      // First attach.
      //
      if (this->state_.attach_state[client_i] != AttachState::kConfirmed) {
        this->state_.attach_state[client_i] = AttachState::kPending;

        StatusOr<slot_offset_type> attached =
            this->page_allocator_->attach_user(user_id, /*user_slot=*/0u);

        BATT_REQUIRE_OK(attached);

        Status sync_status = this->page_allocator_->sync(*attached);

        BATT_REQUIRE_OK(sync_status);

        this->state_.attach_state[client_i] = AttachState::kConfirmed;
      }

      // Verify the attachment status.
      //
      Optional<llfs::PageAllocatorAttachmentStatus> attachment =
          this->page_allocator_->get_client_attachment_status(user_id);
      EXPECT_TRUE(attachment);
      BATT_CHECK(attachment);
      if (attachment) {
        EXPECT_EQ(attachment->user_id, user_id);
      }

      // Make random changes until we hit a simulated crash.
      //

      for (;;) {
        // TODO [tastolfi 2022-01-18]
        break;
      }

      this->state_.finished_ok[client_i] = true;

      return OkStatus();
    })()
        .IgnoreError();
  }

  batt::FakeTaskScheduler fake_scheduler_;
  batt::FakeExecutionContext& context_ = this->fake_scheduler_.get_context();
  PageAllocatorState state_;
  std::shared_ptr<FakeLogDevice::State> fake_log_state_;
  MemoryLogDevice mem_log_{kSimLogSize};
  Optional<FakeLogDeviceFactory<MemoryLogStorageDriver>> fake_log_factory_;
  std::unique_ptr<PageAllocator> page_allocator_;
};

TEST(PageAllocatorTest, StateMachineSim)
{
  PageAllocatorModel model;

  PageAllocatorModel::Result r = model.check_model();

  VLOG(1) << "FINAL RESULT ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   - "
          << std::endl
          << r;

  EXPECT_TRUE(r.ok);
}

}  // namespace
