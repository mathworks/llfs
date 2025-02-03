//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_recycler.hpp>
//
#include <llfs/page_recycler.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/log_device_snapshot.hpp>
#include <llfs/memory_log_device.hpp>
#include <llfs/testing/fake_log_device.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/stream_util.hpp>

#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

namespace {

using namespace batt::int_types;

using llfs::testing::FakeLogDevice;
using llfs::testing::FakeLogDeviceFactory;

using llfs::kMaxPageRefDepth;
using llfs::MaxRefsPerPage;
using llfs::MemoryLogDevice;
using llfs::MemoryLogStorageDriver;
using llfs::OkStatus;
using llfs::PageDeleter;
using llfs::PageId;
using llfs::PageRecycler;
using llfs::PageToRecycle;
using llfs::Slice;
using llfs::slot_less_than;
using llfs::slot_offset_type;
using llfs::Status;
using llfs::StatusOr;

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

constexpr usize kNumFakePageIds = 50;

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

struct FakePage {
  FakePage() = default;

  FakePage(const FakePage& other)
      : page_id{other.page_id}
      , out_refs{other.out_refs}
      , in_ref_count{other.in_ref_count.load()}
      , max_ref_depth{other.max_ref_depth}
      , deleted{other.deleted.load()}
  {
  }

  PageId page_id;
  std::vector<PageId> out_refs;
  std::atomic<i32> in_ref_count{0};
  u32 max_ref_depth = 0;
  std::atomic<bool> deleted{false};
};

inline std::ostream& operator<<(std::ostream& out, const FakePage& t)
{
  return out << "FakePage{.id=" << t.page_id << ", .out_refs=" << batt::dump_range(t.out_refs)
             << ", .in_ref_count=" << t.in_ref_count << ", .max_ref_depth=" << t.max_ref_depth
             << ", .deleted=" << t.deleted << ",}";
}

inline std::ostream& operator<<(std::ostream& out, const std::unique_ptr<FakePage>& t)
{
  return out << *t;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class MockPageDeleter : public llfs::PageDeleter
{
 public:
  MOCK_METHOD(Status, delete_pages,
              (const Slice<const PageToRecycle>& to_delete, PageRecycler& recycler,
               slot_offset_type caller_slot, batt::Grant& recycle_grant, i32 recycle_depth),
              (override));
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PageRecyclerTest : public ::testing::Test
{
 public:
  static constexpr auto kDefaultMaxRefsPerPage = llfs::MaxRefsPerPage{16};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void SetUp() override
  {
    llfs::suppress_log_output_for_test() = true;

    for (usize i = 0; i < this->fake_page_id_.size(); ++i) {
      this->fake_page_id_[i] = llfs::PageId{i + 0x101};
    }

    this->recycler_options_.set_max_refs_per_page(kDefaultMaxRefsPerPage);
  }

  void TearDown() override
  {
    llfs::suppress_log_output_for_test() = false;
  }

  // Create a randomized DAG of pages that reference each other.
  //
  template <typename Rng>
  void generate_fake_pages(Rng& rng, usize count, usize max_branching_factor)
  {
    this->fake_pages_.clear();
    std::uniform_int_distribution<usize> pick_num_out_refs(0, max_branching_factor);
    for (usize n = 0; n < count; ++n) {
      auto fake_page = std::make_unique<FakePage>();
      fake_page->page_id = PageId{n};

      if (n > max_branching_factor) {
        const usize num_out_refs = pick_num_out_refs(rng);
        usize lower_bound = 0;
        usize upper_bound = n - num_out_refs;
        for (usize m = 0; m < num_out_refs; ++m) {
          std::uniform_int_distribution<u64> pick_out_ref(lower_bound, upper_bound);

          const usize out_ref_n = pick_out_ref(rng);
          BATT_CHECK_LT(out_ref_n, n);

          FakePage& out_ref_page = *this->fake_pages_[PageId{out_ref_n}];

          if (out_ref_page.max_ref_depth + 1 < kMaxPageRefDepth) {
            fake_page->out_refs.emplace_back(PageId{out_ref_n});
            fake_page->max_ref_depth =
                std::max(fake_page->max_ref_depth, out_ref_page.max_ref_depth + 1);
            out_ref_page.in_ref_count.fetch_add(1);
          }
          lower_bound = out_ref_n + 1;
          ++upper_bound;
        }
      }

      PageId page_id = fake_page->page_id;
      this->fake_pages_.emplace(page_id, std::move(fake_page));
    }

    this->fake_page_root_set_.clear();
    for (const auto& [page_id, fake_page] : this->fake_pages_) {
      if (fake_page->in_ref_count == 0) {
        this->fake_page_root_set_.emplace_back(page_id);
      }
    }
    std::sort(this->fake_page_root_set_.begin(), this->fake_page_root_set_.end());
  }

  void run_crash_recovery_test();

  u64 get_log_size() const noexcept
  {
    return PageRecycler::calculate_log_size(
        this->recycler_options_,
        /*max_buffered_page_count=*/llfs::PageCount{kNumFakePageIds});
  }

  batt::Status recover_page_recycler(bool start = true)
  {
    static const std::string_view kTestRecyclerName = "TestRecycler";

    BATT_CHECK_EQ(this->recycler_, nullptr);
    BATT_CHECK_EQ(this->unique_page_recycler_, nullptr);

    batt::StatusOr<std::unique_ptr<llfs::PageRecycler>> page_recycler_recovery =
        llfs::PageRecycler::recover(
            batt::Runtime::instance().default_scheduler(), kTestRecyclerName,
            this->recycler_options_, this->mock_deleter_,
            *std::make_unique<llfs::BasicLogDeviceFactory>([this] {
              auto mem_log = std::make_unique<llfs::MemoryLogDevice>(this->get_log_size());
              if (this->mem_log_snapshot_) {
                mem_log->restore_snapshot(*this->mem_log_snapshot_, llfs::LogReadMode::kDurable);
              }
              this->p_mem_log_ = mem_log.get();
              return mem_log;
            }));

    if (page_recycler_recovery.ok()) {
      EXPECT_NE(this->p_mem_log_, nullptr);

      this->recycler_ = page_recycler_recovery->get();
      this->unique_page_recycler_ = std::move(*page_recycler_recovery);

      if (start) {
        // Important: Start the PageRecycler!
        //
        this->recycler_->start();
      }
    }

    return page_recycler_recovery.status();
  }

  void save_log_snapshot()
  {
    this->mem_log_snapshot_ =
        llfs::LogDeviceSnapshot::from_device(*this->p_mem_log_, llfs::LogReadMode::kDurable);
  }

  // Returns a matcher for a single-page slice passed to `PageDeleter::delete_pages`.
  //
  auto match_page_id(llfs::PageId expected_page_id)
  {
    return ::testing::Truly(
        [expected_page_id](const batt::Slice<const llfs::PageToRecycle>& to_delete) {
          return to_delete.size() == 1 && to_delete[0].page_id == expected_page_id;
        });
  }

  // Returns an ever increasing unique_offset value for page recycler calls.
  //
  slot_offset_type get_and_incr_unique_offset()
  {
    return ++this->unique_offset_;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Actual fake page data for crash recovery tetst.
  //
  std::unordered_map<PageId, std::unique_ptr<FakePage>, PageId::Hash> fake_pages_;

  // The set of 'root' page ids for crash recovery test.
  //
  std::vector<PageId> fake_page_root_set_;

  // The object under test.
  //
  PageRecycler* recycler_ = nullptr;

  // Options used when creating the recycler inside recover_page_recycler().
  //
  llfs::PageRecyclerOptions recycler_options_;

  // A mock to use when constructing PageRecycler instances via recover_page_recycler().
  //
  ::testing::StrictMock<MockPageDeleter> mock_deleter_;

  // If present, this log snapshot will be used to initialize the PageRecycler inside
  // recover_page_recycler(), simulating an actual recovery from disk.
  //
  batt::Optional<llfs::LogDeviceSnapshot> mem_log_snapshot_;

  // Non-owning pointer to the log device used by the recycler (recover_page_recycler() tests only).
  //
  llfs::MemoryLogDevice* p_mem_log_ = nullptr;

  // A collection of fake PageId values to use in testing.
  //
  std::array<llfs::PageId, kNumFakePageIds> fake_page_id_;

  // The owning pointer for the recycler.
  //
  std::unique_ptr<llfs::PageRecycler> unique_page_recycler_;

  // This is to track unique_offset for PageRecycler tests.
  //
  slot_offset_type unique_offset_{0};

  // This mutex is to ensure serialized access to unique_offset counter.
  //
  std::mutex recycle_pages_mutex_;
};

class FakePageDeleter : public PageDeleter
{
 public:
  explicit FakePageDeleter(PageRecyclerTest* test) noexcept : test_{test}
  {
  }

  Status delete_pages(const Slice<const PageToRecycle>& to_delete, PageRecycler& recycler,
                      slot_offset_type caller_slot, batt::Grant& recycle_grant,
                      i32 recycle_depth) override
  {
    BATT_CHECK_NOT_NULLPTR(this->test_->recycler_);
    BATT_CHECK_EQ(this->test_->recycler_, &recycler);

    const boost::uuids::uuid& caller_uuid = recycler.uuid();

    if (to_delete.empty()) {
      return OkStatus();
    }

    // All pages to delete should have the same depth; save the depth of the first one here so we
    // can verify this below.
    //
    const auto depth = recycle_depth;

    // We collect below the set of pages referenced by `to_delete` whose ref_count goes to 0 as
    // a result of processing this delete; these are saved in `dead_pages`.
    //
    std::vector<PageId> dead_pages;

    // The result of recursively recycling any newly dereferenced pages.
    //
    StatusOr<slot_offset_type> result;
    auto on_return = batt::finally([&] {
      this->recursive_recycle_events_.push(result).IgnoreError();
      //
      // If the queue is closed, we are shutting down so ignoring is the right thing to do.
    });

    // Verify that the caller has recovered the correct UUID.
    //
    auto iter = this->current_slot_.find(caller_uuid);
    BATT_CHECK_NE(iter, this->current_slot_.end());

    // Simulate the PageAllocator "exactly-once" mechanism here.
    //
    const bool duplicate_op = !slot_less_than(iter->second, caller_slot);
    iter->second = caller_slot;

    // Find all the pages to recursively delete.
    //
    for (const PageToRecycle& next : to_delete) {
      BATT_CHECK_EQ(next.depth, depth);

      FakePage& fake_page = *this->test_->fake_pages_[next.page_id];
      BATT_CHECK_EQ(fake_page.in_ref_count, 0);

      // If the page is already deleted, it must have been *after* all its out-refs were flushed to
      // the recycler log, so just treat this as a no-op.
      //
      if (fake_page.deleted) {
        continue;
      }

      for (PageId child_id : fake_page.out_refs) {
        FakePage& fake_child = *this->test_->fake_pages_[child_id];

        // If this is the first time we are releasing the references from `fake_page`, then all the
        // referent (child) pages MUST still be alive; verify this below (and decrement the child
        // ref count).
        //
        if (!duplicate_op) {
          BATT_CHECK_GT(fake_child.in_ref_count, 0)
              << BATT_INSPECT(fake_child) << BATT_INSPECT(fake_page) << BATT_INSPECT(caller_slot);
          BATT_CHECK(!fake_child.deleted) << BATT_INSPECT(fake_child) << BATT_INSPECT(fake_page);

          LLFS_VLOG(1) << "dereferencing " << fake_child << BATT_INSPECT(caller_slot);
          fake_child.in_ref_count.fetch_sub(1);
        }

        // To be on the safe side, if a child is now unreferenced, add it to the dead pages.
        //
        if (fake_child.in_ref_count.load() == 0) {
          dead_pages.emplace_back(child_id);
        }
      }
    }

    if (dead_pages.empty()) {
      // If no new dead pages were discovered by this operation, we still want to notify the test
      // task to recheck for completion.
      //
      BATT_REQUIRE_OK(this->recursive_recycle_events_.push(caller_slot));
      result = caller_slot;
    } else {
      // Recursively recycle any newly dead pages.  If we try to recycle the same page multiple
      // times, that is OK, since PageIds are never reused.
      //
      std::lock_guard lock(this->test_->recycle_pages_mutex_);
      result = this->test_->recycler_->recycle_pages(as_slice(dead_pages),
                                                     this->test_->get_and_incr_unique_offset(),  //
                                                     &recycle_grant, depth + 1);
      BATT_REQUIRE_OK(result);

      // We want to be absolutely sure we will never again read any of the pages in `to_recycle`
      // before marking as "deleted," so wait for the recycler's log to flush to make sure transfer
      // of ownership is complete.
      //
      Status flush_status = this->test_->recycler_->await_flush(*result);
      BATT_REQUIRE_OK(flush_status);
    }

    // Since we successfully passed the `recycle_pages` and `await_flush` calls above, it is now
    // safe to delete the pages.
    //
    for (const PageToRecycle& next : to_delete) {
      BATT_CHECK_EQ(next.depth, depth);
      FakePage& fake_page = *this->test_->fake_pages_[next.page_id];
      LLFS_VLOG(1) << "deleting " << fake_page << " at recycler slot " << caller_slot;
      fake_page.deleted = true;
    }

    return OkStatus();
  }

  void notify_caught_up(PageRecycler& recycler, slot_offset_type slot) override
  {
    LLFS_VLOG(1) << "CAUGHT UP ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -";

    const boost::uuids::uuid& caller_uuid = recycler.uuid();
    auto iter = this->current_slot_.find(caller_uuid);
    BATT_CHECK_NE(iter, this->current_slot_.end());

    this->recursive_recycle_events_.push(slot).IgnoreError();
  }

  void notify_failure(PageRecycler& recycler, Status failure) override
  {
    const boost::uuids::uuid& caller_uuid = recycler.uuid();
    auto iter = this->current_slot_.find(caller_uuid);
    BATT_CHECK_NE(iter, this->current_slot_.end());
    BATT_CHECK(!failure.ok());

    this->recursive_recycle_events_.push(failure).IgnoreError();
  }

  PageRecyclerTest* test_;
  std::unordered_map<boost::uuids::uuid, slot_offset_type, boost::hash<boost::uuids::uuid>>
      current_slot_;
  batt::Queue<StatusOr<slot_offset_type>> recursive_recycle_events_;
};

TEST_F(PageRecyclerTest, CrashRecovery)
{
  boost::asio::io_context io;

  batt::Task task{io.get_executor(),
                  [this] {
                    this->run_crash_recovery_test();
                  },
                  "PageRecyclerTest_CrashRecovery"};

  ASSERT_NO_FATAL_FAILURE(io.run());

  task.join();
}

void PageRecyclerTest::run_crash_recovery_test()
{
  // Note that increasing the page_count here most likely impact the test execution as we might run
  // out of grant space.
  //
  const usize fake_page_count = 155;
  const u32 max_branching_factor = 8;

  const auto options = llfs::PageRecyclerOptions{}  //
                           .set_max_refs_per_page(max_branching_factor);

  const u64 log_size = PageRecycler::calculate_log_size(options);
  LLFS_VLOG(1) << BATT_INSPECT(log_size);

  EXPECT_GE(PageRecycler::calculate_max_buffered_page_count(options, log_size),
            PageRecycler::default_max_buffered_page_count(options));

  for (u64 seed = 0; seed < 10000; ++seed) {
    std::default_random_engine rng{seed};
    for (usize i = 0; i < 10; ++i) {
      (void)rng();
    }
    this->generate_fake_pages(rng, fake_page_count, max_branching_factor);

    MemoryLogDevice mem_log{log_size};

    auto fake_log_state = std::make_shared<FakeLogDevice::State>();

    FakeLogDeviceFactory<MemoryLogStorageDriver> fake_log_factory{mem_log, mem_log.driver().impl(),
                                                                  batt::make_copy(fake_log_state)};

    FakePageDeleter fake_deleter{this};

    StatusOr<std::unique_ptr<PageRecycler>> status_or_recycler = PageRecycler::recover(
        /*TODO [tastolfi 2022-01-21] use fake*/ batt::Runtime::instance().default_scheduler(),
        "FakeRecycler", options, fake_deleter, fake_log_factory);

    ASSERT_TRUE(status_or_recycler.ok());

    const u64 max_failure_time =
        fake_log_state->device_time + (fake_page_count * max_branching_factor);
    std::uniform_int_distribution<u64> pick_failure_time{fake_log_state->device_time,
                                                         max_failure_time};

    fake_log_state->failure_time = pick_failure_time(rng);

    LLFS_VLOG(1) << BATT_INSPECT(seed) << " PageRecycler created; "
                 << BATT_INSPECT(fake_log_state->device_time)
                 << BATT_INSPECT(fake_log_state->failure_time) << " (max=" << max_failure_time
                 << ")";

    PageRecycler& recycler = **status_or_recycler;
    this->recycler_ = &recycler;

    // Simulate attaching the recycler to the storage pool.
    //
    fake_deleter.current_slot_[recycler.uuid()] = 0;

    recycler.start();

    bool failed = false;
    usize progress = 0;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    const auto recycle_root_pages = [&](PageRecycler& recycler,
                                        const Slice<const PageId>& root_pages) {
      for (PageId root_id : root_pages) {
        const std::array<PageId, 1> to_recycle = {root_id};

        BATT_DEBUG_INFO("Test - recycle_pages");
        {
          std::lock_guard lock(this->recycle_pages_mutex_);
          StatusOr<slot_offset_type> recycle_status =
              recycler.recycle_pages(to_recycle, this->get_and_incr_unique_offset());
          if (!recycle_status.ok()) {
            failed = true;
            break;
          }

          BATT_DEBUG_INFO("Test - await_flush");
          Status flush_status = recycler.await_flush(*recycle_status);
          if (!flush_status.ok()) {
            failed = true;
            break;
          }
        }

        ++progress;
      }
    };

    const auto flush_all_events = [&] {
      for (;;) {
        BATT_DEBUG_INFO("Test - await_next (recursive_recycle_events) live=" << batt::dump_range(
                            batt::as_seq(this->fake_pages_.begin(), this->fake_pages_.end())  //
                                | batt::seq::filter([](const auto& kv_pair) {
                                    return kv_pair.second->in_ref_count > 0 ||
                                           !kv_pair.second->deleted;
                                  })  //
                                |     //
                                batt::seq::map([](const auto& kv_pair) {
                                  return *kv_pair.second;
                                })  //
                                | batt::seq::collect_vec(),
                            batt::Pretty::True));

        StatusOr<slot_offset_type> event_status =
            fake_deleter.recursive_recycle_events_.await_next();
        if (!event_status.ok()) {
          failed = true;
          break;
        }

        bool done = true;
        for (const auto& [page_id, p_fake_page] : this->fake_pages_) {
          if (!p_fake_page->deleted) {
            done = false;
            break;
          }
          EXPECT_EQ(p_fake_page->in_ref_count, 0);
        }

        if (done) {
          break;
        }
      }
    };
    //+++++++++++-+-+--+----- --- -- -  -  -   -

    recycle_root_pages(recycler, as_slice(this->fake_page_root_set_));
    if (!failed) {
      ASSERT_NO_FATAL_FAILURE(flush_all_events());
    }

    // Quiesce the recycler.
    //
    BATT_DEBUG_INFO("Test - quiesce first recycler");
    recycler.halt();
    recycler.join();

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
    // Recover after simulated crash.
    //
    auto fake_recovered_log_state = std::make_shared<FakeLogDevice::State>();

    FakeLogDeviceFactory<MemoryLogStorageDriver> fake_recovered_log_factory{
        mem_log, mem_log.driver().impl(), batt::make_copy(fake_recovered_log_state)};

    BATT_DEBUG_INFO("Test - recover after simulated crash");

    StatusOr<std::unique_ptr<PageRecycler>> status_or_recovered_recycler = PageRecycler::recover(
        /*TODO [tastolfi 2022-01-21] use fake*/ batt::Runtime::instance().default_scheduler(),
        "RecoveredFakeRecycler", options, fake_deleter, fake_recovered_log_factory);

    ASSERT_TRUE(status_or_recovered_recycler.ok())
        << BATT_INSPECT(*fake_log_state) << BATT_INSPECT(*fake_recovered_log_state);

    PageRecycler& recovered_recycler = **status_or_recovered_recycler;
    this->recycler_ = &recovered_recycler;

    ASSERT_EQ(recycler.uuid(), recovered_recycler.uuid());

    recovered_recycler.start();

    failed = false;
    recycle_root_pages(recovered_recycler, as_slice(this->fake_page_root_set_.data() + progress,
                                                    this->fake_page_root_set_.size() - progress));

    ASSERT_FALSE(failed);
    ASSERT_NO_FATAL_FAILURE(flush_all_events());

    LLFS_VLOG(1) << "Run Finished" << BATT_INSPECT(seed)
                 << BATT_INSPECT(fake_recovered_log_state->device_time);
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test plan:
//   - There are N total pages to recycle
//   1. Recycle 0..N-2; intercept the first call to the mock deleter and hold the recycler task
//      there until we have finished calling PageRecycler::recycle_page for all pages < N-2.
//   2. In the mock action for page 0, simulate the discovery of dead page N-1 at depth == 1,
//      forcing this page to move to the front of the line (higher depths MUST be processed first!),
//      even though it is at the end of the log.
//   3. When we see the mock deletion of page N-1, allow it to succeed and then set an expectation
//      on the _next_ page deletion (which will probably be a batch containing pages {1, 2}).  This
//      deletion indicates that the recycler has confirmed the flush of a Commit slot for (the batch
//      containing) page N-1, and also that a Prepare slot has been written for the batch with {1,
//      2}.  Inside the mock action, fail the deletion and force the recycler to shut down (first
//      taking a snapshot of the log so we can recover).
//   4. Recover the recycler from the log snapshot, but don't start the recycler task yet
//   5. Set a repeating expectation on the mock deleter to track the pages which are deleted
//      post-recovery.  Specify that depth must equal 0, so that if we get a duplicate deletion of
//      page N-1 (the failure mode of https://github.com/mathworks/llfs/issues/30), that will fail
//      the test
//     (unexpected Mock call; we are using StrictMock)
//   6. Start the recycler
//   7. Delete the one remaining page, N-2, so that we can tell when we are finished.
//   8. Verify that the correct things happen: all pages deleted, no duplicate deletions.
//
TEST_F(PageRecyclerTest, DuplicatePageDeletion)
{
  this->recycler_options_.set_batch_size(2);

  batt::Task test_task{
      batt::Runtime::instance().schedule_task(), [&] {
        {
          batt::Status recovery = this->recover_page_recycler();
          ASSERT_TRUE(recovery.ok()) << BATT_INSPECT(recovery);
        }

        batt::Watch<usize> delete_count{0};
        batt::Watch<usize> continue_count{0};

        // Recycle the first page; allow its batch to commit.
        //
        EXPECT_CALL(this->mock_deleter_,
                    delete_pages(this->match_page_id(this->fake_page_id_[0]),
                                 ::testing::Ref(*this->recycler_), /*caller_slot=*/testing::_,
                                 /*recycle_grant=*/testing::_, /*recycle_depth=*/0))
            .WillOnce(::testing::Invoke(
                [&](const batt::Slice<const llfs::PageToRecycle>& to_delete,
                    llfs::PageRecycler& /*recycler*/, llfs::slot_offset_type /*caller_slot*/,
                    batt::Grant& recycle_grant, i32 recycle_depth) -> batt::Status {
                  LLFS_VLOG(1) << "First delete: " << batt::dump_range(to_delete)
                               << BATT_INSPECT(this->p_mem_log_->driver().get_trim_pos());

                  BATT_CHECK_EQ(recycle_depth + 1, 1);
                  {
                    std::lock_guard lock(this->recycle_pages_mutex_);
                    BATT_CHECK_OK(this->recycler_->recycle_page(this->fake_page_id_.back(),
                                                                this->get_and_incr_unique_offset(),
                                                                &recycle_grant, recycle_depth + 1));
                  }
                  delete_count.fetch_add(1);
                  BATT_CHECK_OK(continue_count.await_equal(1));
                  return batt::OkStatus();
                }));

        for (usize i = 0; i < this->fake_page_id_.size() - 2; ++i) {
          {
            std::lock_guard lock(this->recycle_pages_mutex_);
            batt::StatusOr<llfs::slot_offset_type> result = this->recycler_->recycle_page(
                this->fake_page_id_[i], this->get_and_incr_unique_offset());
            ASSERT_TRUE(result.ok()) << BATT_INSPECT(result);
          }

          if (i == 0) {
            LLFS_VLOG(1) << "waiting for " << BATT_INSPECT(this->fake_page_id_[0]);
            batt::Status status = delete_count.await_equal(1);
            ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
            LLFS_VLOG(1) << "allowing recycler task to continue...";

            // Set the expectation of a second delete, generated inside the job for the first page,
            // at depth == 1.  Once we receive this call, we save the log in a snapshot and shut
            // everything down.
            //
            EXPECT_CALL(this->mock_deleter_,
                        delete_pages(this->match_page_id(this->fake_page_id_.back()),
                                     ::testing::Ref(*this->recycler_),
                                     /*caller_slot=*/testing::_,
                                     /*recycle_grant=*/testing::_, /*recycle_depth=*/1))
                .WillOnce(::testing::Invoke(
                    [&](const batt::Slice<const llfs::PageToRecycle>& to_delete,
                        llfs::PageRecycler& /*recycler*/, llfs::slot_offset_type /*caller_slot*/,
                        batt::Grant& /*recycle_grant*/, i32 /*recycle_depth*/) -> batt::Status {
                      LLFS_VLOG(1) << "Second delete: " << batt::dump_range(to_delete)
                                   << BATT_INSPECT(this->p_mem_log_->driver().get_trim_pos());

                      // Now expect fake page [1] (possibly [2] also) to be deleted, and when it
                      // does, simulate a crash so that txn will be unresolved.
                      //
                      EXPECT_CALL(this->mock_deleter_,
                                  delete_pages(/*to_recycle=*/::testing::_,
                                               ::testing::Ref(*this->recycler_),
                                               /*caller_slot=*/testing::_,
                                               /*recycle_grant=*/testing::_, /*recycle_depth=*/0))
                          .WillOnce(::testing::InvokeWithoutArgs([&] {
                            LLFS_VLOG(1) << "Third delete; simulate crash";
                            this->save_log_snapshot();
                            this->recycler_->halt();
                            return batt::StatusCode::kClosed;
                          }));

                      delete_count.fetch_add(1);

                      return batt::OkStatus();
                    }));
          }

          //ASSERT_TRUE(result.ok()) << BATT_INSPECT(result);
        }

        continue_count.fetch_add(1);  // 0 -> 1

        // Wait for the recycler to stop and then reset everything.
        //
        this->recycler_->join();
        this->p_mem_log_ = nullptr;
        this->unique_page_recycler_ = nullptr;
        this->recycler_ = nullptr;

        // Recover a new recycler from the snapshot.
        {
          batt::Status recovery = this->recover_page_recycler(/*start=*/false);
          ASSERT_TRUE(recovery.ok()) << BATT_INSPECT(recovery);
        }

        // Expect for pages 1..n (but NOT 0) to be deleted again.
        //
        std::map<usize /*fake_page_id*/, usize /*delete_count*/> post_recovery_deletions;

        EXPECT_CALL(this->mock_deleter_,
                    delete_pages(/*to_recycle=*/::testing::_, ::testing::Ref(*this->recycler_),
                                 /*caller_slot=*/testing::_,
                                 /*recycle_grant=*/testing::_, /*recycle_depth=*/0))
            .WillRepeatedly(::testing::Invoke(
                [&](const batt::Slice<const llfs::PageToRecycle>& to_delete,
                    llfs::PageRecycler& /*recycler*/, llfs::slot_offset_type /*caller_slot*/,
                    batt::Grant& /*recycle_grant*/, i32 /*recycle_depth*/) -> batt::Status  //
                {
                  // TODO [tastolfi 2023-05-22] check more of the inputs to this function

                  LLFS_VLOG(1) << "Post-recovery delete: " << batt::dump_range(to_delete);

                  for (const llfs::PageToRecycle& p : to_delete) {
                    post_recovery_deletions[p.page_id.int_value()] += 1;
                  }

                  delete_count.fetch_add(to_delete.size());

                  LLFS_VLOG(1) << BATT_INSPECT(delete_count.get_value());

                  return batt::OkStatus();
                }));

        // Start the recycler.
        //
        this->recycler_->start();

        // Insert the final page after the recovery; it will be processed in FIFO order, i.e. after
        // everything else the recycler has queued.
        //
        {
          std::lock_guard lock(this->recycle_pages_mutex_);
          batt::StatusOr<llfs::slot_offset_type> result =
              this->recycler_->recycle_page(this->fake_page_id_[this->fake_page_id_.size() - 2],
                                            this->get_and_incr_unique_offset());
        }

        // Wait for all pages to be deleted.
        //
        BATT_CHECK_OK(delete_count.await_equal(this->fake_page_id_.size()));

        // These two should have been deleted prior to the simulated crash recovery.
        //
        EXPECT_EQ(post_recovery_deletions[this->fake_page_id_.front().int_value()], 0u);
        EXPECT_EQ(post_recovery_deletions[this->fake_page_id_.back().int_value()], 0u);

        for (usize i = 1; i < this->fake_page_id_.size() - 1; ++i) {
          EXPECT_EQ(post_recovery_deletions[this->fake_page_id_[i].int_value()], 1u)
              << BATT_INSPECT(i);
        }
      }};

  test_task.join();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(PageRecyclerTest, NoRefreshBatchedPage)
{
  enum TestStep : i32 {
    kWaitingForDeletePages,
    kDeletePagesCalled,
    kDeletePagesOkToReturn,
  };

  // Initialize helpers and test data.
  //
  batt::Watch<i32> test_step{kWaitingForDeletePages};

  batt::Task test_task{
      batt::Runtime::instance().schedule_task(), [&] {
        // Create the PageRecycler to test.
        //
        batt::Status recovery = this->recover_page_recycler();
        ASSERT_TRUE(recovery.ok()) << BATT_INSPECT(recovery);

        // Expect some pages to be deleted; when they are, verify the page_ids, and wait until the
        // main test task signals it is OK to continue.
        //
        EXPECT_CALL(this->mock_deleter_,
                    delete_pages(this->match_page_id(this->fake_page_id_[0]),
                                 ::testing::Ref(*this->recycler_), /*caller_slot=*/testing::_,
                                 /*recycle_grant=*/testing::_, /*recycle_depth=*/0))
            .WillOnce(::testing::InvokeWithoutArgs([&] {
              BATT_CHECK_EQ(test_step.get_value(), kWaitingForDeletePages);

              test_step.set_value(kDeletePagesCalled);
              test_step.await_equal(kDeletePagesOkToReturn).IgnoreError();
              test_step.set_value(kWaitingForDeletePages);

              return batt::OkStatus();
            }));

        // Give some PageIds to delete.
        //
        {
          std::lock_guard lock(this->recycle_pages_mutex_);
          batt::StatusOr<llfs::slot_offset_type> result = this->recycler_->recycle_page(
              this->fake_page_id_[0], this->get_and_incr_unique_offset());

          ASSERT_TRUE(result.ok()) << BATT_INSPECT(result);
        }

        // Wait for the MockPageDeleter to be invoked.
        //
        ASSERT_TRUE(test_step.await_equal(kDeletePagesCalled).ok());

        // While the MockPageDeleter is still active, insert enough new pages to ensure that we will
        // refresh the page we just recycled.
        //
        for (usize i = 1; i < this->fake_page_id_.size(); ++i) {
          {
            std::lock_guard lock(this->recycle_pages_mutex_);
            batt::StatusOr<llfs::slot_offset_type> result = this->recycler_->recycle_page(
                this->fake_page_id_[i], this->get_and_incr_unique_offset());

            ASSERT_TRUE(result.ok()) << BATT_INSPECT(result);
            batt::Status flush_status = this->recycler_->await_flush(*result);

            EXPECT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
          }
        }

        this->save_log_snapshot();
        this->p_mem_log_->close().IgnoreError();

        test_step.set_value(kDeletePagesOkToReturn);
      }};

  test_task.join();

  ASSERT_TRUE(this->mem_log_snapshot_);

  // Scan the log to make sure that the first (deleted) page was never refreshed.
  //
  llfs::MemoryLogDevice mem_log2{this->get_log_size()};

  mem_log2.restore_snapshot(*this->mem_log_snapshot_, llfs::LogReadMode::kDurable);

  std::unique_ptr<llfs::LogDevice::Reader> log_reader =
      mem_log2.new_reader(/*slot_lower_bound=*/batt::None, llfs::LogReadMode::kDurable);
  llfs::TypedSlotReader<llfs::PageRecycleEvent> slot_reader{*log_reader};

  usize page_0_insert_or_refresh_count = 0;

  slot_reader
      .run(batt::WaitForResource::kFalse,
           /*visitor=*/batt::make_case_of_visitor(
               [&](const llfs::SlotParse& slot,
                   const llfs::PageToRecycle& inserted) -> batt::Status {
                 LLFS_VLOG(1) << slot.offset << ": " << inserted;
                 if (llfs::PageId{inserted.page_id} == this->fake_page_id_[0] &&
                     inserted.batch_slot == batt::None) {
                   page_0_insert_or_refresh_count += 1;
                   EXPECT_EQ(inserted.depth, 0);
                 }
                 return batt::OkStatus();
               },
               [](const llfs::SlotParse& slot, const auto& event) {
                 LLFS_VLOG(1) << slot.offset << ": " << event;
                 return batt::OkStatus();
               }))
      .IgnoreError();

  EXPECT_EQ(page_0_insert_or_refresh_count, 1u);
}

}  // namespace
