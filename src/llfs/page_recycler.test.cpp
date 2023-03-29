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

constexpr usize kNumFakePageIds = 10;

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
  static constexpr auto kMaxRefsPerPage = llfs::MaxRefsPerPage{16};

  const u64 kLogSize = PageRecycler::calculate_log_size(
      kMaxRefsPerPage, /*max_buffered_page_count=*/llfs::PageCount{kNumFakePageIds});

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void SetUp() override
  {
    llfs::suppress_log_output_for_test() = true;

    for (usize i = 0; i < this->fake_page_id_.size(); ++i) {
      this->fake_page_id_[i] = llfs::PageId{i + 0x101};
    }
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

  batt::Status recover_page_recycler()
  {
    static const std::string_view kTestRecyclerName = "TestRecycler";

    BATT_CHECK_EQ(this->recycler_, nullptr);
    BATT_CHECK_EQ(this->unique_page_recycler_, nullptr);

    batt::StatusOr<std::unique_ptr<llfs::PageRecycler>> page_recycler_recovery =
        llfs::PageRecycler::recover(
            batt::Runtime::instance().default_scheduler(), kTestRecyclerName, kMaxRefsPerPage,
            this->mock_deleter_, *std::make_unique<llfs::BasicLogDeviceFactory>([this] {
              auto mem_log = std::make_unique<llfs::MemoryLogDevice>(kLogSize);
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

      // Important: Start the PageRecycler!
      //
      this->recycler_->start();
    }

    return page_recycler_recovery.status();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  std::unordered_map<PageId, std::unique_ptr<FakePage>, PageId::Hash> fake_pages_;
  std::vector<PageId> fake_page_root_set_;
  PageRecycler* recycler_ = nullptr;
  ::testing::StrictMock<MockPageDeleter> mock_deleter_;
  batt::Optional<llfs::LogDeviceSnapshot> mem_log_snapshot_;
  llfs::MemoryLogDevice* p_mem_log_ = nullptr;
  std::array<llfs::PageId, kNumFakePageIds> fake_page_id_;
  std::unique_ptr<llfs::PageRecycler> unique_page_recycler_;
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
      this->recursive_recycle_events_.push(result);
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
      this->recursive_recycle_events_.push(caller_slot);
      result = caller_slot;
    } else {
      // Recursively recycle any newly dead pages.  If we try to recycle the same page multiple
      // times, that is OK, since PageIds are never reused.
      //
      result = this->test_->recycler_->recycle_pages(as_slice(dead_pages),  //
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

    this->recursive_recycle_events_.push(slot);
  }

  void notify_failure(PageRecycler& recycler, Status failure) override
  {
    const boost::uuids::uuid& caller_uuid = recycler.uuid();
    auto iter = this->current_slot_.find(caller_uuid);
    BATT_CHECK_NE(iter, this->current_slot_.end());
    BATT_CHECK(!failure.ok());

    this->recursive_recycle_events_.push(failure);
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
  const usize fake_page_count = 256;
  const u32 max_branching_factor = 8;

  for (u64 seed = 0; seed < 10000; ++seed) {
    std::default_random_engine rng{seed};
    for (usize i = 0; i < 10; ++i) {
      (void)rng();
    }
    this->generate_fake_pages(rng, fake_page_count, max_branching_factor);

    const u64 log_size = PageRecycler::calculate_log_size(MaxRefsPerPage{max_branching_factor});
    LLFS_VLOG(1) << BATT_INSPECT(log_size);

    EXPECT_EQ(PageRecycler::calculate_max_buffered_page_count(MaxRefsPerPage{max_branching_factor},
                                                              log_size),
              PageRecycler::default_max_buffered_page_count(MaxRefsPerPage{max_branching_factor}));

    MemoryLogDevice mem_log{log_size};

    auto fake_log_state = std::make_shared<FakeLogDevice::State>();

    FakeLogDeviceFactory<MemoryLogStorageDriver> fake_log_factory{mem_log, mem_log.driver().impl(),
                                                                  batt::make_copy(fake_log_state)};

    FakePageDeleter fake_deleter{this};

    StatusOr<std::unique_ptr<PageRecycler>> status_or_recycler = PageRecycler::recover(
        /*TODO [tastolfi 2022-01-21] use fake*/ batt::Runtime::instance().default_scheduler(),
        "FakeRecycler", MaxRefsPerPage{max_branching_factor}, fake_deleter, fake_log_factory);

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
        StatusOr<slot_offset_type> recycle_status = recycler.recycle_pages(to_recycle);
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
        "RecoveredFakeRecycler", MaxRefsPerPage{max_branching_factor}, fake_deleter,
        fake_recovered_log_factory);

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
        batt::Status recovery1 = this->recover_page_recycler();
        ASSERT_TRUE(recovery1.ok()) << BATT_INSPECT(recovery1);

        // Expect some pages to be deleted; when they are, verify the page_ids, and wait until the
        // main test task signals it is OK to continue.
        //
        EXPECT_CALL(
            this->mock_deleter_,
            delete_pages(
                ::testing::Truly([&](const batt::Slice<const llfs::PageToRecycle>& to_delete) {
                  return to_delete.size() == 1 && to_delete[0].page_id == this->fake_page_id_[0];
                }),
                ::testing::_ /*recycler*/, testing::_ /*caller_slot*/, testing::_ /*recycle_grant*/,
                0 /*recycle_depth*/))
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
          batt::StatusOr<llfs::slot_offset_type> result =
              this->recycler_->recycle_page(this->fake_page_id_[0]);

          ASSERT_TRUE(result.ok()) << BATT_INSPECT(result);
        }

        // Wait for the MockPageDeleter to be invoked.
        //
        ASSERT_TRUE(test_step.await_equal(kDeletePagesCalled).ok());

        // While the MockPageDeleter is still active, insert enough new pages to ensure that we will
        // refresh the page we just recycled.
        //
        for (usize i = 1; i < this->fake_page_id_.size(); ++i) {
          batt::StatusOr<llfs::slot_offset_type> result = this->recycler_->recycle_pages(
              batt::as_slice(std::vector<PageId>{this->fake_page_id_[i]}));
          ASSERT_TRUE(result.ok()) << BATT_INSPECT(result);

          batt::Status flush_status = this->recycler_->await_flush(*result);
          EXPECT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);
        }

        this->mem_log_snapshot_ =
            llfs::LogDeviceSnapshot::from_device(*this->p_mem_log_, llfs::LogReadMode::kDurable);
        this->p_mem_log_->close().IgnoreError();

        test_step.set_value(kDeletePagesOkToReturn);
      }};

  test_task.join();

  ASSERT_TRUE(this->mem_log_snapshot_);

  // Scan the log to make sure that the first (deleted) page was never refreshed.
  //
  llfs::MemoryLogDevice mem_log2{kLogSize};

  mem_log2.restore_snapshot(*this->mem_log_snapshot_, llfs::LogReadMode::kDurable);

  std::unique_ptr<llfs::LogDevice::Reader> log_reader =
      mem_log2.new_reader(/*slot_lower_bound=*/batt::None, llfs::LogReadMode::kDurable);
  llfs::TypedSlotReader<llfs::PageRecycleEvent> slot_reader{*log_reader};

  usize page_0_count = 0;

  slot_reader
      .run(batt::WaitForResource::kFalse,
           /*visitor=*/batt::make_case_of_visitor(
               [&](const llfs::SlotParse&, const llfs::PageToRecycle& inserted) -> batt::Status {
                 if (llfs::PageId{inserted.page_id} == this->fake_page_id_[0]) {
                   page_0_count += 1;
                   EXPECT_EQ(inserted.depth, 0);
                 }
                 return batt::OkStatus();
               },
               [](auto&&...) {
                 return batt::OkStatus();
               }))
      .IgnoreError();

  EXPECT_EQ(page_0_count, 1u);
}

}  // namespace
