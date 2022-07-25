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

#include <llfs/testing/fake_log_device.hpp>

#include <llfs/memory_log_device.hpp>
#include <llfs/memory_page_cache.hpp>
#include <llfs/opaque_page_view.hpp>

#include <batteries/state_machine_model.hpp>

namespace {

using namespace llfs::constants;
using namespace llfs::int_types;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct UpsertEvent {
  llfs::little_i32 key;
  llfs::little_i32 value;
};

LLFS_SIMPLE_PACKED_TYPE(UpsertEvent);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct RemoveEvent {
  llfs::little_i32 key;
};

LLFS_SIMPLE_PACKED_TYPE(RemoveEvent);

using TestVolumeEvent =
    llfs::PackedVariant<UpsertEvent, RemoveEvent, llfs::PackedArray<llfs::PackedPageId>>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

u8 expected_byte_value(llfs::PageId page_id)
{
  const u64 v8 = llfs::PageId::Hash{}(page_id);
  const u32 v4 = (v8 >> 32) ^ v8;
  const u16 v2 = (v4 >> 16) ^ v4;
  return (v2 >> 8) ^ v2;
}

class VolumeTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->reset_cache();
    this->reset_logs();
  }

  void TearDown() override
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void reset_cache()
  {
    LLFS_VLOG(1) << "creating PageCache";

    llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> page_cache_created =
        llfs::make_memory_page_cache(batt::Runtime::instance().default_scheduler(),
                                     /*arena_sizes=*/
                                     {
                                         {llfs::PageCount{16}, llfs::PageSize{256}},
                                     },
                                     max_refs_per_page);

    ASSERT_TRUE(page_cache_created.ok());

    this->page_cache = std::move(*page_cache_created);
  }

  void reset_logs()
  {
    this->root_log.emplace(1 * kMiB);

    const u64 recycler_log_size = llfs::PageRecycler::calculate_log_size(max_refs_per_page);

    EXPECT_EQ(llfs::PageRecycler::default_max_buffered_page_count(max_refs_per_page),
              ::llfs::PageRecycler::calculate_max_buffered_page_count(max_refs_per_page,
                                                                      recycler_log_size));

    this->recycler_log.emplace(recycler_log_size);
  }

  void save_uuids(const llfs::Volume& test_volume)
  {
    this->volume_uuid = test_volume.get_volume_uuid();
    this->recycler_uuid = test_volume.get_recycler_uuid();
    this->trimmer_uuid = test_volume.get_trimmer_uuid();
  }

  void validate_uuids(const llfs::Volume& test_volume) const
  {
    EXPECT_EQ(this->volume_uuid, test_volume.get_volume_uuid());
    EXPECT_EQ(this->recycler_uuid, test_volume.get_recycler_uuid());
    EXPECT_EQ(this->trimmer_uuid, test_volume.get_trimmer_uuid());
  }

  template <typename SlotVisitorFn>
  std::unique_ptr<llfs::Volume> open_volume_or_die(llfs::LogDeviceFactory& root_log,
                                                   llfs::LogDeviceFactory& recycler_log,
                                                   SlotVisitorFn&& slot_visitor_fn)
  {
    llfs::StatusOr<std::unique_ptr<llfs::Volume>> test_volume_recovered = llfs::Volume::recover(
        llfs::VolumeRecoverParams{
            &batt::Runtime::instance().default_scheduler(),
            llfs::VolumeOptions{
                .name = "test_volume",
                .uuid = llfs::None,
                .max_refs_per_page = max_refs_per_page,
                .trim_lock_update_interval = llfs::TrimLockUpdateInterval{0u},
            },
            this->page_cache,
            /*root_log=*/&root_log,
            /*recycler_log=*/&recycler_log,
            nullptr,
        },  //
        BATT_FORWARD(slot_visitor_fn));

    BATT_CHECK(test_volume_recovered.ok());

    return std::move(*test_volume_recovered);
  }

  std::unordered_map<i32, i32> read_volume(llfs::Volume& test_volume)
  {
    std::unordered_map<i32, i32> data;

    llfs::StatusOr<llfs::TypedVolumeReader<TestVolumeEvent>> reader_created =
        test_volume.typed_reader<TestVolumeEvent>(
            llfs::SlotRangeSpec{
                .lower_bound = llfs::None,
                .upper_bound = llfs::None,
            },
            llfs::LogReadMode::kDurable);

    BATT_CHECK(reader_created.ok());

    auto reader = std::move(*reader_created);
    auto reader_copy = std::move(reader);

    reader = std::move(reader_copy);

    for (;;) {
      llfs::StatusOr<usize> n_slots_visited = reader.visit_typed_next(
          batt::WaitForResource::kFalse,
          [&data](const llfs::SlotParse& slot, const UpsertEvent& event) {
            data[event.key] = event.value;
            return llfs::OkStatus();
          },
          [&data](const llfs::SlotParse& slot, const RemoveEvent& event) {
            data.erase(event.key);
            return llfs::OkStatus();
          },
          [](const llfs::SlotParse& slot, const llfs::BoxedSeq<llfs::PageId>&) {
            return llfs::OkStatus();
          });

      BATT_CHECK_OK(n_slots_visited);
      if (*n_slots_visited == 0) {
        break;
      }
    }

    return data;
  }

  llfs::StatusOr<llfs::PinnedPage> make_opaque_page(llfs::PageCacheJob& job)
  {
    llfs::StatusOr<std::shared_ptr<llfs::PageBuffer>> page_allocated =
        job.new_page(llfs::PageSize{256}, batt::WaitForResource::kFalse, llfs::Caller::Unknown);

    BATT_REQUIRE_OK(page_allocated);

    const llfs::PageId page_id = page_allocated->get()->page_id();
    llfs::MutableBuffer buffer = page_allocated->get()->mutable_payload();

    std::memset(buffer.data(), expected_byte_value(page_id), buffer.size());

    return job.pin_new(std::make_shared<llfs::OpaquePageView>(std::move(*page_allocated)),
                       llfs::Caller::Unknown);
  }

  bool verify_opaque_page(llfs::PageId page_id, llfs::Optional<i32> expected_ref_count = llfs::None)
  {
    bool ok = true;

    llfs::StatusOr<llfs::PinnedPage> loaded = this->page_cache->get(page_id);

    EXPECT_TRUE(loaded.ok());
    if (!loaded.ok()) {
      return false;
    }

    const llfs::ConstBuffer payload_data = (*loaded)->data()->const_payload();
    const u8* payload_bytes = static_cast<const u8*>(payload_data.data());

    EXPECT_EQ(payload_data.size(), llfs::PageBuffer::max_payload_size(llfs::PageSize{256}));
    if (payload_data.size() != llfs::PageBuffer::max_payload_size(llfs::PageSize{256})) {
      ok = false;
    }
    const u8 v = expected_byte_value(page_id);
    for (usize i = 0; i < payload_data.size(); ++i) {
      EXPECT_EQ(payload_bytes[i], v) << BATT_INSPECT(i) << BATT_INSPECT(page_id);
      if (payload_bytes[i] != v) {
        ok = false;
        break;
      }
    }

    if (expected_ref_count) {
      const llfs::PageArena& arena = this->page_cache->arena_for_page_id(page_id);
      const i32 ref_count = arena.allocator().get_ref_count(page_id).first;

      LLFS_VLOG(1) << BATT_INSPECT(page_id) << BATT_INSPECT(ref_count);

      EXPECT_EQ(ref_count, *expected_ref_count);
      if (ref_count != *expected_ref_count) {
        ok = false;
      }
    }

    return ok;
  }

  template <typename T>
  llfs::StatusOr<llfs::SlotRange> append_job(llfs::Volume& test_volume,
                                             std::unique_ptr<llfs::PageCacheJob> job, T&& payload)
  {
    auto event = llfs::pack_as_variant<TestVolumeEvent>(BATT_FORWARD(payload));

    llfs::StatusOr<llfs::AppendableJob> appendable_job =
        llfs::make_appendable_job(std::move(job), llfs::PackableRef{event});

    BATT_REQUIRE_OK(appendable_job);

    const usize required_size = test_volume.calculate_grant_size(*appendable_job);

    LLFS_VLOG(1) << BATT_INSPECT(required_size);

    llfs::StatusOr<batt::Grant> grant =
        test_volume.reserve(required_size, batt::WaitForResource::kFalse);

    BATT_REQUIRE_OK(grant);

    EXPECT_EQ(grant->size(), required_size);

    return test_volume.append(std::move(*appendable_job), *grant);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  const llfs::MaxRefsPerPage max_refs_per_page{8};

  batt::SharedPtr<llfs::PageCache> page_cache;

  llfs::Optional<llfs::MemoryLogDevice> root_log;

  llfs::Optional<llfs::MemoryLogDevice> recycler_log;

  boost::uuids::uuid volume_uuid;

  boost::uuids::uuid recycler_uuid;

  boost::uuids::uuid trimmer_uuid;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST_F(VolumeTest, RecoverEmptyVolume)
{
  {
    LLFS_VLOG(1) << "creating Volume";

    for (int i = 0; i < 10; ++i) {
      auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
      auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

      std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
          fake_root_log, fake_recycler_log,
          /*slot_visitor_fn=*/[](const llfs::SlotParse&, const std::string_view& user_data) {
            return llfs::OkStatus();
          });

      if (i == 0) {
        this->save_uuids(*test_volume);
      } else {
        this->validate_uuids(*test_volume);
      }

      LLFS_VLOG(1) << "destroying Volume";
    }
  }
  LLFS_VLOG(1) << "done";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST_F(VolumeTest, ReadWriteEvents)
{
  std::vector<llfs::slot_offset_type> upsert_slots;
  {
    // Create an empty volume.
    //
    auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
    auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

    std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
        fake_root_log, fake_recycler_log,
        /*slot_visitor_fn=*/[](const llfs::SlotParse&, const auto& /*payload*/) {
          return llfs::OkStatus();
        });

    this->save_uuids(*test_volume);

    // Verify that there are no key/value pairs.
    {
      std::unordered_map<i32, i32> data = this->read_volume(*test_volume);

      EXPECT_THAT(data, ::testing::IsEmpty());
    }

    // Append slots.
    //
    for (i32 key = 0; key < 10; key += 1) {
      UpsertEvent upsert{key, key * 3 + 1};
      auto upsert_event = llfs::pack_as_variant<TestVolumeEvent>(upsert);
      auto packable_event = llfs::PackableRef{upsert_event};

      llfs::StatusOr<batt::Grant> grant = test_volume->reserve(
          test_volume->calculate_grant_size(packable_event), batt::WaitForResource::kFalse);

      ASSERT_TRUE(grant.ok());

      llfs::StatusOr<llfs::SlotRange> appended = test_volume->append(packable_event, *grant);

      ASSERT_TRUE(appended.ok()) << appended.status();

      upsert_slots.emplace_back(appended->upper_bound);
    }

    // Flush the log.
    //
    llfs::StatusOr<llfs::SlotRange> flushed =
        test_volume->sync(llfs::LogReadMode::kDurable, llfs::SlotUpperBoundAt{upsert_slots.back()});

    ASSERT_TRUE(flushed.ok());

    // Re-read the root log; now we should see the data we inserted.
    //
    {
      std::unordered_map<i32, i32> data = this->read_volume(*test_volume);

      EXPECT_THAT(data, ::testing::UnorderedElementsAre(
                            std::make_pair(0, 1), std::make_pair(1, 4), std::make_pair(2, 7),
                            std::make_pair(3, 10), std::make_pair(4, 13), std::make_pair(5, 16),
                            std::make_pair(6, 19), std::make_pair(7, 22), std::make_pair(8, 25),
                            std::make_pair(9, 28)));
    }
    //
    // Close the first volume; we will re-open the log to recover our data below.
  }

  // Re-open the volume; we should still see the data we inserted.
  //
  {
    auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
    auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

    std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
        fake_root_log, fake_recycler_log,
        /*slot_visitor_fn=*/[](const llfs::SlotParse&, const std::string_view&) {
          return llfs::OkStatus();
        });

    {
      std::unordered_map<i32, i32> data = this->read_volume(*test_volume);

      EXPECT_THAT(data, ::testing::UnorderedElementsAre(
                            std::make_pair(0, 1), std::make_pair(1, 4), std::make_pair(2, 7),
                            std::make_pair(3, 10), std::make_pair(4, 13), std::make_pair(5, 16),
                            std::make_pair(6, 19), std::make_pair(7, 22), std::make_pair(8, 25),
                            std::make_pair(9, 28)));
    }

    // Trim the log to erase some data.
    //
    LLFS_VLOG(1) << "Trimming volume to " << upsert_slots[4];
    llfs::Status trim_set = test_volume->trim(upsert_slots[4]);

    ASSERT_TRUE(trim_set.ok());

    LLFS_VLOG(1) << "Awaiting trim offset " << upsert_slots[4];
    llfs::Status trimmed = test_volume->await_trim(upsert_slots[4]);

    ASSERT_TRUE(trimmed.ok());
  }

  // Re-open the volume one last time; we should see the half we didn't trim.
  //
  {
    auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
    auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

    std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
        fake_root_log, fake_recycler_log,
        /*slot_visitor_fn=*/[](const llfs::SlotParse&, const auto& /*payload*/) {
          return llfs::OkStatus();
        });

    {
      std::unordered_map<i32, i32> data = this->read_volume(*test_volume);

      EXPECT_THAT(data, ::testing::UnorderedElementsAre(
                            std::make_pair(5, 16), std::make_pair(6, 19), std::make_pair(7, 22),
                            std::make_pair(8, 25), std::make_pair(9, 28)));
    }

    this->validate_uuids(*test_volume);
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. Reader::clone_lock() - keep trim from happening when there is no other barrier
//  2. Create multiple Readers, trim the Volume, verify that only the last Reader's trim
//     allows the log to be trimmed.
//
TEST_F(VolumeTest, TrimControl)
{
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. Write a page to a slot, read it back, make sure it doesn't get recycled.
//  2. Write slots with 1..max_refs_per_page new pages
///    a. trim the log and verify that the pages are released
//  3. Write slots with new pages, and reference some random subset of previously written live
//     pages.
//     a. trim the log and verify that the expected pages are released (i.e., make sure ref count >
//        0 keeps pages alive)
//
TEST_F(VolumeTest, PageJobs)
{
  // Create an empty volume.
  //
  auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
  auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

  std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
      fake_root_log, fake_recycler_log,
      /*slot_visitor_fn=*/[](const llfs::SlotParse&, const auto& /*payload*/) {
        return llfs::OkStatus();
      });

  std::vector<llfs::PageId> page_ids;
  std::vector<llfs::SlotRange> new_page_slot;

  for (usize i = 0; i < 10; ++i) {
    std::unique_ptr<llfs::PageCacheJob> job = test_volume->new_job();

    llfs::StatusOr<llfs::PinnedPage> pinned_page = this->make_opaque_page(*job);
    ASSERT_TRUE(pinned_page.ok());

    page_ids.emplace_back(get_page_id(*pinned_page));

    llfs::StatusOr<llfs::SlotRange> appended =
        this->append_job(*test_volume, std::move(job),
                         llfs::as_seq(page_ids) | llfs::seq::decayed() | llfs::seq::boxed());

    ASSERT_TRUE(appended.ok()) << BATT_INSPECT(appended.status());

    new_page_slot.emplace_back(*appended);

    std::set<llfs::PageId> unique_page_ids(page_ids.begin(), page_ids.end());
    EXPECT_THAT(unique_page_ids, ::testing::UnorderedElementsAreArray(page_ids));

    //----- --- -- -  -  -   -
    for (usize j = 0; j < page_ids.size(); ++j) {
      ASSERT_TRUE(this->verify_opaque_page(page_ids[j], /*expected_ref_count=*/2 + (i - j)))
          << BATT_INSPECT(j);
    }

    //----- --- -- -  -  -   -
    batt::Task test_reader_task{
        batt::Runtime::instance().schedule_task(),
        [&] {
          llfs::StatusOr<llfs::VolumeReader> reader = test_volume->reader(
              llfs::SlotRangeSpec{0, llfs::None}, llfs::LogReadMode::kSpeculative);

          ASSERT_TRUE(reader.ok());

          LLFS_VLOG(1) << BATT_INSPECT(reader->slot_range());

          usize job_i = 0;
          llfs::StatusOr<usize> n_slots_read = reader->consume_slots(
              batt::WaitForResource::kFalse,
              [&](const llfs::SlotParse& slot, std::string_view user_data) -> llfs::Status {
                job_i += 1;

                EXPECT_TRUE(slot.depends_on_offset);

                LLFS_VLOG(1) << "Visiting slot: " << BATT_INSPECT(slot)
                             << " user_data=" << batt::c_str_literal(user_data);

                // Parse the event at this slot and verify its contents.
                //
                auto* event =
                    (const llfs::PackedVariantInstance<
                        TestVolumeEvent, llfs::PackedArray<llfs::PackedPageId>>*)user_data.data();

                EXPECT_TRUE(event->verify_case());
                EXPECT_EQ(event->tail.size(), job_i);

                for (usize page_i = 0; page_i < job_i; ++page_i) {
                  BATT_CHECK_LT(page_i, page_ids.size());
                  EXPECT_EQ(event->tail[page_i].as_page_id(), page_ids[page_i]);
                }

                return llfs::OkStatus();
              });

          ASSERT_TRUE(n_slots_read.ok());
          EXPECT_EQ(*n_slots_read, i + 1);
        },
        "VolumeTest_PageJobs_reader_task"};

    test_reader_task.join();
  }

  EXPECT_EQ(page_ids.size(), new_page_slot.size());

  for (usize i = 0; i < new_page_slot.size(); ++i) {
    llfs::Status trimmed = test_volume->trim(new_page_slot[i].upper_bound);

    ASSERT_TRUE(trimmed.ok());

    for (usize j = 0; j < i + 1; ++j) {
      const llfs::PageId id = page_ids[j];
      const llfs::PageArena& arena = this->page_cache->arena_for_page_id(id);
      const i32 target = [&]() -> i32 {
        if (i + 1 == new_page_slot.size()) {
          return 0;
        }
        return /*total=*/(page_ids.size() - j) - /*released_so_far=*/(i - j);
      }();
      LLFS_VLOG(1) << "waiting for ref count; " << BATT_INSPECT(new_page_slot[i]) << BATT_INSPECT(i)
                   << BATT_INSPECT(j) << BATT_INSPECT(id) << BATT_INSPECT(target);
      ASSERT_TRUE(arena.allocator().await_ref_count(id, target));

      if (target > 1) {
        ASSERT_TRUE(this->verify_opaque_page(id, /*expected_ref_count=*/target));
      }
    }
  }

  LLFS_VLOG(1) << BATT_INSPECT(fake_root_log.state()->device_time);
  LLFS_VLOG(1) << BATT_INSPECT(fake_recycler_log.state()->device_time);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct VolumeCrashTestState {
  bool done = false;
};

inline bool operator==(const VolumeCrashTestState& l, const VolumeCrashTestState& r)
{
  return l.done == r.done;
}

BATT_EQUALITY_COMPARABLE(([[maybe_unused]]), VolumeCrashTestState, VolumeCrashTestState);

usize hash_value(VolumeCrashTestState s)
{
  return s.done;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeCrashTestModel
    : public batt::StateMachineModel<VolumeCrashTestState, boost::hash<VolumeCrashTestState>>
{
 public:
  using State = VolumeCrashTestState;

  struct GeneratePageJob {
  };

  struct GenerateCheckpoint {
  };

  struct Trim {
    llfs::slot_offset_type slot_offset;
  };

  using ToDoItem =
      std::variant<UpsertEvent, RemoveEvent, GeneratePageJob, GenerateCheckpoint, Trim>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit VolumeCrashTestModel(VolumeTest& test) : test_{test}
  {
  }

  State initialize() override
  {
    return {.done = false};
  }

  void enter_state(const State& s) override
  {
    this->state_ = s;
    if (this->state_.done) {
      return;
    }

    this->test_.reset_cache();
    this->test_.reset_logs();

    this->fake_root_log_.emplace(
        llfs::testing::make_fake_log_device_factory(*this->test_.root_log));

    this->fake_recycler_log_.emplace(
        llfs::testing::make_fake_log_device_factory(*this->test_.recycler_log));
  }

  void step() override
  {
    if (this->state_.done) {
      return;
    }

    usize volume_log_fail_ts = this->pick_int(0, 10);
    LLFS_VLOG(1) << volume_log_fail_ts;
    //
    // ^^^ TODO [tastolfi 2022-04-05] do something useful with this value
  }

  State leave_state() override
  {
    this->fake_root_log_ = llfs::None;
    this->fake_recycler_log_ = llfs::None;

    return {.done = true};
  }

  bool check_invariants() override
  {
    return true;
  }

  State normalize(const State& s) override
  {
    return s;
  }

 private:
  State state_;

  VolumeTest& test_;

  llfs::Optional<llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver>> fake_root_log_;

  llfs::Optional<llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver>>
      fake_recycler_log_;
};

TEST_F(VolumeTest, CrashRecovery)
{
  VolumeCrashTestModel model{*this};

  VolumeCrashTestModel::Result r = model.check_model();

  EXPECT_TRUE(r.ok);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST_F(VolumeTest, ReaderInterruptedByVolumeClose)
{
  LLFS_VLOG(1) << "creating Volume";

  auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
  auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

  std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
      fake_root_log, fake_recycler_log,
      /*slot_visitor_fn=*/[](const llfs::SlotParse&, const auto& /*payload*/) {
        return llfs::OkStatus();
      });

  boost::asio::io_context io;
  usize slots_consumed = 0;
  batt::Task reader_task{
      io.get_executor(),
      [&] {
        LLFS_VLOG(1) << "inside reader task";

        llfs::StatusOr<llfs::VolumeReader> reader = test_volume->reader(
            llfs::SlotRangeSpec{llfs::None, llfs::None}, llfs::LogReadMode::kSpeculative);
        BATT_CHECK_OK(reader);

        LLFS_VLOG(1) << "reader created";

        llfs::StatusOr<usize> result = reader->consume_slots(
            batt::WaitForResource::kTrue,
            [&](const llfs::SlotParse& /*slot_range*/, const std::string_view& /*payload*/) {
              slots_consumed += 1;
              return llfs::OkStatus();
            });

        LLFS_VLOG(1) << "consume_slots returned " << result;

        BATT_CHECK_EQ(result.status(), batt::StatusCode::kClosed);
      },
      "test reader task"};

  LLFS_VLOG(1) << "before first poll";

  io.poll();
  io.reset();

  LLFS_VLOG(1) << "after first poll; calling volume halt";

  test_volume->halt();
  this->root_log->close().IgnoreError();
  this->recycler_log->close().IgnoreError();

  LLFS_VLOG(1) << "after volume halt; before second poll";

  io.poll();
  io.reset();

  LLFS_VLOG(1) << "after second poll";

  reader_task.join();
  test_volume->join();
}

}  // namespace
