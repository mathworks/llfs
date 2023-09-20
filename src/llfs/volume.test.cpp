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
#include <llfs/packable_ref.hpp>
#include <llfs/page_graph_node.hpp>
#include <llfs/raw_volume_log_data_parser.hpp>
#include <llfs/storage_simulation.hpp>

#include <batteries/cpu_align.hpp>
#include <batteries/env.hpp>
#include <batteries/state_machine_model.hpp>

#include <cstdlib>

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

constexpr usize kTestRootLogSize = 1 * kMiB;

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
    this->root_log.emplace(kTestRootLogSize);

    const auto recycler_options =
        llfs::PageRecyclerOptions{}.set_max_refs_per_page(max_refs_per_page);

    const u64 recycler_log_size = llfs::PageRecycler::calculate_log_size(recycler_options);

    EXPECT_GE(::llfs::PageRecycler::calculate_max_buffered_page_count(recycler_options,
                                                                      recycler_log_size),
              llfs::PageRecycler::default_max_buffered_page_count(recycler_options));

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
                .trim_delay_byte_count = this->trim_delay,
            },
            this->page_cache,
            /*root_log=*/&root_log,
            /*recycler_log=*/&recycler_log,
            nullptr,
        },  //
        BATT_FORWARD(slot_visitor_fn));

    BATT_CHECK(test_volume_recovered.ok()) << BATT_INSPECT(test_volume_recovered.status());

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
          [&data](const llfs::SlotParse& /*slot*/, const UpsertEvent& event) {
            data[event.key] = event.value;
            return llfs::OkStatus();
          },
          [&data](const llfs::SlotParse& /*slot*/, const RemoveEvent& event) {
            data.erase(event.key);
            return llfs::OkStatus();
          },
          [](const llfs::SlotParse& /*slot*/, const llfs::BoxedSeq<llfs::PageId>&) {
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
    BATT_REQUIRE_OK(llfs::OpaquePageView::register_layout(job.cache()));

    llfs::StatusOr<std::shared_ptr<llfs::PageBuffer>> page_allocated = job.new_page(
        llfs::PageSize{256}, batt::WaitForResource::kFalse, llfs::OpaquePageView::page_layout_id(),
        llfs::Caller::Unknown, /*cancel_token=*/llfs::None);

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

    llfs::StatusOr<llfs::PinnedPage> loaded =
        this->page_cache->get_page(page_id, llfs::OkIfNotFound{false});

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

  /** \brief Initializes this->volume.
   */
  void open_volume()
  {
    BATT_CHECK(this->root_log);
    BATT_CHECK(this->recycler_log);

    this->fake_root_log_factory =  //
        llfs::testing::make_fake_log_device_factory(*this->root_log);

    this->fake_recycler_log_factory =  //
        llfs::testing::make_fake_log_device_factory(*this->recycler_log);

    this->volume = this->open_volume_or_die(
        *this->fake_root_log_factory, *this->fake_recycler_log_factory,
        /*slot_visitor_fn=*/[](const llfs::SlotParse&, const std::string_view& /*user_data*/) {
          return llfs::OkStatus();
        });
  }

  /** \brief Appends a single UpsertEvent with the specified key and value to this->volume.
   *
   * On success, pushes a single item onto the back of this->appended_events.
   */
  void append_event(i32 key, i32 value)
  {
    BATT_CHECK_NOT_NULLPTR(this->volume);

    UpsertEvent event{key, value};
    llfs::PackableRef packable_event{event};

    const usize required_size = this->volume->calculate_grant_size(packable_event);

    llfs::StatusOr<batt::Grant> grant =
        this->volume->reserve(required_size, batt::WaitForResource::kFalse);

    BATT_CHECK_OK(grant);

    llfs::StatusOr<llfs::SlotRange> slot = this->volume->append(packable_event, *grant);

    BATT_CHECK_OK(slot);

    this->appended_events.emplace_back(llfs::SlotWithPayload<UpsertEvent>{
        .slot_range = *slot,
        .payload = event,
    });
  }

  /** \brief Initializes this->volume with a non-zero trim_delay and appends a bunch of events.
   */
  void init_events()
  {
    constexpr usize kNumEvents = 100;

    this->trim_delay = llfs::TrimDelayByteCount{256};

    BATT_CHECK_LT(this->trim_delay, kTestRootLogSize);

    this->open_volume();
    for (usize i = 0; i < kNumEvents; ++i) {
      this->append_event(i, i + 100);
    }

    EXPECT_EQ(this->appended_events.size(), kNumEvents);

    EXPECT_TRUE(llfs::slot_less_than(this->appended_events[7].slot_range.upper_bound,  //
                                     this->trim_delay));

    EXPECT_TRUE(llfs::slot_less_than(this->trim_delay,  //
                                     this->appended_events.back().slot_range.lower_bound));
  }

  /** \brief Returns the slot offset range of the appended_events [first_event, last_event)
   * (half-open interval).
   */
  llfs::SlotRange slot_event_range(usize first_event, usize last_event) const
  {
    BATT_CHECK_LT(first_event, this->appended_events.size());
    BATT_CHECK_LT(last_event, this->appended_events.size());
    BATT_CHECK(llfs::slot_less_or_equal(first_event, last_event));

    return llfs::SlotRange{
        this->appended_events[first_event].slot_range.lower_bound,
        this->appended_events[last_event].slot_range.lower_bound,
    };
  }

  /** \brief Verifies that the passed log data contains the appended_events in the specified
   * slot_range.
   */
  void verify_events(const llfs::SlotRange& slot_range, const llfs::ConstBuffer& log_data)
  {
    const auto first = std::lower_bound(this->appended_events.begin(), this->appended_events.end(),
                                        slot_range, llfs::SlotRangeOrder{});

    auto next = first;

    const auto last = std::upper_bound(this->appended_events.begin(), this->appended_events.end(),
                                       slot_range, llfs::SlotRangeOrder{});

    llfs::RawVolumeLogDataParser parser;

    batt::StatusOr<llfs::slot_offset_type> parse_result = parser.parse_chunk(
        slot_range, log_data,
        [&](const llfs::SlotParse& slot_parse, const std::string_view& user_data) -> batt::Status {
          BATT_CHECK_NE(next, last);

          BATT_CHECK_EQ(next->slot_range, slot_parse.offset);

          BATT_CHECK_EQ(user_data.size(), sizeof(UpsertEvent));

          BATT_CHECK_EQ(next->payload.key,
                        reinterpret_cast<const UpsertEvent*>(user_data.data())->key);

          BATT_CHECK_EQ(next->payload.value,
                        reinterpret_cast<const UpsertEvent*>(user_data.data())->value);

          ++next;

          return batt::OkStatus();
        });

    ASSERT_TRUE(parse_result.ok()) << BATT_INSPECT(parse_result.status());
    EXPECT_EQ(next, last);
    ASSERT_NE(first, next);
    EXPECT_EQ(std::prev(next)->slot_range.upper_bound, *parse_result);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  const llfs::MaxRefsPerPage max_refs_per_page{8};

  llfs::TrimDelayByteCount trim_delay{0};

  batt::SharedPtr<llfs::PageCache> page_cache;

  llfs::Optional<llfs::MemoryLogDevice> root_log;

  llfs::Optional<llfs::MemoryLogDevice> recycler_log;

  boost::uuids::uuid volume_uuid;

  boost::uuids::uuid recycler_uuid;

  boost::uuids::uuid trimmer_uuid;

  llfs::Optional<llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver>>
      fake_root_log_factory;

  llfs::Optional<llfs::testing::FakeLogDeviceFactory<llfs::MemoryLogStorageDriver>>
      fake_recycler_log_factory;

  std::unique_ptr<llfs::Volume> volume;

  std::vector<llfs::SlotWithPayload<UpsertEvent>> appended_events;
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
          /*slot_visitor_fn=*/[](const llfs::SlotParse&, const std::string_view& /*user_data*/) {
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

      llfs::StatusOr<batt::Grant> grant = test_volume->reserve(
          test_volume->calculate_grant_size(upsert_event), batt::WaitForResource::kFalse);

      ASSERT_TRUE(grant.ok());

      llfs::StatusOr<llfs::SlotRange> appended = test_volume->append(upsert_event, *grant);

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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, TrimControl_VolumeReaderCloneLock)
{
  this->init_events();

  const llfs::slot_offset_type explicit_trim_offset =
      this->appended_events[50].slot_range.upper_bound;

  // Find the greatest-lower-bound slot boundary for (explicit_trim_offset - this->trim_delay)
  //
  llfs::slot_offset_type expected_trim_pos = explicit_trim_offset;
  for (usize i = 50; i > 0; --i) {
    expected_trim_pos = this->appended_events[i].slot_range.lower_bound;
    if (expected_trim_pos <= explicit_trim_offset - this->trim_delay) {
      break;
    }
  }

  EXPECT_TRUE(llfs::slot_less_than(expected_trim_pos, explicit_trim_offset));
  EXPECT_GE(explicit_trim_offset - expected_trim_pos, this->trim_delay);

  llfs::StatusOr<llfs::VolumeReader> reader =
      this->volume->reader(llfs::SlotRangeSpec{}, llfs::LogReadMode::kSpeculative);

  ASSERT_TRUE(reader.ok()) << BATT_INSPECT(reader.status());

  llfs::Status trim_status = this->volume->trim(explicit_trim_offset);

  ASSERT_TRUE(trim_status.ok());

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(this->volume->trim_control().get_lower_bound(), reader->slot_range().lower_bound);
    batt::Task::sleep(boost::posix_time::milliseconds(1));
  }

  llfs::SlotReadLock cloned_lock = reader->clone_lock();
  reader = batt::Status{batt::StatusCode::kUnknown};

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(this->volume->trim_control().get_lower_bound(), cloned_lock.slot_range().lower_bound);
    batt::Task::sleep(boost::posix_time::milliseconds(1));
  }

  cloned_lock.clear();

  EXPECT_EQ(this->volume->trim_control().get_lower_bound(), explicit_trim_offset);

  EXPECT_TRUE(this->volume->await_trim(expected_trim_pos).ok());

  EXPECT_EQ(expected_trim_pos,
            this->volume->root_log_slot_range(llfs::LogReadMode::kSpeculative).lower_bound);

  EXPECT_EQ(expected_trim_pos,
            this->volume->root_log_slot_range(llfs::LogReadMode::kDurable).lower_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, TrimControl_LastVolumeReaderCausesTrim)
{
  constexpr usize kNumReaders = 10;

  this->init_events();

  const llfs::slot_offset_type explicit_trim_offset =
      this->appended_events[50].slot_range.upper_bound;

  // Find the greatest-lower-bound slot boundary for (explicit_trim_offset - this->trim_delay)
  //
  llfs::slot_offset_type expected_trim_pos = explicit_trim_offset;
  for (usize i = 50; i > 0; --i) {
    expected_trim_pos = this->appended_events[i].slot_range.lower_bound;
    if (expected_trim_pos <= explicit_trim_offset - this->trim_delay) {
      break;
    }
  }

  EXPECT_TRUE(llfs::slot_less_than(expected_trim_pos, explicit_trim_offset));
  EXPECT_GE(explicit_trim_offset - expected_trim_pos, this->trim_delay);

  std::vector<llfs::VolumeReader> readers;

  for (usize i = 0; i < kNumReaders; ++i) {
    readers.emplace_back(BATT_OK_RESULT_OR_PANIC(
        this->volume->reader(llfs::SlotRangeSpec{}, llfs::LogReadMode::kSpeculative)));
  }

  llfs::Status trim_status = this->volume->trim(explicit_trim_offset);

  ASSERT_TRUE(trim_status.ok());

  while (!readers.empty()) {
    for (int i = 0; i < 25; ++i) {
      EXPECT_EQ(this->volume->trim_control().get_lower_bound(), 0u);
      batt::Task::sleep(boost::posix_time::milliseconds(1));
    }
    readers.pop_back();
  }

  EXPECT_EQ(this->volume->trim_control().get_lower_bound(), explicit_trim_offset);

  EXPECT_TRUE(this->volume->await_trim(expected_trim_pos).ok());

  EXPECT_EQ(expected_trim_pos,
            this->volume->root_log_slot_range(llfs::LogReadMode::kSpeculative).lower_bound);

  EXPECT_EQ(expected_trim_pos,
            this->volume->root_log_slot_range(llfs::LogReadMode::kDurable).lower_bound);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. read_lock invalid - should fail
//  2. slot_range.upper_bound too large
//  3. slot_range.lower_bound too small
//  4. slot_range ok, not equal to read_lock.slot_range()
//  5. slot_range.lower_bound is between (read_lock.slot_range().lower_bound - trim_delay) and
//  (read_lock.slot_range().lower_bound) - OK
//  6. slot_range.lower_bound equals (read_lock.slot_range().lower_bound - trim_delay) - OK
//  7. read_lock is otherwise OK, but it just from the wrong SlotLockManager - fail
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_InvalidReadLock)
{
  this->open_volume();

  llfs::SlotReadLock lock;

  EXPECT_FALSE(lock);

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(lock);

  EXPECT_FALSE(log_data.ok());
  EXPECT_EQ(log_data.status(), batt::StatusCode::kInvalidArgument);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_ReadLockWrongSlotManager)
{
  this->open_volume();

  llfs::SlotRange slot_range = this->volume->root_log_slot_range(llfs::LogReadMode::kDurable);
  llfs::SlotLockManager manager;
  llfs::StatusOr<llfs::SlotReadLock> lock = manager.lock_slots(
      slot_range, /*holder=*/"VolumeTest.GetRootLogData_ReadLockWrongSlotManager");

  ASSERT_TRUE(lock.ok()) << BATT_INSPECT(lock.status());
  EXPECT_TRUE(*lock);

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock);

  EXPECT_FALSE(log_data.ok());
  EXPECT_EQ(log_data.status(), batt::StatusCode::kInvalidArgument);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_Ok)
{
  this->init_events();

  EXPECT_GT(this->trim_delay, 0u);

  const auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock =
      this->volume->lock_slots(slot_range, llfs::LogReadMode::kSpeculative,
                               /*holder=*/"VolumeTest.GetRootLogData_Ok");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock);

  ASSERT_TRUE(log_data.ok()) << BATT_INSPECT(log_data.status());
  ASSERT_NO_FATAL_FAILURE(this->verify_events(slot_range, *log_data));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeUpperBoundOverflow)
{
  this->init_events();

  const auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound,
          slot_range.upper_bound - 1,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeUpperBoundOverflow");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  ASSERT_TRUE(log_data.ok()) << BATT_INSPECT(log_data.status());
  ASSERT_NO_FATAL_FAILURE(this->verify_events(slot_range, *log_data));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeLowerBoundUnderflow)
{
  this->init_events();

  const auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound + this->trim_delay / 2,
          slot_range.upper_bound,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeLowerBoundUnderflow");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  ASSERT_TRUE(log_data.ok()) << BATT_INSPECT(log_data.status());
  ASSERT_NO_FATAL_FAILURE(this->verify_events(slot_range, *log_data));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeSubsetOfReadLock_Strict)
{
  this->init_events();

  const auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound + this->trim_delay / 2,
          slot_range.upper_bound - 1,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeSubsetOfReadLock_Strict");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  ASSERT_TRUE(log_data.ok()) << BATT_INSPECT(log_data.status());
  ASSERT_NO_FATAL_FAILURE(this->verify_events(slot_range, *log_data));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeSubsetOfReadLock_UpperAtLimit)
{
  this->init_events();

  auto slot_range = this->slot_event_range(4, 80);
  slot_range.upper_bound =
      this->volume->root_log_slot_range(llfs::LogReadMode::kSpeculative).upper_bound;

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound,
          slot_range.lower_bound + 1,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeSubsetOfReadLock_UpperAtLimit");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  ASSERT_TRUE(log_data.ok()) << BATT_INSPECT(log_data.status());
  ASSERT_NO_FATAL_FAILURE(this->verify_events(slot_range, *log_data));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeSubsetOfReadLock_UpperOverflow)
{
  this->init_events();

  auto slot_range = this->slot_event_range(4, 80);
  slot_range.upper_bound =
      this->volume->root_log_slot_range(llfs::LogReadMode::kSpeculative).upper_bound + 1;

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound,
          slot_range.lower_bound + 1,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeSubsetOfReadLock_UpperOverflow");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  EXPECT_FALSE(log_data.ok());
  EXPECT_EQ(log_data.status(), batt::StatusCode::kOutOfRange);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeSubsetOfReadLock_LowerAtLimit)
{
  this->init_events();

  auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound + this->trim_delay,
          slot_range.upper_bound,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeSubsetOfReadLock_LowerAtLimit");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  ASSERT_TRUE(log_data.ok()) << BATT_INSPECT(log_data.status());
  ASSERT_NO_FATAL_FAILURE(this->verify_events(slot_range, *log_data));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeSubsetOfReadLock_LowerUnderflow)
{
  this->init_events();

  auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound + this->trim_delay + 1,
          slot_range.upper_bound,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeSubsetOfReadLock_LowerUnderflow");

  ASSERT_TRUE(lock.ok()) << lock.status();

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  EXPECT_FALSE(log_data.ok());
  EXPECT_EQ(log_data.status(), batt::StatusCode::kOutOfRange);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeSubsetOfReadLock_NegativeSize)
{
  this->init_events();

  auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound,
          slot_range.upper_bound,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeSubsetOfReadLock_LowerUnderflow");

  ASSERT_TRUE(lock.ok()) << lock.status();

  //----- --- -- -  -  -   -
  std::swap(slot_range.lower_bound, slot_range.upper_bound);
  //----- --- -- -  -  -   -

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  EXPECT_FALSE(log_data.ok());
  EXPECT_EQ(log_data.status(), batt::StatusCode::kInvalidArgument);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, GetRootLogData_SlotRangeSubsetOfReadLock_ZeroSize)
{
  this->init_events();

  auto slot_range = this->slot_event_range(4, 80);

  llfs::StatusOr<llfs::SlotReadLock> lock = this->volume->lock_slots(
      llfs::SlotRange{
          slot_range.lower_bound,
          slot_range.upper_bound,
      },
      llfs::LogReadMode::kSpeculative,
      /*holder=*/"VolumeTest.GetRootLogData_SlotRangeSubsetOfReadLock_LowerUnderflow");

  ASSERT_TRUE(lock.ok()) << lock.status();

  //----- --- -- -  -  -   -
  slot_range.upper_bound = slot_range.lower_bound;
  //----- --- -- -  -  -   -

  llfs::StatusOr<llfs::ConstBuffer> log_data = this->volume->get_root_log_data(*lock, slot_range);

  ASSERT_TRUE(log_data.ok()) << BATT_INSPECT(log_data.status());
  EXPECT_EQ(log_data->size(), 0u);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. Write a page to a slot, read it back, make sure it doesn't get recycled.
//  2. Write slots with 1..max_refs_per_page new pages
///    a. trim the log and verify that the pages are released
//  3. Write slots with new pages, and reference some random subset of previously written live
//     pages.
//     a. trim the log and verify that the expected pages are released (i.e., make sure ref count
//     >
//        0 keeps pages alive)
//
TEST_F(VolumeTest, PageJobs)
{
  ASSERT_FALSE(batt::Runtime::instance().is_halted());

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
                  EXPECT_EQ(event->tail[page_i].unpack(), page_ids[page_i]);
                }

                return llfs::OkStatus();
              });

          ASSERT_TRUE(n_slots_read.ok());
          EXPECT_EQ(*n_slots_read, i + 1);
        },
        "VolumeTest_PageJobs_reader_task"};

    test_reader_task.join();
  }

  batt::Task test_trim_task{
      batt::Runtime::instance().schedule_task(),
      [&] {
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
            LLFS_VLOG(1) << "waiting for ref count; " << BATT_INSPECT(new_page_slot[i])
                         << BATT_INSPECT(i) << BATT_INSPECT(j) << BATT_INSPECT(id)
                         << BATT_INSPECT(target);
            ASSERT_TRUE(arena.allocator().await_ref_count(id, target));

            if (target > 1) {
              ASSERT_TRUE(this->verify_opaque_page(id, /*expected_ref_count=*/target));
            }
          }
        }
      },
      "VolumeTest_PageJobs_trim_task"};
  test_trim_task.join();

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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class VolumeSimTest : public ::testing::Test
{
 public:
  using RecoverySimTestSlot = llfs::PackedVariant<llfs::PackedPageId>;

  struct RecoverySimState {
    u32 seed = 0;

    /** \brief The page to be committed in the first job.
     */
    llfs::PageId first_page_id;

    /** \brief Committed in the second job; this is the root page (it refers to third page).
     */
    llfs::PageId second_root_page_id;

    /** \brief Committed in the second job; this is not referenced from the root log, only from
     * second page.
     */
    llfs::PageId third_page_id;

    /** \brief The status value returned from commit_second_job_pre_crash.
     */
    batt::Status pre_crash_status;

    /** \brief Sets expectations for recovery, post crash.
     */
    bool second_job_will_commit = false;

    /** \brief Sets expectations for recovery, post crash.
     */
    bool second_job_will_not_commit = true;

    // State variables to track how much we have recovered.
    //
    bool recovered_first_page = false;
    bool recovered_second_page = false;
    bool no_unknown_pages = true;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief Returns a slot visitor function for use when recovering/verifying the Volume,
     * post-crash.
     */
    auto get_slot_visitor();

    /** \brief Resets all member data that might have been modified by the slot visitor.
     */
    void reset_visitor_outputs();
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Builds and commits a new page in the specified job, with references to the
   * specified page ids.
   */
  static batt::StatusOr<llfs::PageId> build_page_with_refs_to(
      const std::vector<llfs::PageId>& referenced_page_ids, llfs::PageSize page_size,
      llfs::PageCacheJob& job, llfs::StorageSimulation& sim);

  /** \brief Commits a slot and job as a page transaction to the volume.
   */
  static batt::StatusOr<llfs::SlotRange> commit_job_to_root_log(
      std::unique_ptr<llfs::PageCacheJob> job, llfs::PageId root_page_id, llfs::Volume& volume,
      llfs::StorageSimulation& sim);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Runs the Volume crash/recovery simulation test.
   *
   * \param seed Used to initialize the pseudo-random number generator that drives the simulation.
   */
  void run_recovery_sim(u32 seed);

  /** \brief Commit one job with a page from the 1kb device.
   */
  void commit_first_job(RecoverySimState& state, llfs::StorageSimulation& sim,
                        llfs::Volume& volume);

  /** \brief Commits a second job that references two new pages and one old one (the page from the
   * first job), all pages from a different PageDevice.
   *
   * Unlike the first job, this one is allowed (in fact, expected) to fail.
   */
  batt::Status commit_second_job_pre_crash(RecoverySimState& state, llfs::StorageSimulation& sim,
                                           llfs::Volume& volume);

  /** \brief Checks to make sure that recovery was successful.
   */
  void verify_post_recovery_expectations(RecoverySimState& state, llfs::StorageSimulation& sim,
                                         llfs::Volume& volume);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const llfs::PageCount pages_per_device = llfs::PageCount{4};
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeSimTest, RecoverySimulation)
{
  static const u32 kInitialSeed =  //
      batt::getenv_as<u32>("LLFS_VOLUME_SIM_SEED").value_or(253689123);

  static const u32 kNumSeeds =  //
      batt::getenv_as<u32>("LLFS_VOLUME_SIM_COUNT").value_or(256);

  static const u32 kCpuPin =  //
      batt::getenv_as<u32>("LLFS_VOLUME_SIM_CPU").value_or(0);

  static const usize kNumThreads =  //
      batt::getenv_as<u32>("LLFS_VOLUME_SIM_THREADS").value_or(std::thread::hardware_concurrency());

  static const bool kMultiProcess =  //
      batt::getenv_as<bool>("LLFS_VOLUME_SIM_MULTI_PROCESS").value_or(false);

  static const u32 kNumSeedsPerThread = (kNumSeeds + kNumThreads - 1) / kNumThreads;

  std::vector<std::thread> threads;

  if (kMultiProcess) {
    for (usize thread_i = 0; thread_i < kNumThreads; thread_i += 1) {
      std::string command = batt::to_string(                                      //
          "LLFS_VOLUME_SIM_SEED=", kInitialSeed + kNumSeedsPerThread * thread_i,  //
          " LLFS_VOLUME_SIM_COUNT=", kNumSeedsPerThread,                          //
          " LLFS_VOLUME_SIM_CPU=", thread_i,                                      //
          " LLFS_VOLUME_SIM_THREADS=1",                                           //
          " LLFS_VOLUME_SIM_MULTI_PROCESS=0",                                     //
          " GTEST_FILTER=VolumeSimTest.RecoverySimulation",                       //
          " bin/llfs_Test");

      std::cout << command << std::endl;

      threads.emplace_back([command] {
        EXPECT_EQ(0, std::system(command.c_str()));
      });
    }
  } else {
    for (usize thread_i = 0; thread_i < kNumThreads; thread_i += 1) {
      threads.emplace_back([thread_i, this] {
        batt::pin_thread_to_cpu((thread_i + kCpuPin) % std::thread::hardware_concurrency())
            .IgnoreError();
        const u32 first_seed = kInitialSeed + kNumSeedsPerThread * thread_i;
        const u32 last_seed = first_seed + kNumSeedsPerThread;
        LLFS_VLOG(1) << BATT_INSPECT(thread_i) << BATT_INSPECT(first_seed)
                     << BATT_INSPECT(last_seed);
        for (u32 seed = first_seed; seed < last_seed; ++seed) {
          ASSERT_NO_FATAL_FAILURE(this->run_recovery_sim(seed));
        }
      });
    }
  }

  for (auto& t : threads) {
    t.join();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeSimTest, ConcurrentAppendJobs)
{
  for (usize seed = 0; seed < 256; ++seed) {
    std::mt19937 rng{seed};

    llfs::StorageSimulation sim{batt::StateMachineEntropySource{
        /*entropy_fn=*/[&rng](usize min_value, usize max_value) -> usize {
          std::uniform_int_distribution<usize> pick_value{min_value, max_value};
          return pick_value(rng);
        }}};

    // Add three page devices so we can verify that the ref counts are correctly recovered from
    // a subset.
    //
    sim.add_page_arena(this->pages_per_device, llfs::PageSize{1 * kKiB});

    sim.register_page_reader(llfs::PageGraphNodeView::page_layout_id(), __FILE__, __LINE__,
                             llfs::PageGraphNodeView::page_reader());

    const auto main_task_fn = [&] {
      // Create the simulated Volume.
      //
      {
        batt::StatusOr<std::unique_ptr<llfs::Volume>> recovered_volume = sim.get_volume(
            "TestVolume", /*slot_visitor_fn=*/
            [](auto&&...) {
              return batt::OkStatus();
            },
            /*root_log_capacity=*/64 * kKiB);

        ASSERT_TRUE(recovered_volume.ok()) << recovered_volume.status();

        llfs::Volume& volume = **recovered_volume;

        // Test plan:
        //  1. Concurrently, on `this->pages_per_device` different tasks:
        //     - build a page and commit it to the volume
        //  2. Wait for everything to be flushed
        //  3. Verify that the page ref count for each page is 2.

        std::vector<llfs::PageId> page_ids(this->pages_per_device);
        std::vector<std::unique_ptr<batt::Task>> tasks;

        // Create the tasks; one per page.
        //
        for (usize task_i = 0; task_i < this->pages_per_device; ++task_i) {
          tasks.emplace_back(std::make_unique<batt::Task>(
              sim.task_scheduler().schedule_task(),
              [task_i, &page_ids, &sim, &volume] {
                std::unique_ptr<llfs::PageCacheJob> job = volume.new_job();

                //----- --- -- -  -  -   -
                // Build the page and save its id.
                //
                llfs::PageId page_id =
                    BATT_OK_RESULT_OR_PANIC(VolumeSimTest::build_page_with_refs_to(
                        /*refs=*/{}, llfs::PageSize{1 * kKiB}, *job, sim));

                page_ids[task_i] = page_id;

                //----- --- -- -  -  -   -
                // Commit the job to the Volume root log.
                //
                LLFS_VLOG(1) << BATT_INSPECT(task_i) << BATT_INSPECT(page_id)
                             << " starting commit...";

                llfs::SlotRange slot = BATT_OK_RESULT_OR_PANIC(
                    VolumeSimTest::commit_job_to_root_log(std::move(job), page_id, volume, sim));

                LLFS_VLOG(1) << BATT_INSPECT(task_i) << BATT_INSPECT(page_id)
                             << " commit finished!";

                //----- --- -- -  -  -   -
                // Wait for everything to be flushed.
                //
                BATT_CHECK_OK(
                    volume.sync(llfs::LogReadMode::kDurable, llfs::SlotUpperBoundAt{
                                                                 .offset = slot.upper_bound,
                                                             }));
                // Done!
              },
              batt::Task::DeferStart{true},  //
              /*name=*/batt::to_string("TestCommitTask", task_i)));
        }

        for (auto& p_task : tasks) {
          p_task->start();
        }

        for (auto& p_task : tasks) {
          p_task->join();
        }

        // We expect the ref count for each page in the 1kib device to have the default initial
        // ref count (2).  If there are ordering problems between the phases of the jobs, then
        // some of the page ref count (PRC) updates may be dropped (due to user_slot de-duping).
        //
        constexpr i32 kExpectedRefCount = 2;

        for (const llfs::PageArena& arena : sim.cache()->arenas_for_page_size(1 * kKiB)) {
          for (llfs::PageId page_id : page_ids) {
            EXPECT_EQ(arena.allocator().get_ref_count(page_id).first, kExpectedRefCount);
          }
          break;
        }

        volume.halt();
        volume.join();
      }
    };

    sim.run_main_task(main_task_fn);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto VolumeSimTest::RecoverySimState::get_slot_visitor()
{
  return [this](const llfs::SlotParse /*slot*/, const llfs::PageId& page_id) {
    if (page_id == this->first_page_id) {
      this->recovered_first_page = true;
    } else if (page_id == this->second_root_page_id) {
      this->recovered_second_page = true;
    } else {
      this->no_unknown_pages = false;
    }
    return batt::OkStatus();
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeSimTest::RecoverySimState::reset_visitor_outputs()
{
  this->recovered_first_page = false;
  this->recovered_second_page = false;
  this->no_unknown_pages = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeSimTest::run_recovery_sim(u32 seed)
{
  RecoverySimState state;

  state.seed = seed;

  std::mt19937 rng{state.seed};

  llfs::StorageSimulation sim{batt::StateMachineEntropySource{
      /*entropy_fn=*/[&rng](usize min_value, usize max_value) -> usize {
        std::uniform_int_distribution<usize> pick_value{min_value, max_value};
        return pick_value(rng);
      }}};

  // Add three page devices so we can verify that the ref counts are correctly recovered from
  // a subset.
  //
  sim.add_page_arena(this->pages_per_device, llfs::PageSize{1 * kKiB});
  sim.add_page_arena(this->pages_per_device, llfs::PageSize{2 * kKiB});
  sim.add_page_arena(this->pages_per_device, llfs::PageSize{4 * kKiB});

  sim.register_page_reader(llfs::PageGraphNodeView::page_layout_id(), __FILE__, __LINE__,
                           llfs::PageGraphNodeView::page_reader());

  const auto main_task_fn = [&] {
    // Create the simulated Volume.
    //
    {
      batt::StatusOr<std::unique_ptr<llfs::Volume>> recovered_volume = sim.get_volume(
          "TestVolume", /*slot_visitor_fn=*/
          [](auto&&...) {
            return batt::OkStatus();
          },
          /*root_log_capacity=*/64 * kKiB);

      ASSERT_TRUE(recovered_volume.ok()) << recovered_volume.status();

      llfs::Volume& volume = **recovered_volume;

      ASSERT_NO_FATAL_FAILURE(this->commit_first_job(state, sim, volume));

      // Now that the initial job has been committed, allow failures to be injected into the
      // simulation (according to our entropy source).
      //
      sim.set_inject_failures_mode(true);

      // Commit another job with two new pages: one that references first_page_id and is
      // referenced by the other one, which is referenced from the root log.
      //
      state.pre_crash_status = this->commit_second_job_pre_crash(state, sim, volume);

      // Simulate a full crash and recovery.
      //
      sim.crash_and_recover();

      // Terminate the volume.
      //
      volume.halt();
      volume.join();
    }
    EXPECT_TRUE(state.first_page_id.is_valid()) << BATT_INSPECT(state.seed);

    // Recover system state, post-crash.
    //
    sim.set_inject_failures_mode(false);
    {
      // Recover the Volume.
      //
      batt::StatusOr<std::unique_ptr<llfs::Volume>> recovered_volume = sim.get_volume(
          "TestVolume",
          llfs::TypedSlotReader<RecoverySimTestSlot>::make_slot_visitor(state.get_slot_visitor()));

      ASSERT_TRUE(recovered_volume.ok()) << BATT_INSPECT(recovered_volume.status());
      ASSERT_NO_FATAL_FAILURE(
          this->verify_post_recovery_expectations(state, sim, **recovered_volume));
    }
  };

  sim.run_main_task(main_task_fn);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeSimTest::commit_first_job(RecoverySimState& state, llfs::StorageSimulation& sim,
                                     llfs::Volume& volume)
{
  std::unique_ptr<llfs::PageCacheJob> job = volume.new_job();

  ASSERT_NE(job, nullptr);

  batt::StatusOr<llfs::PageId> new_page_id =
      this->build_page_with_refs_to({}, llfs::PageSize{1 * kKiB}, *job, sim);

  ASSERT_TRUE(new_page_id.ok()) << BATT_INSPECT(new_page_id.status());

  // Save the page_id so we can use it later.
  //
  state.first_page_id = *new_page_id;

  // Write the page and slot to the Volume.
  //
  batt::StatusOr<llfs::SlotRange> slot_range =
      this->commit_job_to_root_log(std::move(job), state.first_page_id, volume, sim);

  ASSERT_TRUE(slot_range.ok()) << BATT_INSPECT(slot_range.status());

  sim.log_event("first job successfully appended! slot_range=", *slot_range);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status VolumeSimTest::commit_second_job_pre_crash(RecoverySimState& state,
                                                        llfs::StorageSimulation& sim,
                                                        llfs::Volume& volume)
{
  std::unique_ptr<llfs::PageCacheJob> job = volume.new_job();
  BATT_CHECK_NOT_NULLPTR(job);

  //----- --- -- -  -  -   -
  // Build the 4k page; this will reference the 1k page, and be referenced from the 2k
  // page.
  //
  BATT_ASSIGN_OK_RESULT(
      state.third_page_id,
      this->build_page_with_refs_to({state.first_page_id}, llfs::PageSize{4 * kKiB}, *job, sim));

  //----- --- -- -  -  -   -
  // Build the 2k page; this will be referenced from the log.
  //
  BATT_ASSIGN_OK_RESULT(
      state.second_root_page_id,
      this->build_page_with_refs_to({state.third_page_id}, llfs::PageSize{2 * kKiB}, *job, sim));

  //----- --- -- -  -  -   -

  // Once we start committing the job, we are no longer sure that it _won't_ commit.
  //
  state.second_job_will_not_commit = false;

  // Commit the job.
  //
  BATT_ASSIGN_OK_RESULT(
      llfs::SlotRange slot_range,
      this->commit_job_to_root_log(std::move(job), state.second_root_page_id, volume, sim));

  sim.log_event("second job successfully appended! slot_range=", slot_range);

  // Now that the job has successfully been committed, set expectations accordingly.
  //
  state.second_job_will_commit = true;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeSimTest::verify_post_recovery_expectations(RecoverySimState& state,
                                                      llfs::StorageSimulation& sim,
                                                      llfs::Volume& volume)
{
  for (bool use_captured_log_data : {false, true}) {
    state.reset_visitor_outputs();

    llfs::SlotRange slot_range{0, 0};

    // It's possible that the simulated crash happened before the commit slot was flushed;
    // even if so, the job should still be durable, so the commit slot would have been
    // written during recovery, so we can create a reader and scan for it now.
    //
    if (!state.recovered_first_page || !state.recovered_second_page) {
      auto slot_range_to_read = llfs::SlotRangeSpec{
          .lower_bound = batt::None,
          .upper_bound = batt::None,
      };

      if (use_captured_log_data) {
        batt::StatusOr<llfs::SlotReadLock> read_lock =
            volume.lock_slots(slot_range_to_read, llfs::LogReadMode::kDurable,
                              /*lock_holder=*/"VolumeSimTest::verify_post_recovery_expectations");

        ASSERT_TRUE(read_lock.ok()) << read_lock;

        slot_range = read_lock->slot_range();
        BATT_CHECK(!llfs::slot_less_than(slot_range.upper_bound, slot_range.lower_bound));

        batt::StatusOr<llfs::ConstBuffer> log_bytes = volume.get_root_log_data(*read_lock);

        ASSERT_TRUE(log_bytes.ok())
            << BATT_INSPECT(log_bytes.status()) << BATT_INSPECT(read_lock->slot_range());

        llfs::RawVolumeLogDataParser parser;
        batt::StatusOr<llfs::slot_offset_type> parse_result =
            parser.parse_chunk(read_lock->slot_range(), *log_bytes,
                               llfs::TypedSlotReader<RecoverySimTestSlot>::make_slot_visitor(
                                   state.get_slot_visitor()));

        ASSERT_TRUE(parse_result.ok()) << BATT_INSPECT(parse_result.status());
        EXPECT_EQ(*parse_result, read_lock->slot_range().upper_bound);

      } else {
        {
          batt::StatusOr<llfs::TypedVolumeReader<RecoverySimTestSlot>> volume_reader =
              volume.typed_reader(slot_range_to_read, llfs::LogReadMode::kDurable,
                                  batt::StaticType<RecoverySimTestSlot>{});

          ASSERT_TRUE(volume_reader.ok()) << BATT_INSPECT(volume_reader.status());

          volume_reader
              ->consume_typed_slots(batt::WaitForResource::kFalse, state.get_slot_visitor())
              .IgnoreError();
        }
        slot_range = volume.root_log_slot_range(llfs::LogReadMode::kDurable);
      }
    }
    EXPECT_TRUE(state.recovered_first_page) << BATT_INSPECT(use_captured_log_data)
                                            << BATT_INSPECT(slot_range) << BATT_INSPECT(state.seed);
    EXPECT_TRUE(state.no_unknown_pages);

    if (state.second_job_will_not_commit) {
      EXPECT_FALSE(state.recovered_second_page);
    }
    if (state.second_job_will_commit) {
      EXPECT_TRUE(state.recovered_second_page);
    }
    if (state.recovered_second_page) {
      EXPECT_FALSE(state.second_job_will_not_commit);

      for (const llfs::PageArena& arena : sim.cache()->arenas_for_page_size(1 * kKiB)) {
        EXPECT_EQ(arena.allocator().free_pool_size(), this->pages_per_device - 1);
        EXPECT_EQ(arena.allocator().get_ref_count(state.first_page_id).first, 3);
        ASSERT_TRUE(sim.has_data_for_page_id(state.first_page_id).ok());
        EXPECT_TRUE(*sim.has_data_for_page_id(state.first_page_id));
      }
      for (const llfs::PageArena& arena : sim.cache()->arenas_for_page_size(2 * kKiB)) {
        EXPECT_EQ(arena.allocator().free_pool_size(), this->pages_per_device - 1);
        EXPECT_EQ(arena.allocator().get_ref_count(state.second_root_page_id).first, 2);
        ASSERT_TRUE(sim.has_data_for_page_id(state.second_root_page_id).ok());
        EXPECT_TRUE(*sim.has_data_for_page_id(state.second_root_page_id));
      }
      for (const llfs::PageArena& arena : sim.cache()->arenas_for_page_size(4 * kKiB)) {
        EXPECT_EQ(arena.allocator().free_pool_size(), this->pages_per_device - 1);
        EXPECT_EQ(arena.allocator().get_ref_count(state.third_page_id).first, 2);
        ASSERT_TRUE(sim.has_data_for_page_id(state.third_page_id).ok());
        EXPECT_TRUE(*sim.has_data_for_page_id(state.third_page_id));
      }
    } else {
      for (const llfs::PageArena& arena : sim.cache()->arenas_for_page_size(1 * kKiB)) {
        EXPECT_EQ(arena.allocator().free_pool_size(), this->pages_per_device - 1);
        EXPECT_EQ(arena.allocator().get_ref_count(state.first_page_id).first, 2);
        ASSERT_TRUE(sim.has_data_for_page_id(state.first_page_id).ok());
        EXPECT_TRUE(*sim.has_data_for_page_id(state.first_page_id));
      }
      for (const llfs::PageArena& arena : sim.cache()->arenas_for_page_size(2 * kKiB)) {
        EXPECT_EQ(arena.allocator().free_pool_size(), this->pages_per_device);
        if (state.second_root_page_id.is_valid()) {
          EXPECT_EQ(arena.allocator().get_ref_count(state.second_root_page_id).first, 0);
          if (!llfs::Volume::write_new_pages_asap()) {
            ASSERT_TRUE(sim.has_data_for_page_id(state.second_root_page_id).ok());
            EXPECT_FALSE(*sim.has_data_for_page_id(state.second_root_page_id));
          }
        }
      }
      for (const llfs::PageArena& arena : sim.cache()->arenas_for_page_size(4 * kKiB)) {
        EXPECT_EQ(arena.allocator().free_pool_size(), this->pages_per_device);
        if (state.third_page_id.is_valid()) {
          EXPECT_EQ(arena.allocator().get_ref_count(state.third_page_id).first, 0);
          if (!llfs::Volume::write_new_pages_asap()) {
            ASSERT_TRUE(sim.has_data_for_page_id(state.third_page_id).ok());
            EXPECT_FALSE(*sim.has_data_for_page_id(state.third_page_id));
          }
        }
      }
    }
    if (!state.second_job_will_not_commit) {
      EXPECT_TRUE(state.second_root_page_id.is_valid());
      EXPECT_TRUE(state.third_page_id.is_valid());
    }
    EXPECT_EQ(state.second_root_page_id.is_valid(), state.third_page_id.is_valid());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<llfs::PageId> VolumeSimTest::build_page_with_refs_to(
    const std::vector<llfs::PageId>& referenced_page_ids, llfs::PageSize page_size,
    llfs::PageCacheJob& job, llfs::StorageSimulation& /*sim*/)
{
  batt::StatusOr<llfs::PageGraphNodeBuilder> page_builder =
      llfs::PageGraphNodeBuilder::from_new_page(job.new_page(
          page_size, batt::WaitForResource::kFalse, llfs::PageGraphNodeView::page_layout_id(),
          /*callers=*/0, /*cancel_token=*/llfs::None));

  BATT_REQUIRE_OK(page_builder);

  for (llfs::PageId page_id : referenced_page_ids) {
    page_builder->add_page(page_id);
  }

  batt::StatusOr<llfs::PinnedPage> pinned_page = std::move(*page_builder).build(job);
  BATT_REQUIRE_OK(pinned_page);

  return pinned_page->page_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<llfs::SlotRange> VolumeSimTest::commit_job_to_root_log(
    std::unique_ptr<llfs::PageCacheJob> job, llfs::PageId root_page_id, llfs::Volume& volume,
    llfs::StorageSimulation& sim)
{
  // Wrap the page id in an event variant.
  //
  auto slot_data =
      llfs::pack_as_variant<RecoverySimTestSlot>(llfs::PackedPageId::from(root_page_id));

  // Make an appendable job for the slot event and page job.
  //
  BATT_ASSIGN_OK_RESULT(llfs::AppendableJob appendable_job,
                        llfs::make_appendable_job(std::move(job), llfs::PackableRef{slot_data}));

  // Reserve space in the Volume.
  //
  BATT_ASSIGN_OK_RESULT(
      batt::Grant slot_grant,
      volume.reserve(volume.calculate_grant_size(appendable_job), batt::WaitForResource::kFalse));

  // Append the job!
  //
  sim.log_event("appending job with root_page_id=", root_page_id, "...");

  return volume.append(std::move(appendable_job), slot_grant);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeTest, AppendableJobGrantSize)
{
  auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
  auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

  std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
      fake_root_log, fake_recycler_log,
      /*slot_visitor_fn=*/[](const llfs::SlotParse&, const std::string_view& /*user_data*/) {
        return llfs::OkStatus();
      });

  ASSERT_NE(test_volume, nullptr);

  std::unique_ptr<llfs::PageCacheJob> job = test_volume->new_job();

  ASSERT_NE(job, nullptr);

  auto user_data_null_page = batt::seq::single_item(llfs::PageId{})  //
                             | batt::seq::boxed();

  llfs::StatusOr<llfs::AppendableJob> appendable_job =
      llfs::make_appendable_job(std::move(job), llfs::PackableRef{user_data_null_page});

  ASSERT_TRUE(appendable_job.ok()) << BATT_INSPECT(appendable_job.status());

  const usize size_from_job = appendable_job->calculate_grant_size();

  const usize size_from_volume = test_volume->calculate_grant_size(*appendable_job);

  EXPECT_EQ(size_from_job, size_from_volume);

  llfs::PrepareJob prepare_job = llfs::prepare(*appendable_job);
  const usize prepare_slot_size = llfs::packed_sizeof_slot(prepare_job);
  const usize commit_size_from_prepare = llfs::packed_sizeof_commit(prepare_job);

  std::vector<char> prepare_job_storage(llfs::packed_sizeof(prepare_job) * 2);

  llfs::PackedPrepareJob* packed_prepare_job = [&] {
    batt::MutableBuffer prepare_job_buffer{prepare_job_storage.data(), prepare_job_storage.size()};
    llfs::DataPacker packer{prepare_job_buffer};
    llfs::PackedPrepareJob* result = llfs::pack_object(prepare_job, &packer);

    EXPECT_EQ(packer.space(), llfs::packed_sizeof(prepare_job));

    return result;
  }();

  ASSERT_NE(packed_prepare_job, nullptr);
  EXPECT_EQ(llfs::packed_sizeof_slot(*packed_prepare_job), prepare_slot_size);

  llfs::CommitJob commit_job{
      .prepare_slot_offset = 0,
      .packed_prepare = packed_prepare_job,
  };

  EXPECT_EQ(commit_size_from_prepare, llfs::packed_sizeof(commit_job));

  const usize commit_slot_size = llfs::packed_sizeof_slot(commit_job);
  const usize size_from_events = prepare_slot_size + commit_slot_size;

  EXPECT_EQ(size_from_events, size_from_job);

  std::vector<char> commit_job_storage(llfs::packed_sizeof(commit_job) * 2);

  llfs::PackedCommitJob* packed_commit_job = [&] {
    batt::MutableBuffer commit_job_buffer{commit_job_storage.data(), commit_job_storage.size()};
    llfs::DataPacker packer{commit_job_buffer};
    llfs::PackedCommitJob* result = llfs::pack_object(commit_job, &packer);

    EXPECT_EQ(packer.space(), llfs::packed_sizeof(commit_job));

    return result;
  }();

  ASSERT_NE(packed_commit_job, nullptr);
  EXPECT_EQ(llfs::packed_sizeof_slot(*packed_commit_job), commit_slot_size);
}

}  // namespace
