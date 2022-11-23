//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_events.hpp>
//
#include <llfs/volume_events.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/testing/packed_type_test_fixture.hpp>

namespace {

using namespace llfs::int_types;

class VolumeEventsTest : public llfs::testing::PackedTypeTestFixture
{
 public:
  llfs::TrimmedPrepareJob make_trimmed_prepare_job(llfs::slot_offset_type prepare_slot,
                                                   usize n_pages)
  {
    llfs::TrimmedPrepareJob object;
    object.prepare_slot = prepare_slot;
    object.page_ids = batt::as_seq(this->page_ids) | batt::seq::take_n(n_pages) |
                      batt::seq::decayed() | batt::seq::boxed();

    return object;
  }

  std::vector<llfs::PageId> page_ids{
      llfs::PageId{1}, llfs::PageId{2},  llfs::PageId{3},  llfs::PageId{4},
      llfs::PageId{5}, llfs::PageId{6},  llfs::PageId{7},  llfs::PageId{8},
      llfs::PageId{9}, llfs::PageId{10}, llfs::PageId{11}, llfs::PageId{12},
  };
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Test plan:
//  - do all of:
//    1. serialize using pack_object
//    2. unpack via unpack_object
//       1. success
//       2. failure - buffer truncated
//    3. unpack via unpack_cast
//  - for each of:
//    a. page_ids empty
//    b. 1 page_ids
//    c. >1 page_ids
//
TEST_F(VolumeEventsTest, TrimmedPrepareJobPackUnpackNoPages)
{
  llfs::TrimmedPrepareJob object;
  object.prepare_slot = 1996;
  object.page_ids = batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed();

  ASSERT_NE(this->pack_into_buffer(object), nullptr);

  {
    batt::StatusOr<const llfs::PackedTrimmedPrepareJob&> packed =
        llfs::unpack_cast<llfs::PackedTrimmedPrepareJob>(this->const_buffer());

    ASSERT_TRUE(packed.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(packed->prepare_slot, object.prepare_slot);
    EXPECT_EQ(packed->page_ids.size(), 0u);

    batt::StatusOr<llfs::TrimmedPrepareJob> unpacked = this->unpack_from_buffer(*packed);

    ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(unpacked->prepare_slot, object.prepare_slot);
    EXPECT_EQ((batt::make_copy(unpacked->page_ids) | batt::seq::collect_vec()),
              (batt::make_copy(object.page_ids) | batt::seq::collect_vec()));
  }
  {
    batt::StatusOr<const llfs::PackedTrimmedPrepareJob&> packed =
        llfs::unpack_cast<llfs::PackedTrimmedPrepareJob>(this->const_buffer(1));

    ASSERT_FALSE(packed.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(packed.status(), llfs::StatusCode::kUnpackCastStructOver);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeEventsTest, TrimmedPrepareJobPackUnpackSinglePage)
{
  llfs::TrimmedPrepareJob object;
  object.prepare_slot = 2001;
  object.page_ids = batt::seq::single_item(llfs::PageId{0x2468}) | batt::seq::boxed();

  ASSERT_NE(this->pack_into_buffer(object), nullptr);

  {
    batt::StatusOr<const llfs::PackedTrimmedPrepareJob&> packed =
        llfs::unpack_cast<llfs::PackedTrimmedPrepareJob>(this->const_buffer());

    ASSERT_TRUE(packed.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(packed->prepare_slot, object.prepare_slot);
    EXPECT_EQ(packed->page_ids.size(), 1u);

    batt::StatusOr<llfs::TrimmedPrepareJob> unpacked = this->unpack_from_buffer(*packed);

    ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(unpacked->prepare_slot, object.prepare_slot);
    EXPECT_EQ((batt::make_copy(unpacked->page_ids) | batt::seq::collect_vec()),
              (batt::make_copy(object.page_ids) | batt::seq::collect_vec()));
  }
  {
    batt::StatusOr<const llfs::PackedTrimmedPrepareJob&> packed =
        llfs::unpack_cast<llfs::PackedTrimmedPrepareJob>(this->const_buffer(1));

    ASSERT_FALSE(packed.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(packed.status(), llfs::StatusCode::kUnpackCastStructOver);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeEventsTest, TrimmedPrepareJobPackUnpackMultiPage)
{
  llfs::TrimmedPrepareJob object = this->make_trimmed_prepare_job(40000, 12);

  ASSERT_NE(this->pack_into_buffer(object), nullptr);

  {
    batt::StatusOr<const llfs::PackedTrimmedPrepareJob&> packed =
        llfs::unpack_cast<llfs::PackedTrimmedPrepareJob>(this->const_buffer());

    ASSERT_TRUE(packed.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(packed->prepare_slot, object.prepare_slot);
    EXPECT_EQ(packed->page_ids.size(), 12u);

    batt::StatusOr<llfs::TrimmedPrepareJob> unpacked = this->unpack_from_buffer(*packed);

    ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(unpacked.status());
    EXPECT_EQ(unpacked->prepare_slot, object.prepare_slot);
    EXPECT_EQ((batt::make_copy(unpacked->page_ids) | batt::seq::collect_vec()),
              (batt::make_copy(object.page_ids) | batt::seq::collect_vec()));
  }
  {
    batt::StatusOr<const llfs::PackedTrimmedPrepareJob&> packed =
        llfs::unpack_cast<llfs::PackedTrimmedPrepareJob>(this->const_buffer(1));

    ASSERT_FALSE(packed.ok()) << BATT_INSPECT(packed.status());
    EXPECT_EQ(packed.status(), llfs::StatusCode::kUnpackCastStructOver);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(VolumeEventsTest, TrimEventPackUnpack)
{
  usize prev_packed_size = 0;

  for (usize n_trimmed_jobs = 0; n_trimmed_jobs < 10; ++n_trimmed_jobs) {
    std::vector<llfs::TrimmedPrepareJob> jobs;
    for (usize i = 0; i < n_trimmed_jobs; ++i) {
      jobs.emplace_back(this->make_trimmed_prepare_job(/*prepare_slot=*/i, /*n_pages=*/i));
    }

    std::vector<llfs::slot_offset_type> commits;
    for (usize i = 1; i < n_trimmed_jobs; ++i) {
      commits.emplace_back(/*prepare_slot=*/i - 1);
    }

    llfs::VolumeTrimEvent trim_event;
    trim_event.old_trim_pos = n_trimmed_jobs;
    trim_event.new_trim_pos = (n_trimmed_jobs + 1) * 1917;

    trim_event.committed_jobs =  //
        batt::as_seq(commits)    //
        | batt::seq::decayed()   //
        | batt::seq::boxed();

    trim_event.trimmed_prepare_jobs =  //
        batt::as_seq(jobs)             //
        | batt::seq::decayed()         //
        | batt::seq::boxed();

    EXPECT_GT(llfs::packed_sizeof(trim_event), prev_packed_size);
    prev_packed_size = llfs::packed_sizeof(trim_event);

    ASSERT_NE(this->pack_into_buffer(trim_event), nullptr);

    {
      batt::StatusOr<const llfs::PackedVolumeTrimEvent&> packed =
          llfs::unpack_cast<llfs::PackedVolumeTrimEvent>(this->const_buffer());

      ASSERT_TRUE(packed.ok()) << BATT_INSPECT(packed.status());
      EXPECT_EQ(packed->old_trim_pos, trim_event.old_trim_pos);
      EXPECT_EQ(packed->new_trim_pos, trim_event.new_trim_pos);
      if (n_trimmed_jobs > 1) {
        ASSERT_TRUE(packed->committed_jobs);
        ASSERT_EQ(packed->committed_jobs->size(), n_trimmed_jobs - 1);
      } else {
        EXPECT_FALSE(packed->committed_jobs);
      }
      ASSERT_EQ(packed->trimmed_prepare_jobs.size(), n_trimmed_jobs);

      for (usize i = 0; i < n_trimmed_jobs; ++i) {
        const auto& job = packed->trimmed_prepare_jobs[i];

        ASSERT_TRUE(job);
        EXPECT_EQ(job->prepare_slot, i);
        EXPECT_EQ(job->page_ids.size(), i);

        for (usize j = 0; j < i; ++j) {
          EXPECT_EQ(this->page_ids[j], job->page_ids[j].as_page_id())
              << BATT_INSPECT(i) << BATT_INSPECT(j);
        }
      }

      batt::StatusOr<llfs::VolumeTrimEvent> unpacked = this->unpack_from_buffer(*packed);

      ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(unpacked.status());
      EXPECT_EQ(unpacked->old_trim_pos, trim_event.old_trim_pos);
      EXPECT_EQ(unpacked->new_trim_pos, trim_event.new_trim_pos);
      EXPECT_EQ((batt::make_copy(unpacked->committed_jobs) |
                 batt::seq::map([](llfs::PackedSlotOffset offset) -> llfs::slot_offset_type {
                   return offset.value();
                 }) |
                 batt::seq::collect_vec()),
                commits);
      EXPECT_EQ((batt::make_copy(unpacked->trimmed_prepare_jobs) | batt::seq::collect_vec()).size(),
                n_trimmed_jobs);
    }

    {
      batt::StatusOr<const llfs::PackedVolumeTrimEvent&> packed =
          llfs::unpack_cast<llfs::PackedVolumeTrimEvent>(this->const_buffer(1));

      EXPECT_EQ(packed.status(), llfs::StatusCode::kUnpackCastStructOver);
    }
  }
}

}  // namespace
