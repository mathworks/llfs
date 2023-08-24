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

#include <boost/range/irange.hpp>

namespace {

using namespace llfs::int_types;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Fake user data, for testing only.
 */
struct FakeUserData {
  std::string str;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const FakeUserData& obj)
{
  return obj.str.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PackedRawData* pack_object(const FakeUserData& obj, llfs::DataPacker* dst)
{
  llfs::Optional<std::string_view> packed = dst->pack_raw_data(obj.str.data(), obj.str.size());
  if (!packed) {
    return nullptr;
  }
  return (llfs::PackedRawData*)packed->data();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::BoxedSeq<llfs::PageId> trace_refs(const FakeUserData& obj)
{
  const llfs::PackedPageId* packed_ids = (const llfs::PackedPageId*)obj.str.data();
  usize count = obj.str.size() / sizeof(llfs::PackedPageId);

  return batt::as_seq(boost::irange(usize{0}, count))  //
         | batt::seq::map([packed_ids](usize i) -> llfs::PageId {
             return packed_ids[i].unpack();
           })  //
         | batt::seq::boxed();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeJobEventsTest : public ::testing::Test
{
 public:
  template <typename T>
  void pack_test_object(const T& obj, std::vector<char>& storage)
  {
    usize size = packed_sizeof(obj);

    storage.clear();
    storage.resize(size);

    llfs::MutableBuffer buffer{storage.data(), storage.size()};
    llfs::DataPacker packer{buffer};

    ASSERT_TRUE(pack_object(obj, &packer));
    EXPECT_EQ(packer.space(), 0u)
        << "packed_sizeof did not report the actual size used by pack_object!";
  }

  void init_user_data(usize user_data_size) noexcept
  {
    this->user_data_.str.clear();
    this->root_refs_.clear();

    if (user_data_size < sizeof(llfs::PackedPageId)) {
      this->user_data_.str = std::string(user_data_size, 'a');

    } else {
      this->root_refs_.resize(user_data_size / sizeof(llfs::PackedPageId));
      for (usize i = 0; i < this->root_refs_.size(); ++i) {
        this->root_refs_[i] = llfs::PackedPageId{
            .id_val = i,
        };
      }
      this->user_data_.str = std::string_view{(const char*)this->root_refs_.data(),
                                              this->root_refs_.size() * sizeof(llfs::PackedPageId)};
    }
    EXPECT_EQ(this->user_data_.str.size(), user_data_size);
  }

  void init_new_page_ids(usize new_page_count) noexcept
  {
    this->new_page_ids_.clear();
    for (usize i = 0; i < new_page_count; ++i) {
      this->new_page_ids_.emplace_back(llfs::PageId{i});
    }
  }

  void init_deleted_page_ids(usize deleted_page_count) noexcept
  {
    this->deleted_page_ids_.clear();
    for (usize i = 0; i < deleted_page_count; ++i) {
      this->deleted_page_ids_.emplace_back(llfs::PageId{i + 1000});
    }
  }

  void init_page_device_ids(usize page_device_count) noexcept
  {
    this->page_device_ids_.clear();
    for (usize i = 0; i < page_device_count; ++i) {
      this->page_device_ids_.emplace_back(i);
    }
  }

  void pack_prepare_job(std::vector<char>& storage) noexcept
  {
    this->pack_test_object(
        llfs::PrepareJob{
            .new_page_ids = (batt::as_seq(this->new_page_ids_)  //
                             | batt::seq::decayed()             //
                             | batt::seq::boxed()),

            .deleted_page_ids = (batt::as_seq(this->deleted_page_ids_) |  //
                                 batt::seq::decayed() |                   //
                                 batt::seq::boxed()),

            .page_device_ids = (batt::as_seq(this->page_device_ids_) |  //
                                batt::seq::decayed() |                  //
                                batt::seq::boxed()),

            .user_data = llfs::PackableRef{this->user_data_},
        },
        storage);
  }

  void verify_page_ids(const llfs::PackedArray<llfs::PackedPageId>& actual_ids,
                       const std::vector<llfs::PageId>& expected_ids, const char* file, int line)
  {
    EXPECT_EQ((as_seq(actual_ids)  //
               | batt::seq::map([](const llfs::PackedPageId& id) {
                   return id.unpack();
                 })  //
               | batt::seq::collect_vec()),
              expected_ids)
        << BATT_INSPECT(file) << BATT_INSPECT(line);
  }

  void verify_page_ids(const llfs::PackedArray<llfs::PackedPageId>& actual_ids,
                       const std::vector<llfs::PackedPageId>& expected_ids, const char* file,
                       int line)
  {
    EXPECT_EQ((as_seq(actual_ids)      //
               | batt::seq::decayed()  //
               | batt::seq::collect_vec()),
              expected_ids)
        << BATT_INSPECT(file) << BATT_INSPECT(line);
  }

  void verify_prepare_job(const llfs::PackedPrepareJob& unpacked) noexcept
  {
    ASSERT_TRUE(unpacked.root_page_ids);
    ASSERT_TRUE(unpacked.new_page_ids);
    ASSERT_TRUE(unpacked.deleted_page_ids);
    ASSERT_TRUE(unpacked.page_device_ids);

    EXPECT_THAT(unpacked.user_data(), ::testing::StrEq(this->user_data_.str));

    this->verify_page_ids(  //
        *unpacked.root_page_ids, this->root_refs_, __FILE__, __LINE__);

    this->verify_page_ids(  //
        *unpacked.new_page_ids, this->new_page_ids_, __FILE__, __LINE__);

    this->verify_page_ids(  //
        *unpacked.deleted_page_ids, this->deleted_page_ids_, __FILE__, __LINE__);

    EXPECT_EQ((as_seq(*unpacked.page_device_ids)  //
               | batt::seq::map([](const auto& id) {
                   return id.value();
                 })  //
               | batt::seq::collect_vec()),
              this->page_device_ids_);
  }

  void verify_commit_job(const llfs::PackedCommitJob& unpacked) noexcept
  {
    ASSERT_TRUE(unpacked.root_page_ids);

    EXPECT_THAT(unpacked.user_data(), ::testing::StrEq(this->user_data_.str));

    this->verify_page_ids(  //
        *unpacked.root_page_ids, this->root_refs_, __FILE__, __LINE__);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  FakeUserData user_data_;
  std::vector<llfs::PackedPageId> root_refs_;
  std::vector<llfs::PageId> new_page_ids_;
  std::vector<llfs::PageId> deleted_page_ids_;
  std::vector<llfs::page_device_id_int> page_device_ids_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// PrepareJob Test Plan
// --------------------
//  1. Minimal event, pack/unpack
//     - zero size user data
//     - no root page ids, new page ids, deleted page ids, or page device ids
//  2. Test all combinations of zero size, size=1, and size > 1 for each field.
//
TEST_F(VolumeJobEventsTest, PrepareJobTest)
{
  for (usize user_data_size : {usize{0}, usize{1}, usize{sizeof(llfs::PackedPageId) * 1},
                               usize{sizeof(llfs::PackedPageId) * 17}}) {
    for (usize new_page_count : {0, 1, 101}) {
      for (usize deleted_page_count : {0, 1, 102}) {
        for (usize page_device_count : {0, 1, 7}) {
          std::vector<char> storage;

          this->init_user_data(user_data_size);
          this->init_new_page_ids(new_page_count);
          this->init_deleted_page_ids(deleted_page_count);
          this->init_page_device_ids(page_device_count);

          ASSERT_NO_FATAL_FAILURE(this->pack_prepare_job(storage));

          llfs::StatusOr<const llfs::PackedPrepareJob&> unpacked =
              llfs::unpack_cast(storage, batt::StaticType<llfs::PackedPrepareJob>{});

          ASSERT_TRUE(unpacked.ok()) << BATT_INSPECT(unpacked.status());
          EXPECT_EQ(llfs::packed_sizeof(*unpacked), storage.size());

          llfs::StatusOr<llfs::Ref<const llfs::PackedPrepareJob>> legacy_unpacked =
              llfs::unpack_object(*unpacked, nullptr);

          ASSERT_TRUE(legacy_unpacked.ok()) << BATT_INSPECT(legacy_unpacked.status());
          EXPECT_EQ(legacy_unpacked->pointer(), std::addressof(*unpacked));

          this->verify_prepare_job(*unpacked);
        }
      }
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// CommitJob Test Plan
// --------------------
//  Repeat the PrepareJob cases, and follow up with a CommitJob; verify user_data and root_page_ids.
//
TEST_F(VolumeJobEventsTest, CommitJobTest)
{
  for (usize user_data_size : {usize{0}, usize{1}, usize{sizeof(llfs::PackedPageId) * 1},
                               usize{sizeof(llfs::PackedPageId) * 17}}) {
    for (usize new_page_count : {0, 1, 101}) {
      for (usize deleted_page_count : {0, 1, 102}) {
        for (usize page_device_count : {0, 1, 7}) {
          std::vector<char> prepare_storage;

          llfs::slot_offset_type fake_prepare_slot = 0xf00dabcde8765;

          this->init_user_data(user_data_size);
          this->init_new_page_ids(new_page_count);
          this->init_deleted_page_ids(deleted_page_count);
          this->init_page_device_ids(page_device_count);

          ASSERT_NO_FATAL_FAILURE(this->pack_prepare_job(prepare_storage));

          llfs::StatusOr<const llfs::PackedPrepareJob&> unpacked_prepare =
              llfs::unpack_cast(prepare_storage, batt::StaticType<llfs::PackedPrepareJob>{});

          ASSERT_TRUE(unpacked_prepare.ok()) << BATT_INSPECT(unpacked_prepare.status());

          this->verify_prepare_job(*unpacked_prepare);

          std::vector<char> commit_storage;

          ASSERT_NO_FATAL_FAILURE(this->pack_test_object(
              llfs::CommitJob{
                  .prepare_slot = fake_prepare_slot,
                  .prepare_job = std::addressof(*unpacked_prepare),
              },
              commit_storage));

          llfs::StatusOr<const llfs::PackedCommitJob&> unpacked_commit =
              llfs::unpack_cast(commit_storage, batt::StaticType<llfs::PackedCommitJob>{});

          ASSERT_TRUE(unpacked_commit.ok()) << BATT_INSPECT(unpacked_commit.status());
          EXPECT_EQ(llfs::packed_sizeof(*unpacked_commit), commit_storage.size());
          EXPECT_EQ(unpacked_commit->prepare_slot, fake_prepare_slot);
          EXPECT_EQ(unpacked_commit->prepare_slot_size,
                    llfs::packed_sizeof_slot_with_payload_size(prepare_storage.size()));

          llfs::StatusOr<llfs::Ref<const llfs::PackedCommitJob>> legacy_unpacked =
              llfs::unpack_object(*unpacked_commit, nullptr);

          ASSERT_TRUE(legacy_unpacked.ok()) << BATT_INSPECT(legacy_unpacked.status());
          EXPECT_EQ(legacy_unpacked->pointer(), std::addressof(*unpacked_commit));

          this->verify_commit_job(*unpacked_commit);

          // Simulate a trim of the prepare slot and re-verify the commit (which should be
          // independent).
          //
          std::memset(prepare_storage.data(), 0xab, prepare_storage.size());
          prepare_storage.clear();

          this->verify_commit_job(*unpacked_commit);
        }
      }
    }
  }
}

}  // namespace
