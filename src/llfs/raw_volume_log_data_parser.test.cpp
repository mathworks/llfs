//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/raw_volume_log_data_parser.hpp>
//
#include <llfs/raw_volume_log_data_parser.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/testing/mock_slot_visitor_fn.hpp>

#include <llfs/uuid.hpp>
#include <llfs/volume_events.hpp>

namespace {

using namespace llfs::int_types;

using llfs::MockSlotVisitorFn;

// Test Plan:
//
//  1. Default construct, test get_visited_upper_bound
//  2. Parse an empty log, expect nothing
//  3. Pack some volume events to a buffer, parse:
//     a. the whole buffer
//     b. the first half of events
//     c. upper bound is in the middle of an event, splits a prepare/commit pair
//

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class RawVolumeLogDataParserTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->reset_parser();
  }

  void reset_parser()
  {
    this->parser.emplace();
  }

  // Packs the event as a VolumeEventVariant slot at the end of `this->log_buffer`.
  //
  template <typename T>
  void append_event(T&& event)
  {
    const usize slot_body_size = sizeof(llfs::VolumeEventVariant) + packed_sizeof(event);
    const usize slot_size = llfs::packed_sizeof_slot(event);

    this->log_buffer.resize(this->log_buffer.size() + slot_size);

    char* const slot_end = this->log_buffer.data() + this->log_buffer.size();
    char* const slot_begin = slot_end - slot_size;

    BATT_CHECK_GE(slot_begin, this->log_buffer.data());

    {
      llfs::MutableBuffer slot_buffer{slot_begin, slot_size};
      llfs::DataPacker packer{slot_buffer};

      BATT_CHECK_NOT_NULLPTR(packer.pack_varint(slot_body_size));
      BATT_CHECK_NOT_NULLPTR(llfs::pack_object(
          llfs::pack_as_variant<llfs::VolumeEventVariant>(BATT_FORWARD(event)), &packer));
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::Optional<llfs::RawVolumeLogDataParser> parser;

  ::testing::StrictMock<MockSlotVisitorFn> mock_visitor;

  std::vector<char> log_buffer;

  llfs::PackedVolumeIds volume_ids{
      .main_uuid = llfs::random_uuid(),
      .recycler_uuid = llfs::random_uuid(),
      .trimmer_uuid = llfs::random_uuid(),
  };

  const std::string_view user_event_1 = "user event 1";
  const std::string_view user_event_2 = "user event 2";
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(RawVolumeLogDataParserTest, Test)
{
  EXPECT_EQ(this->parser->get_visited_upper_bound(), batt::None);

  // Parse empty log
  {
    const llfs::ConstBuffer buffer;

    EXPECT_EQ(buffer.size(), 0u);

    batt::StatusOr<llfs::slot_offset_type> result =
        this->parser->parse_chunk(llfs::SlotRange{0, 0}, buffer, std::ref(this->mock_visitor));

    ASSERT_TRUE(result.ok());
    EXPECT_EQ(*result, 0u);
  }

  // Pack some volume events to a buffer
  {
    const usize first_slot_begin = this->log_buffer.size();

    this->append_event(this->volume_ids);

    EXPECT_GT(this->log_buffer.size(), first_slot_begin);

    const usize first_slot_end = this->log_buffer.size();

    //----- --- -- -  -  -   -
    // Since we only have an internal Volume event in the log, we do _not_ expect the visitor to be
    // invoked, so no need for EXPECT_CALL(this->mock_visitor, ...) here.
    //----- --- -- -  -  -   -
    {
      batt::StatusOr<llfs::slot_offset_type> result = this->parser->parse_chunk(
          llfs::SlotRange{0, this->log_buffer.size()},
          llfs::ConstBuffer{this->log_buffer.data(), this->log_buffer.size()},
          std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), first_slot_end);

      this->reset_parser();
    }

    // Now pack a user event; the parser should invoke the visitor with this event.
    //
    const usize second_slot_begin = this->log_buffer.size();

    this->append_event(llfs::pack_as_raw(user_event_1));

    EXPECT_GT(this->log_buffer.size(), second_slot_begin);
    EXPECT_EQ(first_slot_end, second_slot_begin);

    const usize second_slot_end = this->log_buffer.size();

    const std::string user_event_1_packed_variant = batt::to_string(
        llfs::index_of_type_within_packed_variant<llfs::VolumeEventVariant, llfs::PackedRawData>(),
        this->user_event_1);

    const llfs::SlotParse user_event_1_slot{
        .offset =
            llfs::SlotRange{
                .lower_bound = second_slot_begin,
                .upper_bound = second_slot_end,
            },
        .body = user_event_1_packed_variant,
        .depends_on_offset = batt::None,
        .total_grant_spent = second_slot_end - second_slot_begin,
    };

    {
      EXPECT_CALL(this->mock_visitor, visit_slot(user_event_1_slot, this->user_event_1))
          .WillOnce(::testing::Return(batt::OkStatus()));

      batt::StatusOr<llfs::slot_offset_type> result = this->parser->parse_chunk(
          llfs::SlotRange{0, this->log_buffer.size()},
          llfs::ConstBuffer{this->log_buffer.data(), this->log_buffer.size()},
          std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), second_slot_end);

      this->reset_parser();
    }

    // The third slot will be a prepare/commit pair.  First we test that the user data isn't passed
    // to the visitor after just the prepare.
    //
    const llfs::PackAsRawData third_slot_user_data = llfs::pack_as_raw(this->user_event_2);

    const usize third_slot_begin = this->log_buffer.size();

    EXPECT_EQ(third_slot_begin, second_slot_end);

    this->append_event(llfs::PrepareJob{
        .new_page_ids = batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed(),
        .deleted_page_ids = batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed(),
        .page_device_ids = batt::seq::Empty<llfs::page_device_id_int>{} | batt::seq::boxed(),
        .user_data = llfs::PackableRef{third_slot_user_data},
    });

    const usize third_slot_end = this->log_buffer.size();

    EXPECT_GT(third_slot_end, third_slot_begin);
    {
      EXPECT_CALL(this->mock_visitor, visit_slot(user_event_1_slot, this->user_event_1))
          .WillOnce(::testing::Return(batt::OkStatus()));

      batt::StatusOr<llfs::slot_offset_type> result = this->parser->parse_chunk(
          llfs::SlotRange{0, this->log_buffer.size()},
          llfs::ConstBuffer{this->log_buffer.data(), this->log_buffer.size()},
          std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), third_slot_end);

      this->reset_parser();
    }

    // Now we add the commit event; this time when we use the parser, it should hand us both events.
    //
    const usize fourth_slot_begin = this->log_buffer.size();

    EXPECT_EQ(fourth_slot_begin, third_slot_end);

    llfs::PackedCommitJob packed_commit_job_event{
        .reserved_ = {0},
        .prepare_slot = third_slot_begin,
    };

    const std::string user_event_2_packed_variant = batt::to_string(
        llfs::index_of_type_within_packed_variant<llfs::VolumeEventVariant,
                                                  llfs::PackedCommitJob>(),
        std::string_view{(const char*)&packed_commit_job_event, sizeof(llfs::PackedCommitJob)});

    this->append_event(packed_commit_job_event);

    const usize fourth_slot_end = this->log_buffer.size();

    EXPECT_GT(fourth_slot_end, fourth_slot_begin);

    const llfs::SlotParse user_event_2_slot{
        .offset =
            llfs::SlotRange{
                .lower_bound = fourth_slot_begin,
                .upper_bound = fourth_slot_end,
            },
        .body = user_event_2_packed_variant,
        .depends_on_offset =
            llfs::SlotRange{
                .lower_bound = third_slot_begin,
                .upper_bound = third_slot_end,
            },
        .total_grant_spent = ((third_slot_end - third_slot_begin)  //
                              + (fourth_slot_end - fourth_slot_begin)) *
                             2,
    };

    {
      ::testing::Expectation first_slot_visited =
          EXPECT_CALL(this->mock_visitor, visit_slot(user_event_1_slot, this->user_event_1))
              .WillOnce(::testing::Return(batt::OkStatus()));

      EXPECT_CALL(this->mock_visitor, visit_slot(user_event_2_slot, this->user_event_2))
          .After(first_slot_visited)
          .WillOnce(::testing::Return(batt::OkStatus()));

      batt::StatusOr<llfs::slot_offset_type> result = this->parser->parse_chunk(
          llfs::SlotRange{0, this->log_buffer.size()},
          llfs::ConstBuffer{this->log_buffer.data(), this->log_buffer.size()},
          std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), fourth_slot_end);

      this->reset_parser();
    }

    // Simulate out-of-data.
    {
      EXPECT_CALL(this->mock_visitor, visit_slot(user_event_1_slot, this->user_event_1))
          .WillOnce(::testing::Return(batt::OkStatus()));

      batt::StatusOr<llfs::slot_offset_type> result = this->parser->parse_chunk(
          llfs::SlotRange{0, this->log_buffer.size() - 1},
          llfs::ConstBuffer{this->log_buffer.data(), this->log_buffer.size() - 1},
          std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), third_slot_end);

      this->reset_parser();
    }

    // Parse just slots 3 & 4, minus one byte at the end.
    {
      batt::StatusOr<llfs::slot_offset_type> result =
          this->parser->parse_chunk(llfs::SlotRange{third_slot_begin, fourth_slot_end - 1},
                                    llfs::ConstBuffer{this->log_buffer.data() + third_slot_begin,
                                                      fourth_slot_end - third_slot_begin - 1},
                                    std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), third_slot_end);
    }

    // No call to reset_parser; try again, this time with just slot 4 minus one byte
    {
      batt::StatusOr<llfs::slot_offset_type> result =
          this->parser->parse_chunk(llfs::SlotRange{fourth_slot_begin, fourth_slot_end - 1},
                                    llfs::ConstBuffer{this->log_buffer.data() + fourth_slot_begin,
                                                      fourth_slot_end - fourth_slot_begin - 1},
                                    std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), fourth_slot_begin);
    }

    // Now parse all of slot 4 and verify that the user slot is resolved (this verifies that the
    // parser is remembering past prepare job slots).
    {
      EXPECT_CALL(this->mock_visitor, visit_slot(user_event_2_slot, this->user_event_2))
          .WillOnce(::testing::Return(batt::OkStatus()));

      batt::StatusOr<llfs::slot_offset_type> result =
          this->parser->parse_chunk(llfs::SlotRange{fourth_slot_begin, fourth_slot_end},
                                    llfs::ConstBuffer{this->log_buffer.data() + fourth_slot_begin,
                                                      fourth_slot_end - fourth_slot_begin},
                                    std::ref(this->mock_visitor));

      ASSERT_TRUE(result.ok());
      EXPECT_EQ(this->parser->get_visited_upper_bound(), fourth_slot_end);
    }
  }
}

}  // namespace
