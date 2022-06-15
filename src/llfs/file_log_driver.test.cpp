//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/file_log_driver.hpp>
//
#include <llfs/file_log_driver.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

TEST(FileLogDriverTest, SegmentFilenameParser)
{
  using llfs::FileLogDriver;
  using llfs::None;
  using llfs::SlotRange;

  FileLogDriver::Location loc{"not_used_"};

  EXPECT_EQ(*loc.slot_range_from_segment_file_name("not_used_0000.0001.tdblog"),
            (SlotRange{0x0, 0x1}));
  EXPECT_EQ(*loc.slot_range_from_segment_file_name("not_used_0001.0001.tdblog"),
            (SlotRange{0x1, 0x2}));
  EXPECT_EQ(*loc.slot_range_from_segment_file_name("not_used_0002.1.tdblog"),
            (SlotRange{0x2, 0x3}));
  EXPECT_EQ(*loc.slot_range_from_segment_file_name("not_used_000a.1.tdblog"),
            (SlotRange{0xa, 0xb}));
  EXPECT_EQ(*loc.slot_range_from_segment_file_name("not_used_5b7e.10.tdblog"),
            (SlotRange{0x5b7e, 0x5b8e}));
  EXPECT_EQ(loc.slot_range_from_segment_file_name("not_used_5b7e.a1.tdblo"), None);
  EXPECT_EQ(loc.slot_range_from_segment_file_name("not_used_5b7e..tdblog"), None);
  EXPECT_EQ(loc.slot_range_from_segment_file_name("not_used_.7.tdblog"), None);
  EXPECT_EQ(loc.slot_range_from_segment_file_name("4.tdblog"), None);
  EXPECT_EQ(loc.slot_range_from_segment_file_name(".4.tdblog"), None);
  EXPECT_EQ(*loc.slot_range_from_segment_file_name("3.4.tdblog"), (SlotRange{3, 7}));
  EXPECT_EQ(loc.slot_range_from_segment_file_name(".tdblog"), None);
  EXPECT_EQ(*loc.slot_range_from_segment_file_name("not_used_000a.0.tdblognot_used_000b.0.tdblog"),
            (SlotRange{0xb, 0xb}));
  EXPECT_EQ(*loc.slot_range_from_segment_file_name("not_used_000b.0.tdblognot_used_000a.0.tdblog"),
            (SlotRange{0xa, 0xa}));
}

}  // namespace
