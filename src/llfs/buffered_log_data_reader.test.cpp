//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/buffered_log_data_reader.hpp>
//
#include <llfs/buffered_log_data_reader.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace llfs::int_types;

// Test Plan:
//  1. Construct with empty buffer
//  2. Construct with non-empty buffer, slot_offset == 0
//  3. Construct with non-empty buffer, slot_offset > 0
//  4. Repeated calls to data() with no consume() returns same data
//  5. consume(0)
//  6. consume(data().size())
//  7. consume(n), n < data().size()
//  8. consume(n), n > data().size()
//  9. await()
//     a. SlotUpperBoundAt{n}, n == slot_offset() => OkStatus
//     b. SlotUpperBoundAt{n}, n > slot_offset() => OkStatus
//     c. SlotUpperBoundAt{n}, n < slot_offset() => kOutOfRange
//     d. BytesAvailable{n}, n == data().size() => OkStatus
//     e. BytesAvailable{n}, n < data().size() => OkStatus
//     f. BytesAvailable{n}, n > data().size() => kOutOfRange
//

class BufferedLogDataReaderTest : public ::testing::Test
{
 public:
  const int kNumRepeatedCalls = 5;
  const llfs::slot_offset_type kNonZeroSlotOffset = 98765;

  const llfs::ConstBuffer empty_buffer{"", 0};
  const llfs::ConstBuffer data_buffer{"0123456789", 10};
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  1. Construct with empty buffer
//
TEST_F(BufferedLogDataReaderTest, ConstructFromEmptyBuffer)
{
  {
    llfs::BufferedLogDataReader reader{/*slot_offset=*/0, this->empty_buffer};

    EXPECT_FALSE(reader.is_closed());
    EXPECT_EQ(reader.data().data(), this->empty_buffer.data());
    EXPECT_EQ(reader.data().size(), this->empty_buffer.size());
    EXPECT_EQ(reader.slot_offset(), 0u);
  }
  {
    llfs::BufferedLogDataReader reader{/*slot_offset=*/9999, this->empty_buffer};

    EXPECT_EQ(reader.slot_offset(), 9999u);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  2. Construct with non-empty buffer, slot_offset == 0
//
TEST_F(BufferedLogDataReaderTest, ConstructFromNonEmptyBuffer)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/0, this->data_buffer};

  EXPECT_FALSE(reader.is_closed());
  EXPECT_EQ(reader.data().data(), this->data_buffer.data());
  EXPECT_EQ(reader.data().size(), 10u);
  EXPECT_EQ(reader.slot_offset(), 0u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  3. Construct with non-empty buffer, slot_offset > 0
//
TEST_F(BufferedLogDataReaderTest, ConstructWithSlotOffset)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/kNonZeroSlotOffset, this->data_buffer};

  EXPECT_FALSE(reader.is_closed());
  EXPECT_EQ(reader.data().data(), this->data_buffer.data());
  EXPECT_EQ(reader.data().size(), 10u);
  EXPECT_EQ(reader.slot_offset(), kNonZeroSlotOffset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  4. Repeated calls to data() with no consume() returns same data
//
TEST_F(BufferedLogDataReaderTest, RepeatedDataNoConsume)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/kNonZeroSlotOffset, this->data_buffer};

  for (int n = 0; n < kNumRepeatedCalls; ++n) {
    EXPECT_FALSE(reader.is_closed());
    EXPECT_EQ(reader.data().data(), this->data_buffer.data());
    EXPECT_EQ(reader.data().size(), 10u);
    EXPECT_EQ(reader.slot_offset(), kNonZeroSlotOffset);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  5. consume(0)
//
TEST_F(BufferedLogDataReaderTest, ConsumeZero)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/kNonZeroSlotOffset, this->data_buffer};

  for (int n = 0; n < kNumRepeatedCalls; ++n) {
    reader.consume(0);

    EXPECT_FALSE(reader.is_closed());
    EXPECT_EQ(reader.data().data(), this->data_buffer.data());
    EXPECT_EQ(reader.data().size(), 10u);
    EXPECT_EQ(reader.slot_offset(), kNonZeroSlotOffset);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  6. consume(data().size())
//
TEST_F(BufferedLogDataReaderTest, ConsumeAll)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/kNonZeroSlotOffset, this->data_buffer};

  reader.consume(this->data_buffer.size());

  EXPECT_FALSE(reader.is_closed());
  EXPECT_EQ(reader.data().data(),
            (const void*)((const char*)this->data_buffer.data() + this->data_buffer.size()));
  EXPECT_EQ(reader.data().size(), 0u);
  EXPECT_EQ(reader.slot_offset(), kNonZeroSlotOffset + this->data_buffer.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  7. consume(n), n < data().size()
//
TEST_F(BufferedLogDataReaderTest, ConsumeSome)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/kNonZeroSlotOffset, this->data_buffer};

  usize expected_size = this->data_buffer.size();
  usize bytes_consumed = 0;

  while (expected_size > 0) {
    reader.consume(1);
    expected_size -= 1;
    bytes_consumed += 1;

    EXPECT_FALSE(reader.is_closed());
    EXPECT_EQ(reader.data().data(),
              (const void*)((const char*)this->data_buffer.data() + bytes_consumed));
    EXPECT_EQ(reader.data().size(), expected_size);
    EXPECT_EQ(reader.slot_offset(), kNonZeroSlotOffset + bytes_consumed);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  8. consume(n), n > data().size()
//
TEST_F(BufferedLogDataReaderTest, ConsumeTooMuch)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/kNonZeroSlotOffset, this->data_buffer};

  reader.consume(this->data_buffer.size() + 100);

  EXPECT_FALSE(reader.is_closed());
  EXPECT_EQ(reader.data().data(),
            (const void*)((const char*)this->data_buffer.data() + this->data_buffer.size()));
  EXPECT_EQ(reader.data().size(), 0u);
  EXPECT_EQ(reader.slot_offset(), kNonZeroSlotOffset + this->data_buffer.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//  9. await()
//
TEST_F(BufferedLogDataReaderTest, AwaitSlotUpperBound)
{
  llfs::BufferedLogDataReader reader{/*slot_offset=*/kNonZeroSlotOffset, this->data_buffer};

  const llfs::slot_offset_type expected_upper_bound = kNonZeroSlotOffset + this->data_buffer.size();
  const llfs::slot_offset_type actual_upper_bound = reader.slot_offset() + reader.data().size();

  EXPECT_EQ(expected_upper_bound, actual_upper_bound);

  // a. SlotUpperBoundAt{n}, n == slot_offset() => OkStatus
  {
    batt::Status status = reader.await(llfs::SlotUpperBoundAt{expected_upper_bound});

    EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);
  }

  // b. SlotUpperBoundAt{n}, n > slot_offset() => OkStatus
  {
    batt::Status status = reader.await(llfs::SlotUpperBoundAt{expected_upper_bound - 1});

    EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);
  }

  // c. SlotUpperBoundAt{n}, n < slot_offset() => kOutOfRange
  {
    batt::Status status = reader.await(llfs::SlotUpperBoundAt{expected_upper_bound + 1});

    EXPECT_FALSE(status.ok()) << BATT_INSPECT(status);
    EXPECT_EQ(status, batt::StatusCode::kOutOfRange);
  }

  const llfs::slot_offset_type expected_bytes_available = this->data_buffer.size();
  const llfs::slot_offset_type actual_bytes_available = reader.data().size();

  EXPECT_EQ(expected_bytes_available, actual_bytes_available);

  // d. BytesAvailable{n}, n == data().size() => OkStatus
  {
    batt::Status status = reader.await(llfs::BytesAvailable{expected_bytes_available});

    EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);
  }

  // e. BytesAvailable{n}, n < data().size() => OkStatus
  {
    batt::Status status = reader.await(llfs::BytesAvailable{expected_bytes_available - 1});

    EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);
  }

  // f. BytesAvailable{n}, n > data().size() => kOutOfRange
  {
    batt::Status status = reader.await(llfs::BytesAvailable{expected_bytes_available + 1});

    EXPECT_FALSE(status.ok()) << BATT_INSPECT(status);
    EXPECT_EQ(status, batt::StatusCode::kOutOfRange);
  }
}

}  // namespace
