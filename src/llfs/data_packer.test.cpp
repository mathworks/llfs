//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/data_packer.hpp>
//
#include <llfs/data_packer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/stream_util.hpp>

#include <cstring>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

TEST(DataPackerTest, Arena)
{
  std::array<u8, 64> memory;
  memory.fill(0);

  std::array<u8, 64> expected;
  expected.fill(0);

  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(24);
  ASSERT_TRUE(arena);

  EXPECT_EQ(arena->capacity(), 24u);
  EXPECT_EQ(arena->space(), 24u);
  EXPECT_FALSE(arena->full());
  EXPECT_EQ(arena->unused(), (batt::Interval<isize>{40, 64}));

  EXPECT_EQ(0, std::memcmp(memory.data(), expected.data(), memory.size()));

  const std::string_view test_str = "hello, world";

  llfs::Optional<std::string_view> packed_str = packer.pack_string(test_str, &*arena);
  ASSERT_TRUE(packed_str);

  EXPECT_EQ(*packed_str, test_str);

  // Set up the expected data.
  //
  {
    auto* packed_bytes = reinterpret_cast<llfs::PackedBytes*>(expected.data());
    packed_bytes->data_offset = 40;
    packed_bytes->data_size = test_str.size();
    std::memcpy(expected.data() + 40, test_str.data(), test_str.size());
  }

  // Compare actual to expected.
  //
  EXPECT_EQ(0, std::memcmp(memory.data(), expected.data(), memory.size()))
      << "\nmemory=\n"
      << batt::dump_hex(memory.data(), memory.size()) << "\nexpected=\n"
      << batt::dump_hex(expected.data(), expected.size());

  auto* packed_bytes1 = reinterpret_cast<const llfs::PackedBytes*>(memory.data());

  EXPECT_EQ(packed_bytes1->size(), test_str.size());
  EXPECT_EQ((void*)packed_bytes1->data(), memory.data() + 40);

  // Now pack a string in the regular way and see that it lands at the back of the available region.
  //
  const std::string_view test_str2 = "so long, farewell";
  llfs::Optional<std::string_view> packed_str2 = packer.pack_string(test_str2);
  ASSERT_TRUE(packed_str2);

  EXPECT_EQ(*packed_str2, test_str2);

  // Update the expected data.
  //
  {
    auto* packed_bytes2 = reinterpret_cast<llfs::PackedBytes*>(expected.data()) + 1;
    packed_bytes2->data_offset = 40 - (sizeof(llfs::PackedBytes) + test_str2.size());
    packed_bytes2->data_size = test_str2.size();
    std::memcpy(expected.data() + (40 - test_str2.size()), test_str2.data(), test_str2.size());
  }

  // Compare actual to expected.
  //
  EXPECT_EQ(0, std::memcmp(memory.data(), expected.data(), memory.size()))
      << "\nmemory=\n"
      << batt::dump_hex(memory.data(), memory.size()) << "\nexpected=\n"
      << batt::dump_hex(expected.data(), expected.size());
}

constexpr usize kDataCopySize = 512 * kKiB;
constexpr usize kCopyRepeat = 1 * 1000;

void run_parallel_copy_test(bool on, usize min_task_size)
{
  std::vector<char> src_buffer(kDataCopySize);
  std::vector<char> dst_buffer(kDataCopySize + 100);

  if (on) {
    llfs::DataPacker::min_parallel_copy_size() = min_task_size;
  }

  const void* packed_data = nullptr;

  for (usize i = 0; i < kCopyRepeat; ++i) {
    llfs::DataPacker packer{llfs::MutableBuffer{dst_buffer.data(), dst_buffer.size()}};
    packer.set_worker_pool(batt::WorkerPool::default_pool());

    llfs::PackedBytes* packed_bytes = packer.pack_record<llfs::PackedBytes>();
    ASSERT_NE(packed_bytes, nullptr);

    packed_data = packer.pack_data_to(packed_bytes, src_buffer.data(), src_buffer.size(),
                                      llfs::UseParallelCopy{on});
    ASSERT_NE(packed_data, nullptr);
  }
  ASSERT_EQ(std::memcmp(packed_data, src_buffer.data(), src_buffer.size()), 0);
}

TEST(DataPackerTest, ParallelDataCopyTrue1k)
{
  run_parallel_copy_test(true, 1 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue2k)
{
  run_parallel_copy_test(true, 2 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue4k)
{
  run_parallel_copy_test(true, 4 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue8k)
{
  run_parallel_copy_test(true, 8 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue16k)
{
  run_parallel_copy_test(true, 16 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue32k)
{
  run_parallel_copy_test(true, 32 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue43k)
{
  run_parallel_copy_test(true, 43 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue64k)
{
  run_parallel_copy_test(true, 64 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue75k)
{
  run_parallel_copy_test(true, 75 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue86k)
{
  run_parallel_copy_test(true, 86 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue97k)
{
  run_parallel_copy_test(true, 97 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue128k)
{
  run_parallel_copy_test(true, 128 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue256k)
{
  run_parallel_copy_test(true, 256 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue512k)
{
  run_parallel_copy_test(true, 512 * kKiB);
}
TEST(DataPackerTest, ParallelDataCopyTrue1024k)
{
  run_parallel_copy_test(true, 1024 * kKiB);
}

TEST(DataPackerTest, ParallelDataCopyFalse)
{
  run_parallel_copy_test(false, 0);
}

/*! \brief Runs a DataPacker (pack_record) test to check space allocation correctness.
 *
 * The function is allocating space for 'N' (='count') elements using a DataPacker object.
 * DataPacker object is initialized to have 64 bytes of buffer space. Each element is 4 bytes long.
 * Further, it uses default parameter value when count=1. Post space allocation it verifies
 * DataPacker object's remaining space and unused space range. For a negative test it checks to make
 * sure there was no space allocation.
 *
 * \param count Specify number of elements which data packer will use to allocate the space for.
 *
 */
void run_pack_record_test(const usize count)
{
  struct MyTemp {
    int data;
  };
  constexpr u64 kMemSize = 64;
  u64 requested_space = sizeof(MyTemp) * count;
  std::array<u8, kMemSize> memory;

  memory.fill(0);

  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  EXPECT_EQ(packer.space(), kMemSize);

  MyTemp* p_buff;
  if (count != 1) {
    p_buff = packer.pack_record(batt::StaticType<MyTemp>{}, count);
  } else {
    p_buff = packer.pack_record(batt::StaticType<MyTemp>{});  // Use default 'count' as '1'.
  }

  if (requested_space > kMemSize) {
    requested_space = 0;
    EXPECT_EQ(p_buff, reinterpret_cast<MyTemp*>(0));
  } else {
    EXPECT_GE(p_buff, reinterpret_cast<MyTemp*>(packer.buffer_begin()));
    EXPECT_LE(p_buff, reinterpret_cast<MyTemp*>(packer.buffer_end() - 1));
  }

  const auto space_remaining = kMemSize - requested_space;

  EXPECT_EQ(packer.space(), space_remaining);
  EXPECT_EQ(packer.unused(), (batt::Interval<isize>{i64(requested_space), kMemSize}));
}

TEST(DataPackerTest, PackRecordCnt1)
{
  run_pack_record_test(1);
}

TEST(DataPackerTest, PackRecordCnt5)
{
  run_pack_record_test(5);
}

TEST(DataPackerTest, PackRecordCnt16)
{
  run_pack_record_test(16);
}

TEST(DataPackerTest, PackRecordCntNegative)
{
  run_pack_record_test(17);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Utility / state tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, Contains)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  u32* rec = packer.pack_record<u32>();
  ASSERT_NE(rec, nullptr);
  EXPECT_TRUE(packer.contains(rec));

  u32 outside;
  EXPECT_FALSE(packer.contains(&outside));
}

TEST(DataPackerTest, SizeTracking)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  EXPECT_EQ(packer.size(), 0u);
  EXPECT_EQ(packer.space(), 128u);
  EXPECT_EQ(packer.size() + packer.space(), packer.buffer_size());

  u32* rec = packer.pack_record<u32>();
  ASSERT_NE(rec, nullptr);

  EXPECT_EQ(packer.size(), sizeof(u32));
  EXPECT_EQ(packer.space(), 128u - sizeof(u32));
  EXPECT_EQ(packer.size() + packer.space(), packer.buffer_size());
}

TEST(DataPackerTest, SetFull)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  EXPECT_FALSE(packer.full());
  packer.set_full();
  EXPECT_TRUE(packer.full());
}

TEST(DataPackerTest, InvalidateAndBoolOperator)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  EXPECT_TRUE(static_cast<bool>(packer));
  packer.invalidate();
  EXPECT_FALSE(static_cast<bool>(packer));
}

TEST(DataPackerTest, AvailBuffer)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::MutableBuffer avail = packer.avail_buffer();
  EXPECT_EQ(avail.size(), 64u);
  EXPECT_EQ(avail.data(), memory.data());

  u32* rec = packer.pack_record<u32>();
  ASSERT_NE(rec, nullptr);

  avail = packer.avail_buffer();
  EXPECT_EQ(avail.size(), 60u);
}

TEST(DataPackerTest, WorkerPoolGetSet)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  EXPECT_FALSE(packer.worker_pool());

  packer.set_worker_pool(batt::WorkerPool::default_pool());
  EXPECT_TRUE(packer.worker_pool());

  packer.clear_worker_pool();
  EXPECT_FALSE(packer.worker_pool());
}

TEST(DataPackerTest, ReserveFront)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::MutableBuffer> buf = packer.reserve_front(16);
  ASSERT_TRUE(buf);
  EXPECT_EQ(buf->size(), 16u);
  EXPECT_EQ(buf->data(), memory.data());
  EXPECT_EQ(packer.space(), 48u);

  llfs::Optional<llfs::MutableBuffer> too_big = packer.reserve_front(128);
  EXPECT_FALSE(too_big);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// pack_string tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackStringShort)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<std::string_view> result = packer.pack_string("abc");
  ASSERT_TRUE(result);
  EXPECT_EQ(*result, "abc");
}

TEST(DataPackerTest, PackStringEmpty)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<std::string_view> result = packer.pack_string(std::string_view{});
  ASSERT_TRUE(result);
  EXPECT_EQ(result->size(), 0u);
}

TEST(DataPackerTest, PackStringInsufficientSpace)
{
  std::array<u8, 8> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<std::string_view> result = packer.pack_string("this string is too long to fit");
  EXPECT_FALSE(result);
  EXPECT_TRUE(packer.full());
}

TEST(DataPackerTest, PackStringTo)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::PackedBytes* rec = packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(rec, nullptr);

  const std::string_view test_str = "record string";

  llfs::Optional<std::string_view> result = packer.pack_string_to(rec, test_str);
  ASSERT_TRUE(result);
  EXPECT_EQ(*result, test_str);
  EXPECT_EQ(rec->size(), test_str.size());
  EXPECT_EQ(rec->as_str(), test_str);
}

TEST(DataPackerTest, PackStringToWithArena)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(32);
  ASSERT_TRUE(arena);

  llfs::PackedBytes* rec = packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(rec, nullptr);

  const std::string_view test_str = "arena rec str";

  llfs::Optional<std::string_view> result = packer.pack_string_to(rec, test_str, &*arena);
  ASSERT_TRUE(result);
  EXPECT_EQ(*result, test_str);
  EXPECT_EQ(rec->as_str(), test_str);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// pack_data tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackDataBasic)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  const std::string_view data = "hello packer";

  const void* result = packer.pack_data(data.data(), data.size());
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(std::memcmp(result, data.data(), data.size()), 0);
}

TEST(DataPackerTest, PackDataSmall)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  const u8 small_data[] = {0xAB, 0xCD};

  const void* result = packer.pack_data(small_data, 2);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(std::memcmp(result, small_data, 2), 0);
}

TEST(DataPackerTest, PackDataZeroSize)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  const void* result = packer.pack_data(nullptr, 0);
  ASSERT_NE(result, nullptr);
}

TEST(DataPackerTest, PackDataInsufficientSpace)
{
  std::array<u8, 16> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  std::array<u8, 128> big_data;
  big_data.fill(0xFF);

  const void* result = packer.pack_data(big_data.data(), big_data.size());
  EXPECT_EQ(result, nullptr);
  EXPECT_TRUE(packer.full());
}

TEST(DataPackerTest, PackDataWithArena)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(64);
  ASSERT_TRUE(arena);

  const std::string_view data = "arena data test";

  const void* result = packer.pack_data(data.data(), data.size(), &*arena);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(std::memcmp(result, data.data(), data.size()), 0);
}

TEST(DataPackerTest, PackDataWithArenaInsufficientSpace)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(4);
  ASSERT_TRUE(arena);

  const std::string_view data = "this data is bigger than arena";

  const void* result = packer.pack_data(data.data(), data.size(), &*arena);
  EXPECT_EQ(result, nullptr);
}

TEST(DataPackerTest, PackDataWhenAlreadyFull)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  packer.set_full();

  const void* result = packer.pack_data("test", 4);
  EXPECT_EQ(result, nullptr);

  llfs::Optional<std::string_view> str_result = packer.pack_string("test");
  EXPECT_FALSE(str_result);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// pack_data_to tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackDataToBasic)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::PackedBytes* rec = packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(rec, nullptr);

  const std::string_view data = "pack data to";

  const void* result = packer.pack_data_to(rec, data.data(), data.size());
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(std::memcmp(result, data.data(), data.size()), 0);
  EXPECT_EQ(rec->size(), data.size());
}

TEST(DataPackerTest, PackDataToWithArena)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(64);
  ASSERT_TRUE(arena);

  llfs::PackedBytes* rec = packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(rec, nullptr);

  const std::string_view data = "arena pack to";

  const void* result = packer.pack_data_to(rec, data.data(), data.size(), &*arena);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(std::memcmp(result, data.data(), data.size()), 0);
  EXPECT_EQ(rec->size(), data.size());
}

TEST(DataPackerTest, PackDataToInsufficientSpace)
{
  std::array<u8, 16> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::PackedBytes* rec = packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(rec, nullptr);

  std::array<u8, 128> big_data;
  big_data.fill(0xFF);

  const void* result = packer.pack_data_to(rec, big_data.data(), big_data.size());
  EXPECT_EQ(result, nullptr);
  EXPECT_TRUE(packer.full());
}

TEST(DataPackerTest, PackDataToWithArenaInsufficientSpace)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(4);
  ASSERT_TRUE(arena);

  llfs::PackedBytes* rec = packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(rec, nullptr);

  const std::string_view data = "too much data for arena";

  const void* result = packer.pack_data_to(rec, data.data(), data.size(), &*arena);
  EXPECT_EQ(result, nullptr);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// pack_data_copy tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackDataCopyLarge)
{
  std::array<u8, 128> src_memory;
  src_memory.fill(0);
  llfs::DataPacker src_packer{llfs::MutableBuffer{src_memory.data(), src_memory.size()}};

  const std::string_view data = "copy this data";
  const void* packed = src_packer.pack_data(data.data(), data.size());
  ASSERT_NE(packed, nullptr);

  const auto* src_rec = reinterpret_cast<const llfs::PackedBytes*>(src_memory.data());

  std::array<u8, 128> dst_memory;
  dst_memory.fill(0);
  llfs::DataPacker dst_packer{llfs::MutableBuffer{dst_memory.data(), dst_memory.size()}};

  const llfs::PackedBytes* result = dst_packer.pack_data_copy(*src_rec);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), data.size());
  EXPECT_EQ(result->as_str(), data);
}

TEST(DataPackerTest, PackDataCopySmall)
{
  std::array<u8, 64> src_memory;
  src_memory.fill(0);
  llfs::DataPacker src_packer{llfs::MutableBuffer{src_memory.data(), src_memory.size()}};

  const std::string_view data = "ab";
  const void* packed = src_packer.pack_data(data.data(), data.size());
  ASSERT_NE(packed, nullptr);

  const auto* src_rec = reinterpret_cast<const llfs::PackedBytes*>(src_memory.data());

  std::array<u8, 64> dst_memory;
  dst_memory.fill(0);
  llfs::DataPacker dst_packer{llfs::MutableBuffer{dst_memory.data(), dst_memory.size()}};

  const llfs::PackedBytes* result = dst_packer.pack_data_copy(*src_rec);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), data.size());
  EXPECT_EQ(result->as_str(), data);
}

TEST(DataPackerTest, PackDataCopyWithArena)
{
  std::array<u8, 128> src_memory;
  src_memory.fill(0);
  llfs::DataPacker src_packer{llfs::MutableBuffer{src_memory.data(), src_memory.size()}};

  const std::string_view data = "copy to arena";
  const void* packed = src_packer.pack_data(data.data(), data.size());
  ASSERT_NE(packed, nullptr);

  const auto* src_rec = reinterpret_cast<const llfs::PackedBytes*>(src_memory.data());

  std::array<u8, 128> dst_memory;
  dst_memory.fill(0);
  llfs::DataPacker dst_packer{llfs::MutableBuffer{dst_memory.data(), dst_memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = dst_packer.reserve_arena(64);
  ASSERT_TRUE(arena);

  const llfs::PackedBytes* result = dst_packer.pack_data_copy(*src_rec, &*arena);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), data.size());
  EXPECT_EQ(result->as_str(), data);
}

TEST(DataPackerTest, PackDataCopyInsufficientSpace)
{
  std::array<u8, 128> src_memory;
  src_memory.fill(0);
  llfs::DataPacker src_packer{llfs::MutableBuffer{src_memory.data(), src_memory.size()}};

  const std::string_view data = "big copy data string";
  const void* packed = src_packer.pack_data(data.data(), data.size());
  ASSERT_NE(packed, nullptr);

  const auto* src_rec = reinterpret_cast<const llfs::PackedBytes*>(src_memory.data());

  std::array<u8, 12> dst_memory;
  dst_memory.fill(0);
  llfs::DataPacker dst_packer{llfs::MutableBuffer{dst_memory.data(), dst_memory.size()}};

  const llfs::PackedBytes* result = dst_packer.pack_data_copy(*src_rec);
  EXPECT_EQ(result, nullptr);
  EXPECT_TRUE(dst_packer.full());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// pack_data_copy_to tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackDataCopyToBasic)
{
  std::array<u8, 128> src_memory;
  src_memory.fill(0);
  llfs::DataPacker src_packer{llfs::MutableBuffer{src_memory.data(), src_memory.size()}};

  const std::string_view data = "copy to dst";
  const void* packed = src_packer.pack_data(data.data(), data.size());
  ASSERT_NE(packed, nullptr);

  const auto* src_rec = reinterpret_cast<const llfs::PackedBytes*>(src_memory.data());

  std::array<u8, 128> dst_memory;
  dst_memory.fill(0);
  llfs::DataPacker dst_packer{llfs::MutableBuffer{dst_memory.data(), dst_memory.size()}};

  llfs::PackedBytes* dst_rec = dst_packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(dst_rec, nullptr);

  const llfs::PackedBytes* result = dst_packer.pack_data_copy_to(dst_rec, *src_rec);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), data.size());
  EXPECT_EQ(result->as_str(), data);
}

TEST(DataPackerTest, PackDataCopyToWithArena)
{
  std::array<u8, 128> src_memory;
  src_memory.fill(0);
  llfs::DataPacker src_packer{llfs::MutableBuffer{src_memory.data(), src_memory.size()}};

  const std::string_view data = "copy dst arena";
  const void* packed = src_packer.pack_data(data.data(), data.size());
  ASSERT_NE(packed, nullptr);

  const auto* src_rec = reinterpret_cast<const llfs::PackedBytes*>(src_memory.data());

  std::array<u8, 128> dst_memory;
  dst_memory.fill(0);
  llfs::DataPacker dst_packer{llfs::MutableBuffer{dst_memory.data(), dst_memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = dst_packer.reserve_arena(64);
  ASSERT_TRUE(arena);

  llfs::PackedBytes* dst_rec = dst_packer.pack_record<llfs::PackedBytes>();
  ASSERT_NE(dst_rec, nullptr);

  const llfs::PackedBytes* result = dst_packer.pack_data_copy_to(dst_rec, *src_rec, &*arena);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), data.size());
  EXPECT_EQ(result->as_str(), data);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// pack_raw_data tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackRawDataBasic)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  const std::string_view data = "raw data test";

  llfs::Optional<std::string_view> result = packer.pack_raw_data(data.data(), data.size());
  ASSERT_TRUE(result);
  EXPECT_EQ(*result, data);
  EXPECT_EQ(packer.space(), 128u - data.size());
}

TEST(DataPackerTest, PackRawDataInsufficientSpace)
{
  std::array<u8, 8> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  std::array<u8, 32> big_data;
  big_data.fill(0xAA);

  llfs::Optional<std::string_view> result = packer.pack_raw_data(big_data.data(), big_data.size());
  EXPECT_FALSE(result);
}

TEST(DataPackerTest, PackRawDataParallelCopyEnabled)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};
  packer.set_worker_pool(batt::WorkerPool::default_pool());

  const std::string_view data = "parallel raw data";

  llfs::Optional<std::string_view> result =
      packer.pack_raw_data(data.data(), data.size(), llfs::UseParallelCopy{true});
  ASSERT_TRUE(result);
  EXPECT_EQ(*result, data);
}

TEST(DataPackerTest, PackRawDataParallelCopyDisabled)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  const std::string_view data = "no parallel copy";

  llfs::Optional<std::string_view> result =
      packer.pack_raw_data(data.data(), data.size(), llfs::UseParallelCopy{false});
  ASSERT_TRUE(result);
  EXPECT_EQ(*result, data);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Integer packing tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackIntTypes)
{
  std::array<u8, 256> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  EXPECT_TRUE(packer.pack_u8(u8{0xFF}));
  EXPECT_TRUE(packer.pack_u16(u16{0x1234}));
  EXPECT_TRUE(packer.pack_u32(u32{0xDEADBEEF}));
  EXPECT_TRUE(packer.pack_u64(u64{0x0102030405060708}));

  EXPECT_TRUE(packer.pack_i8(i8{-1}));
  EXPECT_TRUE(packer.pack_i16(i16{-256}));
  EXPECT_TRUE(packer.pack_i32(i32{-100000}));
  EXPECT_TRUE(packer.pack_i64(i64{-1000000000000LL}));
}

TEST(DataPackerTest, PackIntInsufficientSpace)
{
  std::array<u8, 4> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  EXPECT_TRUE(packer.pack_u32(u32{42}));
  EXPECT_FALSE(packer.pack_u8(u8{1}));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Container packing tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerTest, PackArrayBasic)
{
  std::array<u8, 256> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::ArrayPacker<llfs::little_u32>> array =
      packer.pack_array<llfs::little_u32>();
  ASSERT_TRUE(array);
}

TEST(DataPackerTest, PackArrayInsufficientSpace)
{
  std::array<u8, 2> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::ArrayPacker<llfs::little_u32>> array =
      packer.pack_array<llfs::little_u32>();
  EXPECT_FALSE(array);
}

TEST(DataPackerTest, PackVarint)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  u8* result = packer.pack_varint(0);
  ASSERT_NE(result, nullptr);

  result = packer.pack_varint(127);
  ASSERT_NE(result, nullptr);

  result = packer.pack_varint(128);
  ASSERT_NE(result, nullptr);

  result = packer.pack_varint(16383);
  ASSERT_NE(result, nullptr);

  result = packer.pack_varint(0xFFFFFFFF);
  ASSERT_NE(result, nullptr);
}

TEST(DataPackerTest, PackVarintWhenFull)
{
  std::array<u8, 64> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  packer.set_full();
  u8* result = packer.pack_varint(42);
  EXPECT_EQ(result, nullptr);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// DataPackerArena tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(DataPackerArenaTest, ReserveFront)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(64);
  ASSERT_TRUE(arena);

  EXPECT_EQ(arena->capacity(), 64u);
  EXPECT_EQ(arena->space(), 64u);

  llfs::Optional<llfs::DataPacker::Arena> sub_arena = arena->reserve_front(24);
  ASSERT_TRUE(sub_arena);

  EXPECT_EQ(sub_arena->capacity(), 24u);
  EXPECT_EQ(sub_arena->space(), 24u);
  EXPECT_FALSE(sub_arena->full());

  EXPECT_EQ(arena->space(), 40u);
}

TEST(DataPackerArenaTest, ReserveFrontInsufficientSpace)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(16);
  ASSERT_TRUE(arena);

  llfs::Optional<llfs::DataPacker::Arena> sub_arena = arena->reserve_front(32);
  EXPECT_FALSE(sub_arena);
  EXPECT_TRUE(arena->full());
}

TEST(DataPackerArenaTest, ReserveBackInsufficientSpace)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(16);
  ASSERT_TRUE(arena);

  llfs::Optional<llfs::DataPacker::Arena> sub_arena = arena->reserve_back(32);
  EXPECT_FALSE(sub_arena);
  EXPECT_TRUE(arena->full());
}

TEST(DataPackerArenaTest, PackVarintInsufficientSpace)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(1);
  ASSERT_TRUE(arena);
  EXPECT_FALSE(arena->full());

  // 0xFFFFFFFF requires 5 bytes as a varint, but the arena only has 1 byte.
  u8* result = arena->pack_varint(0xFFFFFFFF);
  EXPECT_EQ(result, nullptr);
  EXPECT_TRUE(arena->full());
}

TEST(DataPackerArenaTest, Invalidate)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(32);
  ASSERT_TRUE(arena);

  EXPECT_EQ(arena->capacity(), 32u);
  EXPECT_EQ(arena->space(), 32u);
  EXPECT_FALSE(arena->full());

  arena->invalidate();

  EXPECT_EQ(arena->capacity(), 0u);
  EXPECT_EQ(arena->space(), 0u);
  EXPECT_TRUE(arena->full());
}

TEST(DataPackerArenaTest, SetFull)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(32);
  ASSERT_TRUE(arena);

  EXPECT_FALSE(arena->full());
  EXPECT_EQ(arena->space(), 32u);

  arena->set_full();

  EXPECT_TRUE(arena->full());
  EXPECT_EQ(arena->space(), 32u);

  u8* result = arena->pack_varint(1);
  EXPECT_EQ(result, nullptr);
}

TEST(DataPackerArenaTest, MoveConstruct)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena = packer.reserve_arena(64);
  ASSERT_TRUE(arena);

  llfs::Optional<llfs::MutableBuffer> buf = arena->allocate_front(16);
  ASSERT_TRUE(buf);
  EXPECT_EQ(arena->capacity(), 64u);
  EXPECT_EQ(arena->space(), 48u);

  llfs::DataPacker::Arena moved{std::move(*arena)};

  EXPECT_EQ(moved.capacity(), 64u);
  EXPECT_EQ(moved.space(), 48u);
  EXPECT_FALSE(moved.full());
}

TEST(DataPackerArenaTest, MoveAssign)
{
  std::array<u8, 128> memory;
  memory.fill(0);
  llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

  llfs::Optional<llfs::DataPacker::Arena> arena1 = packer.reserve_arena(32);
  ASSERT_TRUE(arena1);

  llfs::Optional<llfs::DataPacker::Arena> arena2 = packer.reserve_arena(16);
  ASSERT_TRUE(arena2);

  llfs::Optional<llfs::MutableBuffer> buf = arena1->allocate_front(8);
  ASSERT_TRUE(buf);

  EXPECT_EQ(arena1->capacity(), 32u);
  EXPECT_EQ(arena1->space(), 24u);
  EXPECT_EQ(arena2->capacity(), 16u);

  *arena2 = std::move(*arena1);

  EXPECT_EQ(arena2->capacity(), 32u);
  EXPECT_EQ(arena2->space(), 24u);
  EXPECT_FALSE(arena2->full());
}

}  // namespace
