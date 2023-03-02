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

}  // namespace
