//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ring_buffer.hpp>
//
#include <llfs/ring_buffer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <bitset>
#include <cstring>
#include <random>

namespace {

using namespace llfs::int_types;

using llfs::RingBuffer;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(RingBufferTest, Test)
{
  RingBuffer rb{RingBuffer::TempFile{4096}};

  ASSERT_GE(rb.size(), 4096ul);

  auto b = rb.get_mut(0);
  char* p = (char*)b.data();

  const char kTestData[] = "Four score and seven years ago...";

  std::memcpy(&p[0], &kTestData[10], std::strlen(kTestData) - 10);
  std::memcpy(&p[rb.size() - 10], kTestData, 10);

  auto b2 = rb.get(rb.size() * 19 - 10);

  EXPECT_EQ(0, std::memcmp(b2.data(), kTestData, std::strlen(kTestData)));

  using Interval = ::batt::Interval<::batt::isize>;

  for (batt::isize wrap_count = 0; wrap_count < 3; ++wrap_count) {
    EXPECT_THAT(
        rb.physical_offsets_from_logical(Interval{59 + wrap_count * 4096, 298 + wrap_count * 4096}),
        ::testing::ElementsAre(Interval{59, 298}));

    EXPECT_THAT(rb.physical_offsets_from_logical(
                    Interval{789 + wrap_count * 4096, 4096 + wrap_count * 4096}),
                ::testing::ElementsAre(Interval{789, 4096}));

    EXPECT_THAT(rb.physical_offsets_from_logical(
                    Interval{789 + wrap_count * 4096, 4100 + wrap_count * 4096}),
                ::testing::ElementsAre(Interval{789, 4096}, Interval{0, 4}));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(RingBufferTest, PooledBuffers)
{
  const bool previously_enabled = RingBuffer::pool_enabled().exchange(true);
  auto on_scope_exit = batt::finally([&] {
    RingBuffer::pool_enabled().store(previously_enabled);
  });

  for (usize j = 0; j < 3; ++j) {
    // No interference from other tests, please!
    //
    RingBuffer::reset_pool();

    void* ptr_4k = nullptr;
    void* ptr_16k = nullptr;
    {
      RingBuffer rb{RingBuffer::TempFile{4096}};
      ptr_4k = rb.get_mut(0).data();
    }
    {
      RingBuffer rb{RingBuffer::TempFile{16384}};
      ptr_16k = rb.get_mut(0).data();
    }

    EXPECT_NE(ptr_4k, ptr_16k);
    EXPECT_NE(ptr_4k, nullptr);
    EXPECT_NE(ptr_16k, nullptr);

    // Verify that each are re-used from the pool.
    //
    for (usize i = 0; i < 10; ++i) {
      {
        RingBuffer rb{RingBuffer::TempFile{4096}};
        EXPECT_EQ(ptr_4k, rb.get_mut(0).data());
      }
      {
        RingBuffer rb{RingBuffer::TempFile{16384}};
        EXPECT_EQ(ptr_16k, rb.get_mut(0).data());
      }
    }

    // Exhaust the pool and force a realloc.
    //
    {
      RingBuffer rb1{RingBuffer::TempFile{4096}};
      EXPECT_EQ(ptr_4k, rb1.get_mut(0).data());

      RingBuffer rb2{RingBuffer::TempFile{4096}};
      EXPECT_NE(ptr_4k, rb2.get_mut(0).data());
      EXPECT_NE(nullptr, rb2.get_mut(0).data());
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(RingBufferTest, PoolIndex)
{
  const u16 index0 = RingBuffer::pool_index_from_buffer_size(0);

  EXPECT_EQ(index0, (63 << 4)) << BATT_INSPECT(std::bitset<16>{index0});
  EXPECT_EQ(RingBuffer::buffer_size_from_pool_index(index0), 0u);

  std::default_random_engine rng{1};
  for (u64 max_size : {u64{65536}, u64{1024} * 1024, u64{1} << 30, u64{1} << 63}) {
    std::uniform_int_distribution<u64> pick_size(1, max_size);
    for (usize i = 0; i < std::min<usize>(max_size * 2, 20 * 1000 * 1000); ++i) {
      u64 size = pick_size(rng);

      u16 index = RingBuffer::pool_index_from_buffer_size(size);

      EXPECT_GE(index, 0);
      EXPECT_LT(index, RingBuffer::kNumSubpools);

      u64 normalized_size = RingBuffer::buffer_size_from_pool_index(index);

      ASSERT_GE(normalized_size, size) << BATT_INSPECT(index) << " " << std::bitset<12>{index};
      ASSERT_LE(__builtin_popcountll(normalized_size), 4);
    }
  }
}

}  // namespace
