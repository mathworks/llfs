//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_bloom_filter_page.hpp>
//
#include <llfs/packed_bloom_filter_page.hpp>

#include <llfs/page_buffer.hpp>

#include <batteries/async/worker_pool.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace llfs::int_types;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedBloomFilterPageTest, Methods)
{
  using AlignedUnit = std::aligned_storage_t<64, 64>;

  constexpr usize kWordCount = 64;
  constexpr usize kTotalSize =
      sizeof(llfs::PackedBloomFilterPage) + sizeof(llfs::little_u64) * kWordCount;
  constexpr usize kUnitCount = (kTotalSize + sizeof(AlignedUnit) - 1) / sizeof(AlignedUnit);

  std::unique_ptr<AlignedUnit[]> memory{new AlignedUnit[kUnitCount]};
  std::memset(memory.get(), 0, kUnitCount * sizeof(AlignedUnit));

  auto* page = reinterpret_cast<llfs::PackedBloomFilterPage*>(memory.get());

  page->magic = llfs::PackedBloomFilterPage::kMagic;
  page->src_page_id = llfs::PackedPageId::from(llfs::PageId{42});

  auto config = llfs::BloomFilterConfig::from(llfs::BloomFilterLayout::kFlat,
                                               llfs::Word64Count{kWordCount},
                                               llfs::ItemCount{100});

  page->bloom_filter.initialize(config);

  const std::vector<std::string_view> items = {"alpha", "bravo", "charlie", "delta", "echo"};
  page->key_count = items.size();

  for (const auto& item : items) {
    page->bloom_filter.insert(item);
  }

  // page_layout_id
  //
  llfs::PageLayoutId layout_id = llfs::PackedBloomFilterPage::page_layout_id();
  EXPECT_NE(layout_id, llfs::PageLayoutId{});

  // check_magic (should not panic)
  //
  page->check_magic();

  // require_magic ok path
  //
  EXPECT_TRUE(page->require_magic().ok());

  // require_magic error path
  //
  const u64 saved_magic = page->magic;
  page->magic = 0xDEADBEEF;
  EXPECT_FALSE(page->require_magic().ok());
  page->magic = saved_magic;

  // compute_bit_count
  //
  u64 bit_count = page->compute_bit_count();
  EXPECT_GT(bit_count, 0u);

  page->bit_count = bit_count;
  u64 checksum = page->compute_xxh3_checksum();
  EXPECT_NE(checksum, 0u);
  page->xxh3_checksum = checksum;
  page->check_integrity();

  // check_integrity with bit_count == 0 and checksum == 0
  //
  page->bit_count = 0;
  page->xxh3_checksum = 0;
  page->check_integrity();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedBloomFilterPageTest, BuildBloomFilterPage)
{
  const std::vector<std::string> items = {"alpha", "bravo", "charlie", "delta", "echo",
                                           "foxtrot", "golf", "hotel", "india", "juliet"};

  // Without checksum, without explicit hash count.
  //
  {
    auto page_buffer = llfs::PageBuffer::allocate(llfs::PageSize{4096}, llfs::PageId{0});

    llfs::StatusOr<const llfs::PackedBloomFilterPage*> result = llfs::build_bloom_filter_page(
        batt::WorkerPool::null_pool(),
        items,
        [](const std::string& s) -> std::string_view { return s; },
        llfs::BloomFilterLayout::kFlat,
        llfs::BitsPerKey{10},
        llfs::None,
        llfs::PageId{99},
        llfs::ComputeChecksum{false},
        page_buffer);

    ASSERT_TRUE(result.ok()) << result.status();
    const llfs::PackedBloomFilterPage* page = *result;

    EXPECT_EQ(page->magic, llfs::PackedBloomFilterPage::kMagic);
    EXPECT_EQ(page->key_count, items.size());
    EXPECT_EQ(page->src_page_id.as_page_id(), llfs::PageId{99});
    EXPECT_EQ(page->xxh3_checksum, 0u);
    EXPECT_EQ(page->bit_count, 0u);

    for (const std::string& s : items) {
      EXPECT_TRUE(page->bloom_filter.might_contain(std::string_view{s})) << "missing: " << s;
    }
  }

  // With checksum and explicit hash count.
  //
  {
    auto page_buffer = llfs::PageBuffer::allocate(llfs::PageSize{4096}, llfs::PageId{0});

    llfs::StatusOr<const llfs::PackedBloomFilterPage*> result = llfs::build_bloom_filter_page(
        batt::WorkerPool::null_pool(),
        items,
        [](const std::string& s) -> std::string_view { return s; },
        llfs::BloomFilterLayout::kBlocked512,
        llfs::BitsPerKey{10},
        llfs::HashCount{5},
        llfs::PageId{100},
        llfs::ComputeChecksum{true},
        page_buffer);

    ASSERT_TRUE(result.ok()) << result.status();
    const llfs::PackedBloomFilterPage* page = *result;

    EXPECT_EQ(page->magic, llfs::PackedBloomFilterPage::kMagic);
    EXPECT_EQ(page->key_count, items.size());
    EXPECT_EQ(page->bloom_filter.hash_count(), 5u);
    EXPECT_GT(page->bit_count, 0u);
    EXPECT_NE(page->xxh3_checksum, 0u);

    EXPECT_EQ(page->bit_count, page->compute_bit_count());
    EXPECT_EQ(page->xxh3_checksum, page->compute_xxh3_checksum());
    page->check_integrity();

    for (const std::string& s : items) {
      EXPECT_TRUE(page->bloom_filter.might_contain(std::string_view{s})) << "missing: " << s;
    }
  }
}

}  // namespace
