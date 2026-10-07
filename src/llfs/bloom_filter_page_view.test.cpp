//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/bloom_filter_page_view.hpp>
//
#include <llfs/bloom_filter_page_view.hpp>

#include <llfs/memory_page_cache.hpp>
#include <llfs/page_buffer.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/async/worker_pool.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sstream>

namespace {

using namespace llfs::int_types;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BloomFilterPageViewTest, AllMethods)
{
  const std::vector<std::string> items = {"alpha", "bravo", "charlie"};

  auto page_buffer = llfs::PageBuffer::allocate(llfs::PageSize{4096}, llfs::PageId{0});

  llfs::StatusOr<const llfs::PackedBloomFilterPage*> result = llfs::build_bloom_filter_page(
      batt::WorkerPool::null_pool(),
      items,
      [](const std::string& s) -> std::string_view { return s; },
      llfs::BloomFilterLayout::kFlat,
      llfs::BitsPerKey{10},
      llfs::None,
      llfs::PageId{42},
      llfs::ComputeChecksum{false},
      page_buffer);

  ASSERT_TRUE(result.ok()) << result.status();

  std::shared_ptr<const llfs::PageBuffer> const_buffer = page_buffer;
  llfs::BloomFilterPageView view{std::move(const_buffer)};

  // get_page_layout_id
  //
  EXPECT_EQ(view.get_page_layout_id(), llfs::PackedBloomFilterPage::page_layout_id());

  // trace_refs (bloom filter pages have no outgoing refs).
  //
  llfs::BoxedSeq<llfs::PageId> refs = view.trace_refs();
  EXPECT_FALSE(refs.next());

  // min_key / max_key (bloom filter pages have no keys).
  //
  EXPECT_FALSE(view.min_key());
  EXPECT_FALSE(view.max_key());

  // src_page_id
  //
  EXPECT_EQ(view.src_page_id(), llfs::PageId{42});

  // bloom_filter
  //
  const llfs::PackedBloomFilter& bf = view.bloom_filter();
  for (const std::string& s : items) {
    EXPECT_TRUE(bf.might_contain(std::string_view{s})) << "missing: " << s;
  }

  // check_integrity (should not panic)
  //
  view.check_integrity();

  // dump_to_ostream
  //
  std::ostringstream oss;
  view.dump_to_ostream(oss);
  EXPECT_THAT(oss.str(), ::testing::HasSubstr("BloomFilter"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BloomFilterPageViewTest, PageReader)
{
  const std::vector<std::string> items = {"x", "y", "z"};

  auto page_buffer = llfs::PageBuffer::allocate(llfs::PageSize{4096}, llfs::PageId{0});

  llfs::StatusOr<const llfs::PackedBloomFilterPage*> result = llfs::build_bloom_filter_page(
      batt::WorkerPool::null_pool(),
      items,
      [](const std::string& s) -> std::string_view { return s; },
      llfs::BloomFilterLayout::kFlat,
      llfs::BitsPerKey{10},
      llfs::None,
      llfs::PageId{7},
      llfs::ComputeChecksum{false},
      page_buffer);

  ASSERT_TRUE(result.ok()) << result.status();

  llfs::PageReader reader = llfs::BloomFilterPageView::page_reader();

  std::shared_ptr<const llfs::PageBuffer> const_buffer = page_buffer;
  llfs::StatusOr<std::shared_ptr<const llfs::PageView>> view_result = reader(std::move(const_buffer));

  ASSERT_TRUE(view_result.ok()) << view_result.status();

  const auto* bf_view = dynamic_cast<const llfs::BloomFilterPageView*>(view_result->get());
  ASSERT_NE(bf_view, nullptr);
  EXPECT_EQ(bf_view->src_page_id(), llfs::PageId{7});

  // register_layout
  //
  llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> cache_result = llfs::make_memory_page_cache(
      batt::Runtime::instance().default_scheduler(),
      {{llfs::PageCount{4}, llfs::PageSize{4096}}},
      llfs::MaxRefsPerPage{64});

  ASSERT_TRUE(cache_result.ok()) << cache_result.status();

  batt::Status status = llfs::BloomFilterPageView::register_layout(**cache_result);
  EXPECT_TRUE(status.ok()) << status;
}

}  // namespace
