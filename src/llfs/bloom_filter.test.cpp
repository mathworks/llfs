//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/bloom_filter.hpp>
//
#include <llfs/bloom_filter.hpp>

#include <llfs/metrics.hpp>
#include <llfs/slice.hpp>

#include <batteries/segv.hpp>

#include <random>
#include <sstream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using llfs::as_slice;
using llfs::LatencyMetric;
using llfs::LatencyTimer;
using llfs::PackedBloomFilter;
using llfs::parallel_build_bloom_filter;

using namespace llfs::int_types;

using batt::WorkerPool;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename Rng>
std::string make_random_word(Rng& rng)
{
  static std::exponential_distribution<> pick_negative_len;
  static std::uniform_int_distribution<char> pick_letter('a', 'z');

  static constexpr double kMaxLen = 12;

  double nl = std::min(kMaxLen - 1, pick_negative_len(rng));
  usize len = kMaxLen - nl + 1;
  std::ostringstream oss;
  for (usize i = 0; i < len; ++i) {
    oss << pick_letter(rng);
  }
  return std::move(oss).str();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
// TODO [tastolfi 2025-02-04] Add test for re-using same Query object, with multiple filters, each
// of which has different layout, size, and/or hash_count.

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

TEST(BloomFilterTest, RandomItems)
{
  std::vector<std::vector<std::string>> input_sets;
  std::vector<std::string> query_keys;

  // Generate random data for the tests.
  //
  for (usize r = 1; r <= 3; ++r) {
    std::default_random_engine rng{15485863 /*(1M-th prime)*/ * r};

    {
      std::vector<std::string> items;

      for (usize i = 0; i < 10 * 1000; ++i) {
        items.emplace_back(make_random_word(rng));
      }
      for (const auto& s : as_slice(items.data(), 40)) {
        LLFS_VLOG(1) << "sample random word: " << s;
      }
      std::sort(items.begin(), items.end());

      input_sets.emplace_back(std::move(items));
    }

    for (usize j = 0; j < 2 * 1000 * 1000; ++j) {
      std::string query_str = make_random_word(rng);
      query_keys.emplace_back(std::move(query_str));
    }
  }

  using AlignedUnit = std::aligned_storage_t<64, 64>;

  const double kFPRTolerance = 0.075;

  // Construct and verify properties of filters with various values of N, M, and layout
  //
  //----- --- -- -  -  -   -
  for (llfs::BloomFilterLayout layout : {
           llfs::BloomFilterLayout::kFlat,
           llfs::BloomFilterLayout::kBlocked64,
           llfs::BloomFilterLayout::kBlocked512,
       }) {
    //----- --- -- -  -  -   -
    for (const usize n_items : {1, 2, 12, 100, 500, 10 * 1000}) {
      constexpr usize kMaxBitsPerItem = 17;
      //----- --- -- -  -  -   -
      for (const std::vector<std::string>& all_items : input_sets) {
        //----- --- -- -  -  -   -
        for (double bits_per_key = 1; bits_per_key < kMaxBitsPerItem; bits_per_key += 0.37) {
          LLFS_VLOG(1) << "n=" << n_items << " m/n=" << bits_per_key << " layout=" << layout;

          BATT_CHECK_LE(n_items, all_items.size());
          batt::Slice<const std::string> items = batt::as_slice(all_items.data(), n_items);

          auto config = llfs::BloomFilterConfig::from(layout, llfs::ItemCount{n_items},
                                                      llfs::RealBitCount{bits_per_key});

          const double expected_fpr = config.false_positive_rate;

          bits_per_key = std::max<double>(bits_per_key, config.bits_per_key);

          std::unique_ptr<AlignedUnit[]> memory{new AlignedUnit[config.word_count() + 1]};

          auto* filter = (PackedBloomFilter*)memory.get();

          filter->initialize(config);

          EXPECT_EQ(filter->word_index_from_hash(u64{0}), 0u);
          EXPECT_EQ(filter->word_index_from_hash(~u64{0}), filter->word_count() - 1);

          for (usize divisor = 2; divisor < 23; ++divisor) {
            EXPECT_THAT((double)filter->word_index_from_hash(~u64{0} / divisor),
                        ::testing::DoubleNear(filter->word_count() / divisor, 1));
          }

          parallel_build_bloom_filter(
              WorkerPool::default_pool(), items.begin(), items.end(),
              /*get_key_fn=*/
              [](const std::string& s) -> std::string_view {
                return std::string_view{s};
              },
              filter);

          //----- --- -- -  -  -   -
          // Helper function; lookup the passed string in items, returning true if present.
          //
          const auto items_contains = [&items](const std::string_view& s) {
            auto iter = std::lower_bound(items.begin(), items.end(), s);
            return iter != items.end() && *iter == s;
          };
          //----- --- -- -  -  -   -

          for (const std::string& s : items) {
            llfs::BloomFilterQuery<std::string_view> query{s};

            EXPECT_TRUE(items_contains(s));
            EXPECT_TRUE(filter->might_contain(s));
            EXPECT_TRUE(filter->query(query));
          }

          double false_positive_count = 0, negative_query_count = 0;
          double actual_fpr = 0;

          for (const std::string& s : query_keys) {
            if (items_contains(s)) {
              continue;
            }

            llfs::BloomFilterQuery<std::string_view> query{s};
            const bool query_result = filter->might_contain(s);
            EXPECT_EQ(query_result, filter->query(query));

            negative_query_count += 1;
            if (query_result) {
              false_positive_count += 1;
            }
            actual_fpr = false_positive_count / negative_query_count;

            if (false_positive_count > 1000 &&
                std::abs(actual_fpr - expected_fpr) < kFPRTolerance) {
              break;
            }
            if (negative_query_count > 1000 * 1000 && false_positive_count == 0) {
              break;
            }
          }

          if (1.0 / expected_fpr > negative_query_count) {
            ASSERT_LE(actual_fpr, expected_fpr)
                << BATT_INSPECT(false_positive_count) << BATT_INSPECT(negative_query_count)
                << BATT_INSPECT(config) << filter->dump();
          } else {
            ASSERT_THAT(actual_fpr, ::testing::DoubleNear(expected_fpr, kFPRTolerance))
                << BATT_INSPECT(false_positive_count) << BATT_INSPECT(negative_query_count)
                << BATT_INSPECT(config) << filter->dump();
          }
        }  // bits_per_key
      }  // input_sets
    }  // n_items
  }  // layout
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BloomFilterConfigTest, FromOverloads)
{
  for (llfs::BloomFilterLayout layout : {
           llfs::BloomFilterLayout::kFlat,
           llfs::BloomFilterLayout::kBlocked64,
           llfs::BloomFilterLayout::kBlocked512,
       }) {
    const auto min_size = llfs::min_filter_size(layout);

    // from(layout, ItemCount, FalsePositiveRate)
    //
    {
      auto config = llfs::BloomFilterConfig::from(layout, llfs::ItemCount{1000},
                                                   llfs::FalsePositiveRate{0.01});

      EXPECT_EQ(config.layout, layout);
      EXPECT_GE(config.filter_size, min_size);
      EXPECT_GT(config.hash_count, llfs::HashCount{0});
      EXPECT_LT(config.bits_per_key, llfs::RealBitCount{0});
      EXPECT_EQ(config.false_positive_rate, llfs::FalsePositiveRate{0.01});
      if (layout == llfs::BloomFilterLayout::kBlocked512) {
        EXPECT_EQ(config.filter_size.value() % 8, 0u);
      }
    }

    // from(layout, Word64Count, RealBitCount)
    //
    {
      auto config = llfs::BloomFilterConfig::from(layout, llfs::Word64Count{64},
                                                   llfs::RealBitCount{10.0});

      EXPECT_EQ(config.layout, layout);
      EXPECT_GE(config.filter_size, min_size);
      EXPECT_LE(config.filter_size, llfs::Word64Count{64});
      EXPECT_GT(config.hash_count, llfs::HashCount{0});
      EXPECT_GT(config.key_count, llfs::ItemCount{0});
      EXPECT_GT(config.false_positive_rate, llfs::FalsePositiveRate{0});
      EXPECT_LT(config.false_positive_rate, llfs::FalsePositiveRate{1});
      if (layout == llfs::BloomFilterLayout::kBlocked512) {
        EXPECT_EQ(config.filter_size.value() % 8, 0u);
      }
    }

    // from(layout, Word64Count, HashCount)
    //
    {
      auto config =
          llfs::BloomFilterConfig::from(layout, llfs::Word64Count{64}, llfs::HashCount{7});

      EXPECT_EQ(config.layout, layout);
      EXPECT_GE(config.filter_size, min_size);
      EXPECT_LE(config.filter_size, llfs::Word64Count{64});
      EXPECT_EQ(config.hash_count, llfs::HashCount{7});
      EXPECT_GT(config.key_count, llfs::ItemCount{0});
      EXPECT_GT(config.bits_per_key, llfs::RealBitCount{0});
      EXPECT_GT(config.false_positive_rate, llfs::FalsePositiveRate{0});
      EXPECT_LT(config.false_positive_rate, llfs::FalsePositiveRate{1});
      if (layout == llfs::BloomFilterLayout::kBlocked512) {
        EXPECT_EQ(config.filter_size.value() % 8, 0u);
      }
    }

    // from(layout, Word64Count, ItemCount)
    //
    {
      auto config =
          llfs::BloomFilterConfig::from(layout, llfs::Word64Count{64}, llfs::ItemCount{500});

      EXPECT_EQ(config.layout, layout);
      EXPECT_GE(config.filter_size, min_size);
      EXPECT_LE(config.filter_size, llfs::Word64Count{64});
      EXPECT_EQ(config.key_count, llfs::ItemCount{500});
      EXPECT_GT(config.hash_count, llfs::HashCount{0});
      EXPECT_GT(config.bits_per_key, llfs::RealBitCount{0});
      EXPECT_GT(config.false_positive_rate, llfs::FalsePositiveRate{0});
      EXPECT_LT(config.false_positive_rate, llfs::FalsePositiveRate{1});
      if (layout == llfs::BloomFilterLayout::kBlocked512) {
        EXPECT_EQ(config.filter_size.value() % 8, 0u);
      }

      // operator<<(ostream, BloomFilterConfig)
      //
      std::ostringstream oss;
      oss << config;
      const std::string str = oss.str();

      EXPECT_THAT(str, ::testing::HasSubstr("BloomFilterConfig{"));
      EXPECT_THAT(str, ::testing::HasSubstr(".layout="));
      EXPECT_THAT(str, ::testing::HasSubstr(".filter_size="));
      EXPECT_THAT(str, ::testing::HasSubstr(".key_count="));
      EXPECT_THAT(str, ::testing::HasSubstr(".hash_count="));
      EXPECT_THAT(str, ::testing::HasSubstr(".bits_per_key="));
      EXPECT_THAT(str, ::testing::HasSubstr(".false_positive_rate="));
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BloomFilterTest, SequentialBuild)
{
  using AlignedUnit = std::aligned_storage_t<64, 64>;

  const std::vector<std::string> items = {"alpha", "bravo", "charlie", "delta", "echo"};

  auto config = llfs::BloomFilterConfig::from(llfs::BloomFilterLayout::kFlat,
                                               llfs::ItemCount{items.size()},
                                               llfs::RealBitCount{10.0});

  std::unique_ptr<AlignedUnit[]> memory{new AlignedUnit[config.word_count() + 1]};
  auto* filter = reinterpret_cast<PackedBloomFilter*>(memory.get());
  filter->initialize(config);

  parallel_build_bloom_filter(
      WorkerPool::null_pool(), items.begin(), items.end(),
      [](const std::string& s) -> std::string_view {
        return s;
      },
      filter);

  for (const std::string& s : items) {
    EXPECT_TRUE(filter->might_contain(std::string_view{s})) << "missing: " << s;
  }

  // packed_sizeof / packed_sizeof_bloom_filter
  //
  const usize expected_size = sizeof(PackedBloomFilter) + sizeof(llfs::little_u64) * filter->word_count();

  EXPECT_EQ(llfs::packed_sizeof(*filter), expected_size);
  EXPECT_EQ(llfs::packed_sizeof_bloom_filter(config), expected_size);
  EXPECT_EQ(llfs::packed_sizeof(*filter), llfs::packed_sizeof_bloom_filter(config));

  // filter_data_end / get_words / dump
  //
  EXPECT_EQ(filter->filter_data_end(), static_cast<const void*>(&filter->words[filter->word_count()]));
  EXPECT_EQ(filter->get_words().size(), filter->word_count());

  std::ostringstream dump_oss;
  dump_oss << filter->dump();
  EXPECT_FALSE(dump_oss.str().empty());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BloomFilterQueryTest, Operators)
{
  llfs::BloomFilterQuery<std::string_view> a{"hello"};
  llfs::BloomFilterQuery<std::string_view> b{"hello"};
  llfs::BloomFilterQuery<std::string_view> c{"world"};

  EXPECT_TRUE(a == b);
  EXPECT_FALSE(a != b);

  EXPECT_FALSE(a == c);
  EXPECT_TRUE(a != c);

  std::ostringstream oss;
  oss << a;
  const std::string str = oss.str();

  EXPECT_THAT(str, ::testing::HasSubstr("BloomFilterQuery{"));
  EXPECT_THAT(str, ::testing::HasSubstr(".key="));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BloomFilterDeathTest, WordIndexFromHashOutOfBounds)
{
  using AlignedUnit = std::aligned_storage_t<64, 64>;

  auto config = llfs::BloomFilterConfig::from(llfs::BloomFilterLayout::kFlat,
                                               llfs::ItemCount{100},
                                               llfs::RealBitCount{10.0});

  std::unique_ptr<AlignedUnit[]> memory{new AlignedUnit[config.word_count() + 1]};
  auto* filter = reinterpret_cast<PackedBloomFilter*>(memory.get());
  filter->initialize(config);

  filter->word_count_pre_mul_shift_ = 0;
  filter->word_count_post_mul_shift_ = 0;

  EXPECT_DEATH(filter->word_index_from_hash(~u64{0}), "Assertion failed:.*ans.*<.*word_count");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BloomFilterDeathTest, BlockIndexFromHashOutOfBounds)
{
  using AlignedUnit = std::aligned_storage_t<64, 64>;

  auto config = llfs::BloomFilterConfig::from(llfs::BloomFilterLayout::kBlocked512,
                                               llfs::ItemCount{100},
                                               llfs::RealBitCount{10.0});

  std::unique_ptr<AlignedUnit[]> memory{new AlignedUnit[config.word_count() + 1]};
  auto* filter = reinterpret_cast<PackedBloomFilter*>(memory.get());
  filter->initialize(config);

  filter->block_count_pre_mul_shift_ = 0;
  filter->block_count_post_mul_shift_ = 0;

  EXPECT_DEATH(filter->block_index_from_hash(~u64{0}), "Assertion failed:.*ans.*<.*block_count");
}

}  // namespace
