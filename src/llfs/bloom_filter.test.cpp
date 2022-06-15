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

#include <random>
#include <sstream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using turtle_db::LatencyMetric;
using turtle_db::LatencyTimer;

using llfs::as_slice;
using llfs::BloomFilterParams;
using llfs::packed_sizeof_bloom_filter;
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

struct QueryStats {
  usize total = 0;
  usize false_positive = 0;
  double expected_rate = 0;

  double actual_rate() const
  {
    return double(this->false_positive) / double(this->total);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

TEST(BloomFilterTest, RandomItems)
{
  std::default_random_engine rng{15324987};

  std::map<std::pair<u64, u16>, QueryStats> stats;

  LatencyMetric build_latency;
  LatencyMetric query_latency;

  for (usize r = 0; r < 10; ++r) {
    std::vector<std::string> items;
    const usize n_items = 10 * 1000;
    for (usize i = 0; i < n_items; ++i) {
      items.emplace_back(make_random_word(rng));
    }
    for (const auto& s : as_slice(items.data(), 40)) {
      VLOG(1) << "sample random word: " << s;
    }
    std::sort(items.begin(), items.end());

    for (usize bits_per_item = 1; bits_per_item < 16; ++bits_per_item) {
      const BloomFilterParams params{
          .bits_per_item = bits_per_item,
      };

      std::unique_ptr<u8[]> memory{new u8[packed_sizeof_bloom_filter(params, items.size())]};
      PackedBloomFilter* filter = (PackedBloomFilter*)memory.get();
      *filter = PackedBloomFilter::from_params(params, items.size());

      const double actual_bit_rate = double(filter->word_count() * 64) / double(items.size());

      VLOG(1) << BATT_INSPECT(n_items) << " (target)" << BATT_INSPECT(bits_per_item)
              << BATT_INSPECT(filter->word_count_mask) << BATT_INSPECT(filter->hash_count)
              << " bit_rate == " << actual_bit_rate;

      {
        LatencyTimer build_timer{build_latency, items.size()};

        parallel_build_bloom_filter(
            WorkerPool::default_pool(), items.begin(), items.end(),
            [](const auto& v) -> decltype(auto) {
              return v;
            },
            filter);
      }

      for (const std::string& s : items) {
        EXPECT_TRUE(filter->might_contain(s));
      }

      const auto items_contains = [&items](const std::string& s) {
        auto iter = std::lower_bound(items.begin(), items.end(), s);
        return iter != items.end() && *iter == s;
      };

      std::pair<u64, u16> config_key{filter->word_count(), filter->hash_count};
      QueryStats& c_stats = stats[config_key];
      {
        const double k = filter->hash_count;
        const double n = items.size();
        const double m = filter->word_count() * 64;

        c_stats.expected_rate = std::pow(1 - std::exp(-((k * (n + 0.5)) / (m - 1))), k);
      }

      for (usize j = 0; j < n_items * 10; ++j) {
        std::string query = make_random_word(rng);
        c_stats.total += 1;
        const bool ans = LLFS_COLLECT_LATENCY_N(query_latency, filter->might_contain(query),
                                                u64(std::ceil(actual_bit_rate)));
        if (ans && !items_contains(query)) {
          c_stats.false_positive += 1;
        }
      }
    }
  }

  for (const auto& s : stats) {
    EXPECT_LT(s.second.actual_rate() / s.second.expected_rate, 1.02)
        << BATT_INSPECT(s.second.actual_rate()) << BATT_INSPECT(s.second.expected_rate);
  }

  LOG(INFO) << "build latency (per key) == " << build_latency
            << " build rate (keys/sec) == " << build_latency.rate_per_second();

  LOG(INFO) << "normalized query latency (per key*bit) == " << query_latency
            << " query rate (key*bits/sec) == " << query_latency.rate_per_second();
}

}  // namespace
