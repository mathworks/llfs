#include <llfs/trie.hpp>
//
#include <llfs/trie.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/data_packer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/logging.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/packed_seq.hpp>

#include <batteries/compare.hpp>
#include <batteries/interval.hpp>
#include <batteries/optional.hpp>
#include <batteries/segv.hpp>
#include <batteries/stream_util.hpp>

#include <boost/range/iterator_range.hpp>

#include <boost/endian/conversion.hpp>

#include <algorithm>
#include <fstream>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace llfs::int_types;

using llfs::BPTrie;
using llfs::PackedBPTrie;

std::vector<std::string> load_words()
{
  std::vector<std::string> words;
  std::ifstream ifs{"/usr/share/dict/words"};
  std::string word;
  while (ifs.good()) {
    ifs >> word;
    words.emplace_back(word);
  }
  std::sort(words.begin(), words.end());
  return words;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

constexpr usize kSkip = 1000;
constexpr usize kStep = 0;
constexpr usize kTake = 100;
constexpr usize kBenchmarkRepeat = 25;

using batt::Optional;

struct Trial {
  usize skip = 0;
  double step = 1.0;
  usize take = 100;
  Optional<usize> ss_table_size;
  Optional<usize> packed_trie_size;
};

struct SSTableWrapper {
  const std::vector<std::string>& items;

  batt::Interval<usize> find(const std::string_view& key) const
  {
    const auto first = items.begin();
    const auto& [lower, upper] = std::equal_range(first, items.end(), key);
    return {usize(lower - first), usize(upper - first)};
  }
};

struct PackedSSTableWrapper {
  const llfs::PackedArray<llfs::PackedBytes>& items;

  struct Compare {
    bool operator()(const std::string_view& l, const llfs::PackedBytes& r) const
    {
      return l < r.as_str();
    }
    bool operator()(const llfs::PackedBytes& l, const std::string_view& r) const
    {
      return l.as_str() < r;
    }
  };

  batt::Interval<usize> find(const std::string_view& key) const
  {
    const auto first = items.begin();
    const auto& [lower, upper] = std::equal_range(first, items.end(), key, Compare{});
    return {usize(lower - first), usize(upper - first)};
  }
};

TEST(Trie, Test)
{
  auto words = load_words();

  LLFS_LOG_INFO() << BATT_INSPECT(words.size());

  double trials = 0;
  double compression_total_bfs = 0;
  double compression_total_veb = 0;
  double speedup_total_mem_trie = 0;
  double speedup_total_mem_sstable = 0;
  double speedup_total_packed_bfs = 0;
  double speedup_total_packed_veb = 0;

  for (const usize kTake : {10, 50, 80, 100, 200, 500, 1000, 2000, 3000, 3100, 3200, 3300, 4000}) {
    for (const usize kStep :
         {1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 10, 16, 32, 50, 100, 200}) {
      std::vector<std::string> sample;
      {
        usize i = 0;
        for (const auto& word : words) {
          if (i > kSkip) {
            if ((i % kStep) == 0) {
              VLOG(1) << sample.size() << ": " << batt::c_str_literal(word);
              sample.emplace_back(word);
              if (sample.size() >= kTake) {
                break;
              }
            }
          }
          ++i;
        }
      }

      BPTrie trie{sample};

      // Pack the trie first in BFS order.
      //
      trie.set_packed_layout(BPTrie::PackedLayout::kBreadthFirst);
      const usize packed_size_bfs = llfs::packed_sizeof(trie);
      std::unique_ptr<u8[]> buffer_bfs{new u8[packed_size_bfs]};
      const PackedBPTrie* packed_bfs = nullptr;
      {
        llfs::DataPacker packer{llfs::MutableBuffer{buffer_bfs.get(), packed_size_bfs}};
        packed_bfs = llfs::pack_object(trie, &packer);

        ASSERT_NE(packed_bfs, nullptr);
      }

      // Pack the trie again using VEB order.
      //
      trie.set_packed_layout(BPTrie::PackedLayout::kVanEmdeBoas);
      const usize packed_size_veb = llfs::packed_sizeof(trie);
      std::unique_ptr<u8[]> buffer_veb{new u8[packed_size_veb]};
      const PackedBPTrie* packed_veb = nullptr;
      {
        llfs::DataPacker packer{llfs::MutableBuffer{buffer_veb.get(), packed_size_veb}};
        packed_veb = llfs::pack_object(trie, &packer);

        ASSERT_NE(packed_veb, nullptr);
      }

      // Finally, pack in SSTable layout.
      //
      const usize packed_size_sstable =
          sizeof(llfs::PackedArray<llfs::PackedBytes>) +
          (batt::as_seq(sample)                                      //
           | batt::seq::map(BATT_OVERLOADS_OF(llfs::packed_sizeof))  //
           | batt::seq::sum()                                        //
          );
      std::unique_ptr<u8[]> buffer_sstable{new u8[packed_size_sstable]};
      const llfs::PackedArray<llfs::PackedBytes>* packed_sstable = nullptr;
      {
        llfs::DataPacker packer{llfs::MutableBuffer{buffer_sstable.get(), packed_size_sstable}};
        packed_sstable = llfs::pack_object(batt::as_seq(sample) | batt::seq::boxed(), &packer);

        ASSERT_NE(packed_sstable, nullptr) << BATT_INSPECT(packed_size_sstable);
      }

      const double compression_bfs = double(packed_size_bfs) / double(packed_size_sstable);
      const double compression_veb = double(packed_size_veb) / double(packed_size_sstable);

      compression_total_bfs += compression_bfs;
      compression_total_veb += compression_veb;

      trials += 1;

      for (usize i = 0; i < sample.size() * kStep; ++i) {
        if (i + kSkip >= words.size()) {
          break;
        }
        std::string_view word = words[i + kSkip];
        const auto debug_info = [&](std::ostream& out) {
          out << BATT_INSPECT(i) << BATT_INSPECT(kSkip) << BATT_INSPECT(kStep)
              << " word == " << batt::c_str_literal(word)
              << batt::dump_range(sample, batt::Pretty::True);
        };
        batt::Interval<usize> pos = trie.find(word);

        EXPECT_LE(pos.lower_bound, pos.upper_bound);

        if (i > 0 && ((i + kSkip) % kStep) == 0) {
          EXPECT_EQ(pos.lower_bound + 1, pos.upper_bound)
              << BATT_INSPECT(pos) << BATT_INSPECT(i) << BATT_INSPECT(kSkip) << BATT_INSPECT(kStep)
              << BATT_INSPECT(word) << debug_info;
          EXPECT_EQ(sample[pos.lower_bound], word);
        }

        if (pos.upper_bound > pos.lower_bound) {
          ASSERT_GE(word, sample[pos.lower_bound]) << BATT_INSPECT(pos) << debug_info;
        }
        if (pos.upper_bound < sample.size()) {
          ASSERT_LT(word, sample[pos.upper_bound])
              << BATT_INSPECT(pos) << BATT_INSPECT(i) << BATT_INSPECT(sample.size()) << debug_info;
        }

        auto pos2 = packed_bfs->find(word);
        auto pos3 = packed_veb->find(word);

        EXPECT_EQ(pos, pos2) << debug_info;
        EXPECT_EQ(pos, pos3) << debug_info;
      }

      const auto run_timed_bench = [&sample, &words, &kStep](const auto& target) -> double {
        const auto start = std::chrono::steady_clock::now();

        usize checksum = 0;
        for (usize n = 0; n < kBenchmarkRepeat; ++n) {
          for (usize i = 0; i < sample.size() * kStep; ++i) {
            if (i + kSkip >= words.size()) {
              break;
            }
            std::string_view word = words[i + kSkip];
            batt::Interval<usize> pos = target.find(word);
            checksum += pos.lower_bound;
          }
        }

        i64 usec = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

        EXPECT_GT(checksum, 1u);

        return double(usec) / 1000000.0;
      };

      double mem_trie_time = run_timed_bench(trie);
      double mem_sstable_time = run_timed_bench(SSTableWrapper{sample});
      double packed_bfs_time = run_timed_bench(*packed_bfs);
      double packed_veb_time = run_timed_bench(*packed_veb);
      double packed_sstable_time = run_timed_bench(PackedSSTableWrapper{*packed_sstable});

      speedup_total_mem_trie += packed_sstable_time / mem_trie_time;
      speedup_total_mem_sstable += packed_sstable_time / mem_sstable_time;
      speedup_total_packed_bfs += packed_sstable_time / packed_bfs_time;
      speedup_total_packed_veb += packed_sstable_time / packed_veb_time;
    }
  }

  double avg_compression_bfs = (1.0 - compression_total_bfs / trials) * 100.0;
  double avg_compression_veb = (1.0 - compression_total_veb / trials) * 100.0;
  double avg_speedup_mem_trie = speedup_total_mem_trie / trials;
  double avg_speedup_mem_sstable = speedup_total_mem_sstable / trials;
  double avg_speedup_packed_bfs = speedup_total_packed_bfs / trials;
  double avg_speedup_packed_veb = speedup_total_packed_veb / trials;

  LOG(INFO) << BATT_INSPECT(avg_compression_bfs) << "%" << BATT_INSPECT(avg_compression_veb) << "%";
  LOG(INFO) << BATT_INSPECT(avg_speedup_mem_trie);
  LOG(INFO) << BATT_INSPECT(avg_speedup_mem_sstable);
  LOG(INFO) << BATT_INSPECT(avg_speedup_packed_bfs);
  LOG(INFO) << BATT_INSPECT(avg_speedup_packed_veb);
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

struct Rec {
  unsigned id;
  unsigned priority;
};

inline bool operator<(const Rec& l, const Rec& r)
{
  return l.priority < r.priority || (l.priority == r.priority && (l.id > r.id));
}

TEST(Trie, VEBLayoutTest)
{
  for (int max_depth = 1; max_depth <= 24; ++max_depth) {
    std::vector<unsigned> n((1 << max_depth) + 1);
    std::iota(n.begin(), n.end(), 1);

    //std::cerr << BATT_INSPECT_RANGE(n) << std::endl;

    const auto left = [&](unsigned id) -> unsigned {
      if (id > n.size() / 2)
        return 0;
      return id * 2;
    };

    const auto right = [&](unsigned id) -> unsigned {
      if (id > n.size() / 2)
        return 0;
      return id * 2 + 1;
    };

    const auto index_of = [](unsigned id) -> unsigned {
      return id - 1;
    };

    const auto depth = [](unsigned id) -> unsigned {
      return batt::log2_floor(id);
    };

    std::vector<unsigned> l(n.size()), r(n.size()), d(n.size());

    for (unsigned id : n) {
      l[index_of(id)] = left(id);
      r[index_of(id)] = right(id);
      d[index_of(id)] = depth(id);
    }

    //std::cerr << BATT_INSPECT_RANGE(l) << std::endl
    //        << BATT_INSPECT_RANGE(r) << std::endl
    //        << BATT_INSPECT_RANGE(d) << std::endl;

    std::vector<Rec> heap{Rec{1, 32}};
    std::vector<unsigned> layout;

    while (!heap.empty()) {
      std::pop_heap(heap.begin(), heap.end());

      Rec next = heap.back();
      heap.pop_back();

      layout.push_back(next.id);

      if (next.id <= n.size() / 2) {
        unsigned left_id = left(next.id);
        unsigned right_id = right(next.id);

        unsigned left_priority = __builtin_clz(depth(next.id) ^ depth(left_id));
        unsigned right_priority = __builtin_clz(depth(next.id) ^ depth(right_id));

        heap.emplace_back(Rec{left_id, left_priority});
        std::push_heap(heap.begin(), heap.end());

        heap.emplace_back(Rec{right_id, right_priority});
        std::push_heap(heap.begin(), heap.end());
      }
    }

    double total_dist_rlayout = 0;
    std::vector<unsigned> pos(n.size());
    for (unsigned i = 0; i < layout.size(); ++i) {
      unsigned id = layout[i];
      pos[index_of(id)] = i;
    }

    double n_seeds = 10.0;
    for (unsigned seed = 0; seed < unsigned(n_seeds); ++seed) {
      std::default_random_engine rng{seed};

      std::vector<unsigned> rlayout = n;
      std::shuffle(rlayout.begin(), rlayout.end(), rng);

      std::sort(rlayout.begin(), rlayout.end(), [&](unsigned l_id, unsigned r_id) {
        return depth(l_id) < depth(r_id);
      });

      if (max_depth == 4) {
        //std::cerr << BATT_INSPECT_RANGE(rlayout) << std::endl;
      }

      std::vector<unsigned> rpos(n.size());
      for (unsigned i = 0; i < rlayout.size(); ++i) {
        unsigned id = rlayout[i];
        rpos[index_of(id)] = i;
      }

      for (unsigned id : n) {
        if (id > n.size() / 2) {
          break;
        }
        total_dist_rlayout += rpos[index_of(left(id))] - rpos[index_of(id)];
        total_dist_rlayout += rpos[index_of(right(id))] - rpos[index_of(id)];
      }
    }

    //std::cerr << BATT_INSPECT_RANGE(layout) << std::endl;
    //std::cerr << BATT_INSPECT_RANGE(pos) << std::endl;

    std::array<usize, 32> heap_dist_log2, veb_dist_log2;
    heap_dist_log2.fill(0);
    veb_dist_log2.fill(0);

    double total_dist_heap_layout = 0;
    double total_dist_veb_layout = 0;
    for (unsigned id : n) {
      if (id > n.size() / 2) {
        break;
      }

      heap_dist_log2[batt::log2_ceil(index_of(left(id)) - index_of(id))] += 1;
      heap_dist_log2[batt::log2_ceil(index_of(right(id)) - index_of(id))] += 1;

      total_dist_heap_layout += index_of(left(id)) - index_of(id);
      total_dist_heap_layout += index_of(right(id)) - index_of(id);

      veb_dist_log2[batt::log2_ceil(pos[index_of(left(id))] - pos[index_of(id)])] += 1;
      veb_dist_log2[batt::log2_ceil(pos[index_of(right(id))] - pos[index_of(id)])] += 1;

      total_dist_veb_layout += pos[index_of(left(id))] - pos[index_of(id)];
      total_dist_veb_layout += pos[index_of(right(id))] - pos[index_of(id)];
    }

    const auto normalize_pct = [](auto& hist) {
      usize total = batt::as_seq(hist) | batt::seq::decayed() | batt::seq::sum();
      for (usize& n : hist) {
        n = (n * 100) / total;
      }
    };

    normalize_pct(heap_dist_log2);
    normalize_pct(veb_dist_log2);

    std::cerr << "(depth=" << max_depth
              << ") avg(heap)=" << total_dist_heap_layout / double(n.size() / 2) << "  "
              << "avg(veb)=" << total_dist_veb_layout / double(n.size() / 2) << "  "
              << "avg(rnd)=" << total_dist_rlayout / double(n.size() / 2 * n_seeds) << std::endl
              << "veb distribution=" << batt::dump_range(veb_dist_log2, batt::Pretty::False)
              << std::endl
              << "heap distribution=" << batt::dump_range(heap_dist_log2, batt::Pretty::False)
              << std::endl
              << std::endl;
  }
}

}  // namespace
