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

#include <batteries/compare.hpp>
#include <batteries/interval.hpp>
#include <batteries/optional.hpp>
#include <batteries/segv.hpp>
#include <batteries/stream_util.hpp>

#include <boost/range/iterator_range.hpp>

#include <boost/endian/conversion.hpp>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace llfs::int_types;

using llfs::BPTrieNode;
using llfs::PackedBPTrieNode;

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

inline std::ostream& operator<<(std::ostream& out, const BPTrieNode& t)
{
  thread_local usize offset = 0;
  thread_local std::string indent;
  thread_local std::string parent_prefix;

  indent += "| ";
  auto on_scope_exit = batt::finally([&] {
    indent.pop_back();
    indent.pop_back();
  });

  out << std::endl
      << indent << "node: " << batt::c_str_literal(parent_prefix) << "+"
      << batt::c_str_literal(t.prefix()) << "/";

  out << batt::c_str_literal(std::string(1, t.pivot())) << "@" << t.pivot_pos();

  if (t.left() || t.right()) {
    std::string old_parent_prefix = parent_prefix;
    auto on_scope_exit2 = batt::finally([&] {
      parent_prefix = std::move(old_parent_prefix);
    });
    parent_prefix += t.prefix();

    out << std::endl << indent << "left: ";
    if (!t.left()) {
      out << "--";
    } else {
      out << *t.left();
    }
    //----- --- -- -  -  -   -
    offset += t.pivot_pos();
    auto on_scope_exit3 = batt::finally([&] {
      offset -= t.pivot_pos();
    });
    //----- --- -- -  -  -   -
    out << std::endl << indent << "right: ";
    if (!t.right()) {
      out << "--";
    } else {
      out << *t.right();
    }
  } else {
    BATT_CHECK_EQ(t.pivot(), 0u);
    out << std::endl << indent << "index: " << offset;
  }

  return out;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

constexpr usize kSkip = 1000;
constexpr usize kStep = 0;
constexpr usize kTake = 100;
constexpr usize kBenchmarkRepeat = 25;

TEST(Trie, Test)
{
  auto words = load_words();

  LLFS_LOG_INFO() << BATT_INSPECT(words.size());

  double trials = 0;
  double compression_total = 0;
  double mem_speedup = 0;
  double packed_speedup = 0;

  for (const usize kTake : {10, 50, 80, 100, 200, 500, 1000, 2000, 5000}) {
    for (const usize kStep : {1, 2, 3, 4, 5, 6, 7, 8, 10, 16, 32, 50, 100, 200}) {
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

      std::vector<std::unique_ptr<BPTrieNode>> nodes;
      auto* root = llfs::make_trie(sample, nodes);

      auto lookup = sample;

      lookup.emplace_back("\xFF\xFF\xFF\xFF");
      lookup.insert(lookup.begin(), " ");

      const usize size = packed_sizeof(*root);

      std::unique_ptr<u8[]> buffer{new u8[size]};
      const PackedBPTrieNode* packed_root = nullptr;
      {
        llfs::DataPacker packer{llfs::MutableBuffer{buffer.get(), size}};
        packed_root = llfs::pack_object(*root, &packer);

        ASSERT_NE(packed_root, nullptr);
      }

      const usize input_size = (batt::as_seq(sample) | batt::seq::map([](const std::string& s) {
                                  return (s.size() <= 4) ? 0 : s.size();
                                }) |
                                batt::seq::sum()) +
                               sample.size() * sizeof(llfs::PackedBytes);

      const usize no_prefix_count = batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
                                      return (p_node->prefix().size() == 0) ? 1 : 0;
                                    }) |
                                    batt::seq::sum();

      const usize prefix_1_count = batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
                                     return (p_node->prefix().size() == 1) ? 1 : 0;
                                   }) |
                                   batt::seq::sum();

      const usize prefix_2_count = batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
                                     return (p_node->prefix().size() == 2) ? 1 : 0;
                                   }) |
                                   batt::seq::sum();

      const usize prefix_8_16_count =
          batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
            return (p_node->prefix().size() >= 8 && p_node->prefix().size() < 15) ? 1 : 0;
          }) |
          batt::seq::sum();

      const usize short_prefix_count = batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
                                         return (p_node->prefix().size() <= 2) ? 1 : 0;
                                       }) |
                                       batt::seq::sum();

      const usize one_byte_pivot_pos_count = batt::as_seq(nodes) |
                                             batt::seq::map([](const auto& p_node) {
                                               return (p_node->pivot_pos() <= 127) ? 1 : 0;
                                             }) |
                                             batt::seq::sum();

      const double compression = double(size) / double(input_size);

      VLOG(1) << BATT_INSPECT(kStep) << BATT_INSPECT(kTake) << BATT_INSPECT(size)
              << BATT_INSPECT(input_size) << BATT_INSPECT(compression) << BATT_INSPECT(nodes.size())
              << BATT_INSPECT(no_prefix_count) << BATT_INSPECT(prefix_1_count)
              << BATT_INSPECT(prefix_2_count) << BATT_INSPECT(short_prefix_count)
              << BATT_INSPECT(one_byte_pivot_pos_count) << BATT_INSPECT(prefix_8_16_count);

      compression_total += compression;
      trials += 1;

      batt::Interval<i64> search_range{-1, (i64)sample.size() - 1};

      i64 expect_checksum = 0;
      for (usize i = 0; i < sample.size() * kStep; ++i) {
        if (i + kSkip >= words.size()) {
          break;
        }
        std::string_view word = words[i + kSkip];
        i64 pos = llfs::search_trie(root, word, search_range);

        EXPECT_GE(word, lookup[pos + 1]) << BATT_INSPECT(pos);
        EXPECT_LT(word, lookup[pos + 2])
            << BATT_INSPECT(pos) << BATT_INSPECT(i) << BATT_INSPECT(lookup.size());

        i64 pos2 = llfs::search_trie(packed_root, word, search_range);

        EXPECT_EQ(pos, pos2) << BATT_INSPECT(pos2);

        VLOG(1) << batt::c_str_literal(word) << " => " << pos << "  ["
                << batt::c_str_literal(lookup[pos + 1]) << ", "
                << batt::c_str_literal(lookup[pos + 2]) << ")";

        expect_checksum += pos;
      }

      double in_mem_time = 0;
      {
        const auto start = std::chrono::steady_clock::now();

        i64 checksum = 0;
        for (usize n = 0; n < kBenchmarkRepeat; ++n) {
          for (usize i = 0; i < sample.size() * kStep; ++i) {
            if (i + kSkip >= words.size()) {
              break;
            }
            std::string_view word = words[i + kSkip];
            i64 pos = llfs::search_trie(root, word, search_range);
            checksum += pos;
          }
        }

        i64 usec = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

        in_mem_time = double(usec) / 1000000.0;

        VLOG(1) << "(In-Memory) Trie took " << in_mem_time << "s" << std::endl;

        EXPECT_EQ(checksum, i64(expect_checksum * kBenchmarkRepeat));
      }

      double packed_time = 0;
      {
        const auto start = std::chrono::steady_clock::now();

        i64 checksum = 0;
        for (usize n = 0; n < kBenchmarkRepeat; ++n) {
          for (usize i = 0; i < sample.size() * kStep; ++i) {
            if (i + kSkip >= words.size()) {
              break;
            }
            std::string_view word = words[i + kSkip];
            i64 pos = llfs::search_trie(packed_root, word, search_range);
            checksum += pos;
          }
        }

        i64 usec = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

        packed_time = double(usec) / 1000000.0;

        VLOG(1) << "(Packed) Trie took " << packed_time << "s" << std::endl;

        EXPECT_EQ(checksum, i64(expect_checksum * kBenchmarkRepeat));
      }

      double binsearch_time = 0;
      {
        const auto start = std::chrono::steady_clock::now();

        i64 checksum = 0;
        for (usize n = 0; n < kBenchmarkRepeat; ++n) {
          for (usize i = 0; i < sample.size() * kStep; ++i) {
            if (i + kSkip >= words.size()) {
              break;
            }
            std::string_view word = words[i + kSkip];
            i64 pos =
                std::distance(sample.begin(), std::lower_bound(sample.begin(), sample.end(), word));
            checksum += pos;
          }
        }

        i64 usec = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

        binsearch_time = double(usec) / 1000000.0;

        VLOG(1) << "Binary search took " << binsearch_time << "s" << std::endl;

        EXPECT_GE(checksum, i64(expect_checksum * kBenchmarkRepeat));
      }

      mem_speedup += binsearch_time / in_mem_time;
      packed_speedup += binsearch_time / packed_time;
    }
  }

  double avg_compression = compression_total / trials;
  double avg_speedup_mem = mem_speedup / trials;
  double avg_speedup_packed = packed_speedup / trials;
  LOG(INFO) << BATT_INSPECT(avg_compression) << BATT_INSPECT(avg_speedup_mem)
            << BATT_INSPECT(avg_speedup_packed);
}

}  // namespace
