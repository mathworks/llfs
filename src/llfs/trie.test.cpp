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
      << batt::c_str_literal(t.prefix) << "/";

  out << batt::c_str_literal(std::string(1, t.pivot)) << "@" << t.pivot_pos;

  if (t.left || t.right) {
    std::string old_parent_prefix = parent_prefix;
    auto on_scope_exit2 = batt::finally([&] {
      parent_prefix = std::move(old_parent_prefix);
    });
    parent_prefix += t.prefix;

    out << std::endl << indent << "left: ";
    if (!t.left) {
      out << "--";
    } else {
      out << *t.left;
    }
    //----- --- -- -  -  -   -
    offset += t.pivot_pos;
    auto on_scope_exit3 = batt::finally([&] {
      offset -= t.pivot_pos;
    });
    //----- --- -- -  -  -   -
    out << std::endl << indent << "right: ";
    if (!t.right) {
      out << "--";
    } else {
      out << *t.right;
    }
  } else {
    BATT_CHECK_EQ(t.pivot, 0u);
    out << std::endl << indent << "index: " << offset;
  }

  return out;
}

std::string_view find_common_prefix(usize skip_len, const std::string_view& a,
                                    const std::string_view& b)
{
  usize len = std::min(a.size(), b.size());
  if (skip_len >= len) {
    return std::string_view{};
  }

  len -= skip_len;
  auto a_first = a.begin() + skip_len;
  auto b_first = b.begin() + skip_len;
  const auto [a_match_end, b_match_end] = std::mismatch(a_first, std::next(a_first, len), b_first);
  const usize prefix_len = std::distance(a_first, a_match_end);

  return std::string_view{a.data() + skip_len, prefix_len};
}

struct CompareKthByte {
  bool operator()(char ch, const std::string_view& str) const
  {
    if (this->k == str.size()) {
      return false;
    }
    return ((u8)ch < (u8)str[this->k]);
  }

  bool operator()(const std::string_view& str, char ch) const
  {
    if (this->k == str.size()) {
      return true;
    }
    return ((u8)str[this->k] < (u8)ch);
  }

  usize k;
};

template <typename Range>
BPTrieNode* make_trie(usize skip_len, const Range& keys,
                      std::vector<std::unique_ptr<BPTrieNode>>& nodes)
{
  thread_local int depth = 0;

  BATT_CHECK_LT(depth, 25);

  ++depth;
  auto on_scope_exit = batt::finally([&] {
    --depth;
  });

  auto first = std::begin(keys);
  auto last = std::end(keys);
  const usize count = std::distance(first, last);

  if (count == 0) {
    return nullptr;
  }

  auto new_node = std::make_unique<BPTrieNode>();
  auto* node = new_node.get();
  nodes.emplace_back(std::move(new_node));

  if (count == 1) {
    node->prefix = std::string_view{first->data() + skip_len, first->size() - skip_len};
    return node;
  }

  // Find the longest common prefix of the input key range.
  //
  const std::string& min_key = *first;
  const std::string& max_key = *std::prev(last);

  node->prefix = find_common_prefix(skip_len, min_key, max_key);

  // Find a pivot byte that best bisects the input range.
  //
  const usize k = node->prefix.size() + skip_len;

  const auto get_kth_byte = [&](usize i) {
    auto iter = std::next(first, i);
    if (iter->size() == k) {
      return '\0';
    }
    return (*iter)[k];
  };

  const usize middle_pos = count / 2;
  const auto middle_iter = std::next(first, middle_pos);
  const u8 middle_value = get_kth_byte(middle_pos);

  const auto lo_iter = std::lower_bound(first, middle_iter, middle_value, CompareKthByte{k});
  const auto hi_iter = std::upper_bound(middle_iter, last, middle_value, CompareKthByte{k});

  const usize lo_distance = std::distance(lo_iter, middle_iter);
  const usize hi_distance = std::distance(middle_iter, hi_iter);

  const auto pivot_iter = [&] {
    if (lo_distance < hi_distance) {
      return lo_iter;
    } else {
      return hi_iter;
    }
  }();

  node->pivot_pos = std::distance(first, pivot_iter);
  node->pivot = get_kth_byte(node->pivot_pos);

  node->left = make_trie(skip_len + node->prefix.size(),
                         boost::make_iterator_range(first, pivot_iter), nodes);

  node->right = make_trie(skip_len + node->prefix.size(),
                          boost::make_iterator_range(pivot_iter, last), nodes);

  return node;
}

i64 search_trie(const BPTrieNode* parent, std::string_view key, batt::Interval<i64> range)
{
  for (;;) {
    if (!parent) {
      return range.lower_bound + 1;
    }

    const usize prefix_len = parent->prefix.size();
    if (prefix_len != 0) {
      const usize key_len = key.size();
      const usize common_len = std::min(prefix_len, key_len);
      const batt::Order order = batt::compare(key.substr(0, common_len), parent->prefix);

      if (order == batt::Order::Greater) {
        return range.upper_bound;
      }
      if (order == batt::Order::Less || key_len < prefix_len) {
        return range.lower_bound;
      }
    }

    const i64 middle = range.lower_bound + parent->pivot_pos;

    key = key.substr(prefix_len);

    if ((u8)key[0] < (u8)parent->pivot) {
      parent = parent->left;
      range.upper_bound = middle;
    } else {
      parent = parent->right;
      range.lower_bound = middle;
    }
  }
}

[[maybe_unused]] i64 search_packed_trie(const PackedBPTrieNode* parent, std::string_view key,
                                        batt::Interval<i64> range)
{
  for (;;) {
    if (!parent) {
      return range.lower_bound + 1;
    }

    const usize prefix_len = parent->prefix().size();
    if (prefix_len != 0) {
      const usize key_len = key.size();
      const usize common_len = std::min(prefix_len, key_len);
      const batt::Order order = batt::compare(key.substr(0, common_len), parent->prefix());

      if (order == batt::Order::Greater) {
        return range.upper_bound;
      }
      if (order == batt::Order::Less || key_len < prefix_len) {
        return range.lower_bound;
      }
    }

    const i64 middle = range.lower_bound + parent->pivot_pos();

    key = key.substr(prefix_len);

    if ((u8)key[0] < (u8)parent->pivot()) {
      parent = parent->left();
      range.upper_bound = middle;
    } else {
      parent = parent->right();
      range.lower_bound = middle;
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

constexpr usize kSkip = 1000;
constexpr usize kStep = 0;
constexpr usize kTake = 100;

TEST(Trie, Test)
{
  auto words = load_words();

  LLFS_LOG_INFO() << BATT_INSPECT(words.size());

  for (const usize kTake : {10, 50, 80, 100, 200, 500, 1000, 2000, 5000}) {
    for (const usize kStep : {1, 2, 3, 10, 50, 100, 200, 500}) {
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
      auto* root = make_trie(0, sample, nodes);

      auto lookup = sample;
      lookup.emplace_back("~");
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
                                      return (p_node->prefix.size() == 0) ? 1 : 0;
                                    }) |
                                    batt::seq::sum();

      const usize prefix_1_count = batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
                                     return (p_node->prefix.size() == 1) ? 1 : 0;
                                   }) |
                                   batt::seq::sum();

      const usize prefix_2_count = batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
                                     return (p_node->prefix.size() == 2) ? 1 : 0;
                                   }) |
                                   batt::seq::sum();

      const usize short_prefix_count = batt::as_seq(nodes) | batt::seq::map([](const auto& p_node) {
                                         return (p_node->prefix.size() <= 2) ? 1 : 0;
                                       }) |
                                       batt::seq::sum();

      const usize one_byte_pivot_pos_count = batt::as_seq(nodes) |
                                             batt::seq::map([](const auto& p_node) {
                                               return (p_node->pivot_pos <= 127) ? 1 : 0;
                                             }) |
                                             batt::seq::sum();

      VLOG(1) << BATT_INSPECT(size) << BATT_INSPECT(input_size) << BATT_INSPECT(nodes.size())
              << BATT_INSPECT(no_prefix_count) << BATT_INSPECT(prefix_1_count)
              << BATT_INSPECT(prefix_2_count) << BATT_INSPECT(short_prefix_count)
              << BATT_INSPECT(one_byte_pivot_pos_count);

      std::cout << BATT_INSPECT(kStep) << BATT_INSPECT(kTake) << BATT_INSPECT(size)
                << BATT_INSPECT(input_size) << std::endl;

      EXPECT_LT(size, input_size + 100);

      for (usize i = 0; i < sample.size() * kStep; ++i) {
        std::string_view word = words[i + kSkip];
        i64 pos = search_trie(root, word, {-1, (i64)kTake - 1});
        EXPECT_GE(word, lookup[pos + 1]) << BATT_INSPECT(pos);
        EXPECT_LT(word, lookup[pos + 2]) << BATT_INSPECT(pos);

        i64 pos2 = search_packed_trie(packed_root, word, {-1, (i64)kTake - 1});
        EXPECT_EQ(pos, pos2);

        VLOG(1) << batt::c_str_literal(word) << " => " << pos << "  ["
                << batt::c_str_literal(lookup[pos + 1]) << ", "
                << batt::c_str_literal(lookup[pos + 2]) << ")";
      }
    }
  }
}

}  // namespace
