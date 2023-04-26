//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TRIE_IPP
#define LLFS_TRIE_IPP

#include <llfs/config.hpp>
//

#include <llfs/strings.hpp>

#include <batteries/assert.hpp>
#include <batteries/compare.hpp>

namespace llfs {

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Range>
inline BPTrieNode* make_trie(const Range& keys, std::vector<std::unique_ptr<BPTrieNode>>& nodes,
                             usize current_prefix_len)
{
  // Guard against too much recursion.
  //
  thread_local int depth = 0;

  BATT_CHECK_LT(depth, 48);

  ++depth;
  auto on_scope_exit = batt::finally([&] {
    --depth;
  });

  // Grab the iterator pair and find the input range size.
  //
  auto first = std::begin(keys);
  auto last = std::end(keys);
  const usize count = std::distance(first, last);

  // Base case 0: empty input.
  //
  if (count == 0) {
    return nullptr;
  }

  // We know we are going to create at least one node.
  //
  auto new_node = std::make_unique<BPTrieNode>();
  auto* node = new_node.get();
  nodes.emplace_back(std::move(new_node));

  // Base case 1: single key.
  //
  if (count == 1) {
    node->prefix_ = std::string_view{first->data() + current_prefix_len,  //
                                     first->size() - current_prefix_len};
    return node;
  }

  // Find the longest common prefix of the input key range.
  //
  const std::string& min_key = *first;
  const std::string& max_key = *std::prev(last);

  node->prefix_ = find_common_prefix(current_prefix_len, min_key, max_key);

  // This is the first position at which the inputs differ along this branch of the trie.
  //
  const usize k = node->prefix_.size() + current_prefix_len;

  // Helper: given a key index, return the first byte in that key *not* in the prefix we calculated
  // above.
  //
  const auto get_kth_byte = [&](usize i) {
    auto iter = std::next(first, i);
    if (iter->size() == k) {
      return '\0';
    }
    return (*iter)[k];
  };

  // Find a pivot byte that best bisects the input range.
  //
  const usize middle_pos = count / 2;
  const auto middle_iter = std::next(first, middle_pos);
  const u8 middle_value = get_kth_byte(middle_pos);

  // Binary-search the entire input to find the subrange containing the median element.
  //
  const auto [lo_iter, hi_iter] = std::equal_range(first, last, middle_value, CompareKthByte{k});

  // We want to take whichever bound is closer to the middle of the input, so calculate that
  // distance now.
  //
  const usize lo_distance = std::distance(lo_iter, middle_iter);
  const usize hi_distance = std::distance(middle_iter, hi_iter);

  const auto pivot_iter = [&] {
    if (lo_distance < hi_distance) {
      return lo_iter;
    } else {
      return hi_iter;
    }
  }();

  // We have our pivot!  Subdivide the input range and recurse down left (lower) and right (upper)
  // halves.
  //
  node->pivot_pos_ = std::distance(first, pivot_iter);
  node->pivot_ = get_kth_byte(node->pivot_pos());

  current_prefix_len = k;

  node->left_ = make_trie(boost::make_iterator_range(first, pivot_iter), nodes, current_prefix_len);
  node->right_ = make_trie(boost::make_iterator_range(pivot_iter, last), nodes, current_prefix_len);

  return node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline i64 search_trie(const T* parent, std::string_view key, batt::Interval<i64> range)
{
  i64 ans = 0;
  bool done = false;
  while (!done) {
    if (!parent) {
      return range.lower_bound + 1;
    }
    parent->optimized_layout([&](auto* node) {
      const std::string_view& prefix = node->prefix();
      const usize prefix_len = prefix.size();
      if (prefix_len != 0) {
        const usize key_len = key.size();
        const usize common_len = std::min(prefix_len, key_len);
        const batt::Order order = batt::compare(key.substr(0, common_len), prefix);

        if (order == batt::Order::Greater) {
          ans = range.upper_bound;
          done = true;
          return;
        }
        if (order == batt::Order::Less || key_len < prefix_len) {
          ans = range.lower_bound;
          done = true;
          return;
        }
      }

      const i64 middle = range.lower_bound + node->pivot_pos();

      key = key.substr(prefix_len);

      if (key.empty() || (u8)key[0] < (u8)node->pivot()) {
        parent = node->left();
        range.upper_bound = middle;
      } else {
        parent = node->right();
        range.lower_bound = middle;
      }
    });
  }
  return ans;
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
R PackedBPTrieNode::with_dynamic_layout(Fn&& fn) const noexcept
{
  u8 layout = (this->flags & 0b111) | ([&]() -> u8 {
                const u8 prefix_len = this->flags & kPrefixLenMask;
                switch (prefix_len) {
                  case kPrefixLenIsU8:
                    return kHasPrefixLen8;

                  case kPrefixLenIsU16:
                    return kHasPrefixLen16;

                  default:
                    return 0;
                }
              }());

  return batt::static_dispatch<u8, 0, binary_prefix_trie::Base::kLayoutMax>(
      layout, [this, &fn](auto layout_flags) {
        return BATT_FORWARD(fn)(
            reinterpret_cast<const DynamicLayout<decltype(layout_flags)::value>*>(this));
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize PackedBPTrieNode::prefix_len() const noexcept
{
  return this->with_dynamic_layout<usize>([](auto* layout) {
    return layout->prefix_len();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize PackedBPTrieNode::pivot_pos() const noexcept
{
  return this->with_dynamic_layout<usize>([](auto* layout) {
    return layout->pivot_pos();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline u8 PackedBPTrieNode::pivot() const noexcept
{
  return this->with_dynamic_layout<u8>([](auto* layout) {
    return layout->pivot();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline std::string_view PackedBPTrieNode::prefix() const noexcept
{
  return this->with_dynamic_layout<std::string_view>([this](auto* layout) {
    return layout->prefix();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const PackedBPTrieNode* PackedBPTrieNode::left() const noexcept
{
  return this->with_dynamic_layout<const PackedBPTrieNode*>([this](auto* layout) {
    return layout->left();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const PackedBPTrieNode* PackedBPTrieNode::right() const noexcept
{
  return this->with_dynamic_layout<const PackedBPTrieNode*>([this](auto* layout) {
    return layout->right();
  });
}

}  //namespace llfs

#endif  // LLFS_TRIE_IPP
