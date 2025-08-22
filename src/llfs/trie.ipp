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
inline BPTrieNode* make_trie(const Range& keys, BPTrieNodeSet& node_set, usize current_prefix_len,
                             bool is_right_subtree)
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
  auto* node = node_set.new_node();

  // Base case 1: single key.
  //
  if (count == 1) {
    // Implement right-leaf optimization (the parent pivot is always prefix[0] in this case).
    //
    if (is_right_subtree) {
      current_prefix_len += 1;
    }

    node->prefix_ = std::string_view{first->data() + current_prefix_len,  //
                                     first->size() - current_prefix_len};

    node->subtree_node_count_ = 1;

    return node;
  }

  // Find the longest common prefix of the input key range.
  //
  const auto& min_key = *first;
  const auto& max_key = *std::prev(last);

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
  node->pivot_ = get_kth_byte(node->pivot_pos_);

  current_prefix_len = k;
  BATT_CHECK_NE(first, pivot_iter) << BATT_INSPECT(lo_distance) << BATT_INSPECT(hi_distance);
  BATT_CHECK_NE(last, pivot_iter) << BATT_INSPECT(lo_distance) << BATT_INSPECT(hi_distance)
                                  << BATT_INSPECT(middle_pos) << BATT_INSPECT(count)
                                  << batt::dump_range(boost::make_iterator_range(first, last))
                                  << BATT_INSPECT(current_prefix_len);

  node->left_ =
      make_trie(boost::make_iterator_range(first, pivot_iter), node_set, current_prefix_len, false);

  node->right_ =
      make_trie(boost::make_iterator_range(pivot_iter, last), node_set, current_prefix_len, true);

  node->subtree_node_count_ = 1 +  //
                              ((node->left_) ? node->left_->subtree_node_count_ : 0) +
                              ((node->right_) ? node->right_->subtree_node_count_ : 0);

  return node;
}

}  //namespace llfs

#endif  // LLFS_TRIE_IPP
