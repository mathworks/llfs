//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TRIE_HPP
#define LLFS_TRIE_HPP

#include <llfs/config.hpp>
//
#include <llfs/data_packer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_pointer.hpp>

#include <batteries/small_vec.hpp>
#include <batteries/static_dispatch.hpp>
#include <batteries/utility.hpp>

#include <deque>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace llfs {

struct BPTrieNode {
  std::string_view prefix_;
  u8 pivot_ = 0;
  usize pivot_pos_ = 0;
  BPTrieNode* left_ = nullptr;
  BPTrieNode* right_ = nullptr;
};

template <typename Range>
BPTrieNode* make_trie(const Range& keys, std::vector<std::unique_ptr<BPTrieNode>>& nodes,
                      usize current_prefix_len = 0, bool is_right_subtree = false);

class BPTrie
{
 public:
  enum struct PackedLayout {
    kBreadthFirst = 0,
    kVanEmdeBoas = 1,
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename Range>
  explicit BPTrie(const Range& keys)
      : nodes_{}
      , root_{make_trie(keys, this->nodes_)}
      , size_{std::size(keys)}
  {
  }

  const BPTrieNode* root() const noexcept
  {
    return this->root_;
  }

  usize node_count() const noexcept
  {
    return this->nodes_.size();
  }

  usize size() const noexcept
  {
    return this->size_;
  }

  batt::Interval<usize> find(std::string_view key) const noexcept;

  void set_packed_layout(PackedLayout layout) noexcept
  {
    this->layout_ = layout;
  }

  PackedLayout get_packed_layout() const noexcept
  {
    return this->layout_;
  }

  std::string_view get_key(usize index, batt::SmallVecBase<char>& buffer) const noexcept;

 private:
  std::vector<std::unique_ptr<BPTrieNode>> nodes_;
  BPTrieNode* root_;
  usize size_ = 0;
  PackedLayout layout_ = PackedLayout::kVanEmdeBoas;
};

/** \brief
 */
usize build_fast_packed_trie(const BPTrieNode* root, u8* const dst_begin, u8* const dst_end);

/** \brief
 */
i64 fast_search_trie(const u8* src, std::string_view key, batt::Interval<i64> range);

/** \brief
 */
usize packed_fast_trie_size(const BPTrieNode* root);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PackedBPTrieNodeBase {
  PackedBPTrieNodeBase(const PackedBPTrieNodeBase&) = delete;
  PackedBPTrieNodeBase& operator=(const PackedBPTrieNodeBase&) = delete;

  u8 header;
  char prefix_[0];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeBase), 1);

template <typename PivotPos, typename SubtreeOffset>
struct PackedBPTrieNodeParent {
  u8 pivot;
  PivotPos pivot_pos;
  PackedPointer<PackedBPTrieNodeBase, SubtreeOffset> left;
  PackedPointer<PackedBPTrieNodeBase, SubtreeOffset> right;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPointer<PackedBPTrieNodeBase, u8>), 1);
BATT_STATIC_ASSERT_EQ(sizeof(PackedPointer<PackedBPTrieNodeBase, little_u16>), 2);
BATT_STATIC_ASSERT_EQ(sizeof(PackedPointer<PackedBPTrieNodeBase, little_u24>), 3);
BATT_STATIC_ASSERT_EQ(sizeof(PackedPointer<PackedBPTrieNodeBase, little_u32>), 4);

BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<u8, u8>), 1 + 1 + 1 + 1);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<u8, little_u16>), 1 + 1 + 2 + 2);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<u8, little_u24>), 1 + 1 + 3 + 3);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<u8, little_u32>), 1 + 1 + 4 + 4);

BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u16, u8>), 1 + 2 + 1 + 1);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u16, little_u16>), 1 + 2 + 2 + 2);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u16, little_u24>), 1 + 2 + 3 + 3);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u16, little_u32>), 1 + 2 + 4 + 4);

BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u24, u8>), 1 + 3 + 1 + 1);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u24, little_u16>), 1 + 3 + 2 + 2);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u24, little_u24>), 1 + 3 + 3 + 3);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u24, little_u32>), 1 + 3 + 4 + 4);

BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u32, u8>), 1 + 4 + 1 + 1);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u32, little_u16>), 1 + 4 + 2 + 2);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u32, little_u24>), 1 + 4 + 3 + 3);
BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeParent<little_u32, little_u32>), 1 + 4 + 4 + 4);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PackedBPTrie {
  static constexpr usize kMaxPrefixChunkLen = 127;
  static constexpr u8 kPrefixChunkLenMask = 0x7f;
  static constexpr u8 kParentNodeMask = 0x80;

  static constexpr u8 kOffset8 = 0;
  static constexpr u8 kOffset16 = 1;
  static constexpr u8 kOffset24 = 2;
  static constexpr u8 kOffset32 = 3;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  little_u64 size_;
  u8 offset_kind_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize size() const noexcept
  {
    return this->size_;
  }

  const PackedBPTrieNodeBase* root() const noexcept
  {
    return reinterpret_cast<const PackedBPTrieNodeBase*>(this + 1);
  }

  batt::Interval<usize> find(std::string_view key) const noexcept;

  std::string_view get_key(usize index, batt::SmallVecBase<char>& buffer) const noexcept;
};

LLFS_DEFINE_PACKED_TYPE_FOR(BPTrie, PackedBPTrie);

/** \brief Calculate the size of the given sub-trie.
 */
usize packed_sizeof(const BPTrie& node);

/** \brief Pack the trie into its compact serialization.
 */
const PackedBPTrie* pack_object_to(const BPTrie& object, PackedBPTrie* packed,
                                   llfs::DataPacker* dst);

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace binary_prefix_trie {

}  //namespace binary_prefix_trie

}  //namespace llfs

#include <llfs/trie.ipp>

#endif  // LLFS_TRIE_HPP
