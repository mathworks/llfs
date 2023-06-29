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

/** \brief A node within an in-memory BPTrie.  This class is used internally by `BPTrie`.
 */
struct BPTrieNode {
  std::string_view prefix_;
  u8 pivot_ = 0;
  usize pivot_pos_ = 0;
  BPTrieNode* left_ = nullptr;
  BPTrieNode* right_ = nullptr;
};

/** \brief Builds a BPTrie subtree from the given range of keys (std::string_view objects).
 *
 * All new node objects are allocated at the end of the passed `nodes` vector.  This function calls
 * itself recursively; this is why it takes optional `current_prefix_len` and `is_right_subtree`
 * params.  Application code that just wants to turn a sorted range of strings into a BPTrie should
 * leave these params set to their implicit defaults (and probably should just use the BPTrie
 * constructor).
 */
template <typename Range>
BPTrieNode* make_trie(const Range& keys, std::vector<std::unique_ptr<BPTrieNode>>& nodes,
                      usize current_prefix_len = 0, bool is_right_subtree = false);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A binary prefix trie - implements an ordered set of strings.
 *
 * BPTrie is currently a _static_ set implementation; once it has been constructed from an initial
 * set of strings, it can not be modified.
 *
 * IMPORTANT: the string data passed to the BPTrie is *not* owned by the BPTrie itself; rather it is
 * stored as std::string_view objects internally.  That means that the creator of the BPTrie must
 * ensure that the underlying string data stays in scope for at least as long as the BPTrie object;
 * otherwise pointers will dangle!
 *
 * Unlike a traditional prefix trie data structure, whose nodes have a potentially very high
 * branching factor despite the density of keys, the binary prefix trie has a constant branching
 * factor of 2.  This is achieved via a `pivot` field on each node, which partitions the key ranges
 * of the left and right sub-trie.  It also supports key prefix compression via the prefix member.
 */
class BPTrie
{
 public:
  /** \brief Controls the order in which BPTrie nodes are packed.
   */
  enum struct PackedLayout {

    /** \brief Specifies that nodes should be packed in BFS order (i.e., the typical binary heap
     * ordering).
     */
    kBreadthFirst = 0,

    /** \brief Specifies that nodes should be packed in vEB order, which minimizes the average
     * distance between parent and child nodes, therefore optimizing for locality during key search
     * regardless of cache level/block-size (i.e., it is "Cache-Oblivious").
     */
    kVanEmdeBoas = 1,
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a BPTrie from a sorted, unique range of std::string_view keys.
   *
   * If the passed range is *not* sorted or the elements are not unique, behavior is undefined!
   *
   * IMPORTANT: the caller must ensure that all string data remains in-scope for the lifetime of
   * this object, as only string pointers (std::string_view) are stored within the BPTrie.
   */
  template <typename Range>
  explicit BPTrie(const Range& keys)
      : nodes_{}
      , root_{make_trie(keys, this->nodes_)}
      , size_{std::size(keys)}
  {
  }

  /** \brief BPTrie is a move-only type.
   */
  BPTrie(const BPTrie&) = delete;

  /** \brief BPTrie is a move-only type.
   */
  BPTrie& operator=(const BPTrie&) = delete;

  /** \brief BPTrie is a move-only type.
   */
  BPTrie(BPTrie&&) = default;

  /** \brief BPTrie is a move-only type.
   */
  BPTrie& operator=(BPTrie&&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the root node of the trie.
   */
  const BPTrieNode* root() const noexcept
  {
    return this->root_;
  }

  /** \brief Returns the number of nodes in the trie.
   */
  usize node_count() const noexcept
  {
    return this->nodes_.size();
  }

  /** \brief Returns the number of strings in the set.
   */
  usize size() const noexcept
  {
    return this->size_;
  }

  /** \brief Returns an interval of indices into the original range used to construct `this`; this
   * interval is the set of strings which are equal to the passed key.  If the returned interval is
   * non-empty, the key is found and the lower_bound is its position in the original set.  If the
   * returned interval is empty, then lower_bound is the place `key` would have been inserted in the
   * original range.
   */
  batt::Interval<usize> find(std::string_view key) const noexcept;

  /** \brief Changes the node order used to pack this object.  The BFS layout is still supported
   * mainly to be able to compare it to the more search-optimized vEB (default) layout, although
   * YMMV and there may be workloads for which BFS layout performs better.
   */
  void set_packed_layout(PackedLayout layout) noexcept
  {
    this->layout_ = layout;
  }

  /** \brief Returns the current node packing order.
   */
  PackedLayout get_packed_layout() const noexcept
  {
    return this->layout_;
  }

  /** \brief Recovers and returns the original key at the given index.
   *
   * The trie data structure, by its nature, does not store source strings (keys) directly; rather,
   * it stores the set as connected string fragments.  This function builds up the full string at a
   * given index, using the passed `buffer` as the underlying string storage.  The returned
   * std::string_view will point into `buffer`.
   */
  std::string_view get_key(usize index, batt::SmallVecBase<char>& buffer) const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::vector<std::unique_ptr<BPTrieNode>> nodes_;
  BPTrieNode* root_;
  usize size_ = 0;
  PackedLayout layout_ = PackedLayout::kVanEmdeBoas;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief The common layout for all packed trie nodes; internal use only.
 */
struct PackedBPTrieNodeBase {
  PackedBPTrieNodeBase(const PackedBPTrieNodeBase&) = delete;
  PackedBPTrieNodeBase& operator=(const PackedBPTrieNodeBase&) = delete;

  /** \brief Defines the node type and prefix length.
   *
   * The most significant bit, if set, indicates that this is a non-leaf (i.e. "parent") node.  The
   * lower 7 bits are the size of prefix_; if this size is less than the maximum (127), then this is
   * the final segment of the prefix string.  Otherwise, another PackedBPTrieNodeBase follows this
   * one, with a continuation of the prefix string data (even if it is zero-sized).
   *
   * The MSB must be set consistently (all 0x80 or all 0x00) on *all* such adjacent prefix segments
   * for the same node.
   */
  u8 header;

  /** \brief Prefix char data. See comment for PackedBPTrieNodeBase::header.
   */
  char prefix_[0];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedBPTrieNodeBase), 1);

/** \brief The packed layout of non-leaf trie nodes; this layout comes immediately after
 * `PackedBPTrieNodeBase` (plus any prefix_ bytes).
 */
template <typename PivotPos, typename SubtreeOffset>
struct PackedBPTrieNodeParent {
  /** \brief Partitions the left and right sub-tries; the least-ordered string in the right sub-trie
   * is the least-ordered string in the set that begins with `pivot`.
   */
  u8 pivot;

  /** \brief The index (within the sub-trie range) of the least-ordered string that begins with
   * `pivot`.
   */
  PivotPos pivot_pos;

  /** \brief Points to the left sub-trie root node.
   */
  PackedPointer<PackedBPTrieNodeBase, SubtreeOffset> left;

  /** \brief Points to the right sub-trie root node.
   */
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

/** \brief The packed representation of a binary prefix trie (BPTrie).
 */
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

}  //namespace llfs

#endif  // LLFS_TRIE_HPP

#include <llfs/trie.ipp>
