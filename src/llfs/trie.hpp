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

#include <batteries/static_dispatch.hpp>
#include <batteries/utility.hpp>

#include <deque>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace llfs {

struct BPTrieNode {
  const std::string_view& prefix() const noexcept
  {
    return this->prefix_;
  }

  u8 pivot() const noexcept
  {
    return this->pivot_;
  }

  i64 pivot_pos() const noexcept
  {
    return this->pivot_pos_;
  }

  const BPTrieNode* left() const noexcept
  {
    return this->left_;
  }

  const BPTrieNode* right() const noexcept
  {
    return this->right_;
  }

  template <typename Fn>
  void optimized_layout(Fn&& fn) const noexcept
  {
    fn(this);
  }

  //----- --- -- -  -  -   -

  std::string_view prefix_;
  u8 pivot_ = '\0';
  i64 pivot_pos_ = 0;
  BPTrieNode* left_ = nullptr;
  BPTrieNode* right_ = nullptr;
};

template <typename Range>
BPTrieNode* make_trie(const Range& keys, std::vector<std::unique_ptr<BPTrieNode>>& nodes,
                      usize current_prefix_len = 0);

/** \brief Finds the lower-bound position of key in the given trie.
 *
 * T may be wither BPTrieNode or PackedBPTrieNode.
 */
template <typename T>
i64 search_trie(const T* parent, std::string_view key, batt::Interval<i64> range);

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
//
// Layout:
//
// +-------+-------+-------+-------+---------------+---------------+---------------+----------
// + flag  | pivot? | pos1  | pos2? | prefix_len?   | left?         | right?        | prefix...
// +-------+-------+-------+-------+---------------+---------------+---------------+----------
//
struct PackedBPTrieNode {
  static constexpr u8 kHasLeft =  //
      0b00000001;
  static constexpr u8 kHasRight =  //
      0b00000010;
  static constexpr u8 kHasPivotPos16 =  //
      0b00000100;
  static constexpr u8 kHasPrefixLen8 =  //
      0b00001000;
  static constexpr u8 kHasPrefixLen16 =  //
      0b00010000;
  static constexpr u8 kLayoutMax =  //
      0b00011111;
  static constexpr u8 kPrefixLenMask =  //
      0b11111000;
  static constexpr u8 kPrefixLenTinyMax =  //
      0b11101000;
  static constexpr u8 kPrefixLenIsU8 =  //
      0b11110000;
  static constexpr u8 kPrefixLenIsU16 =  //
      0b11111000;
  static constexpr i32 kPrefixLenShift = 3;

  static std::atomic<bool>& use_van_emde_boas_layout()
  {
    static std::atomic<bool> value_{false};
    return value_;
  }

  template <u8 kLayoutFlags>
  struct DynamicLayout;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PackedBPTrieNode(const PackedBPTrieNode&) = delete;
  PackedBPTrieNode& operator=(const PackedBPTrieNode&) = delete;

  u8 flags;

  std::string_view prefix() const noexcept;

  usize pivot_pos() const noexcept;

  u8 pivot() const noexcept;

  usize prefix_len() const noexcept;

  const PackedBPTrieNode* left() const noexcept;

  const PackedBPTrieNode* right() const noexcept;

  template <typename R, typename Fn>
  R with_dynamic_layout(Fn&& fn) const noexcept;

  template <typename Fn>
  void optimized_layout(Fn&& fn) const noexcept
  {
    this->with_dynamic_layout<void>(BATT_FORWARD(fn));
  }
};

LLFS_DEFINE_PACKED_TYPE_FOR(BPTrieNode, PackedBPTrieNode);

using PackedBPTrieNodePointer = llfs::PackedPointer<PackedBPTrieNode, little_u16>;

/** \brief Calculate the size of the given sub-trie.
 */
usize packed_sizeof(const BPTrieNode& node);

/** \brief Pack the trie into its compact serialization.
 */
const PackedBPTrieNode* pack_object_to(const BPTrieNode& root, PackedBPTrieNode* packed_root,
                                       llfs::DataPacker* dst);

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace binary_prefix_trie {

//+++++++++++-+-+--+----- --- -- -  -  -   -

using Base = PackedBPTrieNode;

//+++++++++++-+-+--+----- --- -- -  -  -   -

template <bool kEnablePivot>
struct Pivot;

template <>
struct Pivot<false> : Base {
  u8 pivot() const noexcept
  {
    return 0;
  }
};

template <>
struct Pivot<true> : Base {
  u8 pivot() const noexcept
  {
    return this->pivot_;
  }

  u8 pivot_;
};

//+++++++++++-+-+--+----- --- -- -  -  -   -

template <bool kEnablePivot, bool kEnablePivotPos16>
struct PivotPos;

template <>
struct PivotPos<true, false> : Pivot<true> {
  usize pivot_pos() const noexcept
  {
    return this->pivot_pos_;
  }

  u8 pivot_pos_;
};

template <>
struct PivotPos<true, true> : Pivot<true> {
  usize pivot_pos() const noexcept
  {
    return this->pivot_pos_;
  }

  little_u16 pivot_pos_;
};

template <bool kEnablePivotPos16>
struct PivotPos<false, kEnablePivotPos16> : Pivot<false> {
  usize pivot_pos() const noexcept
  {
    return 0;
  }
};

//+++++++++++-+-+--+----- --- -- -  -  -   -

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits>
struct PrefixLen;

template <bool kEnablePivot, bool kEnablePivotPos16>
struct PrefixLen<kEnablePivot, kEnablePivotPos16, 5>  //
    : PivotPos<kEnablePivot, kEnablePivotPos16> {
  //
  usize prefix_len() const noexcept
  {
    return (this->flags & Base::kPrefixLenMask) >> Base::kPrefixLenShift;
  }
};

template <bool kEnablePivot, bool kEnablePivotPos16>
struct PrefixLen<kEnablePivot, kEnablePivotPos16, 8>  //
    : PivotPos<kEnablePivot, kEnablePivotPos16> {
  //
  usize prefix_len() const noexcept
  {
    return this->prefix_len_;
  }

  u8 prefix_len_;
};

template <bool kEnablePivot, bool kEnablePivotPos16>
struct PrefixLen<kEnablePivot, kEnablePivotPos16, 16>  //
    : PivotPos<kEnablePivot, kEnablePivotPos16> {
  //
  usize prefix_len() const noexcept
  {
    return this->prefix_len_;
  }

  little_u16 prefix_len_;
};

//+++++++++++-+-+--+----- --- -- -  -  -   -

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits, bool kEnableLeft>
struct Left;

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits>
struct Left<kEnablePivot, kEnablePivotPos16, kPrefixLenBits, false>
    : PrefixLen<kEnablePivot, kEnablePivotPos16, kPrefixLenBits> {
  //
  const PackedBPTrieNode* left() const noexcept
  {
    return nullptr;
  }
};

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits>
struct Left<kEnablePivot, kEnablePivotPos16, kPrefixLenBits, true>
    : PrefixLen<kEnablePivot, kEnablePivotPos16, kPrefixLenBits> {
  //
  const PackedBPTrieNode* left() const noexcept
  {
    return this->left_.get();
  }

  PackedBPTrieNodePointer left_;
};

//+++++++++++-+-+--+----- --- -- -  -  -   -

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits, bool kEnableLeft,
          bool kEnableRight>
struct Right;

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits, bool kEnableLeft>
struct Right<kEnablePivot, kEnablePivotPos16, kPrefixLenBits, kEnableLeft, false>
    : Left<kEnablePivot, kEnablePivotPos16, kPrefixLenBits, kEnableLeft> {
  //
  const PackedBPTrieNode* right() const noexcept
  {
    return nullptr;
  }
};

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits, bool kEnableLeft>
struct Right<kEnablePivot, kEnablePivotPos16, kPrefixLenBits, kEnableLeft, true>
    : Left<kEnablePivot, kEnablePivotPos16, kPrefixLenBits, kEnableLeft> {
  //
  const PackedBPTrieNode* right() const noexcept
  {
    return this->right_.get();
  }

  PackedBPTrieNodePointer right_;
};

//+++++++++++-+-+--+----- --- -- -  -  -   -

template <bool kEnablePivot, bool kEnablePivotPos16, int kPrefixLenBits, bool kEnableLeft,
          bool kEnableRight>
struct Prefix : Right<kEnablePivot, kEnablePivotPos16, kPrefixLenBits, kEnableLeft, kEnableRight> {
  //
  std::string_view prefix() const noexcept
  {
    return std::string_view{this->prefix_, this->prefix_len()};
  }

  char prefix_[1];
};

}  //namespace binary_prefix_trie

template <u8 kFlags>
struct PackedBPTrieNode::DynamicLayout
    : binary_prefix_trie::Prefix<
          /*kEnablePivot=*/(kFlags&(binary_prefix_trie::Base::kHasLeft |
                                    binary_prefix_trie::Base::kHasRight)) != 0,           //
          /*kEnablePivotPos16=*/(kFlags& binary_prefix_trie::Base::kHasPivotPos16) != 0,  //
          /*PrefixLenBits=*/
          ((kFlags & binary_prefix_trie::Base::kHasPrefixLen16) != 0)
              ? 16
              : (((kFlags & binary_prefix_trie::Base::kHasPrefixLen8) != 0) ? 8 : 5),
          //
          /*kEnableLeft=*/(kFlags& binary_prefix_trie::Base::kHasLeft) != 0,   //
          /*kEnableRight=*/(kFlags& binary_prefix_trie::Base::kHasRight) != 0  //
          > {
};

}  //namespace llfs

#include <llfs/trie.ipp>

#endif  // LLFS_TRIE_HPP
