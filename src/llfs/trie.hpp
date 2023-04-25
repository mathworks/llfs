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
#include <string_view>
#include <utility>

namespace llfs {

struct BPTrieNode {
  std::string_view prefix;
  u8 pivot = '\0';
  i64 pivot_pos = 0;
  BPTrieNode* left = nullptr;
  BPTrieNode* right = nullptr;
};

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
  static constexpr u8 kLayoutMask =  //
      0b00011111;
  static constexpr u8 kPrefixLenMask =  //
      0b11100000;
  static constexpr i32 kPrefixLenShift = 5;

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
};

LLFS_DEFINE_PACKED_TYPE_FOR(BPTrieNode, PackedBPTrieNode);

using PackedBPTrieNodePointer = llfs::PackedPointer<PackedBPTrieNode, little_u16>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize packed_sizeof(const BPTrieNode& node)
{
  usize size = sizeof(PackedBPTrieNode) + node.prefix.size();

  // pivot
  //
  if (node.left || node.right) {
    size += 1;

    // pivot_pos
    //
    if (node.pivot_pos >= 256) {
      size += 2;
    } else {
      size += 1;
    }
  }

  // prefix_len
  //
  if (node.prefix.size() >= 8) {
    if (node.prefix.size() >= 256) {
      size += 2;
    } else {
      size += 1;
    }
  }

  // left
  //
  if (node.left) {
    size += sizeof(PackedBPTrieNodePointer) + packed_sizeof(*node.left);
  }

  // right
  //
  if (node.right) {
    size += sizeof(PackedBPTrieNodePointer) + packed_sizeof(*node.right);
  }

  return size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const PackedBPTrieNode* pack_object_to(const BPTrieNode& root, PackedBPTrieNode* packed_root,
                                              llfs::DataPacker* dst)
{
  std::deque<std::pair<const BPTrieNode*, PackedBPTrieNodePointer*>> queue;

  queue.emplace_back(&root, nullptr);

  bool first = true;

  while (!queue.empty()) {
    const auto& [node, p_pointer] = queue.front();
    queue.pop_front();

    BATT_CHECK_NOT_NULLPTR(node);

    PackedBPTrieNode* packed_node = [&]() -> PackedBPTrieNode* {
      if (first) {
        first = false;
        return packed_root;
      }
      return dst->pack_record<PackedBPTrieNode>();
    }();

    if (!packed_node) {
      return nullptr;
    }

    packed_node->flags = 0;

    if (node->left || node->right) {
      if (!dst->pack_u8(node->pivot)) {
        return nullptr;
      }

      if (node->pivot_pos >= 256) {
        packed_node->flags |= PackedBPTrieNode::kHasPivotPos16;
        if (!dst->pack_u16(BATT_CHECKED_CAST(u16, node->pivot_pos))) {
          return nullptr;
        }
      } else {
        if (!dst->pack_u8(BATT_CHECKED_CAST(u8, node->pivot_pos))) {
          return nullptr;
        }
      }
    }

    if (node->prefix.size() < 8) {
      u8 prefix_len = node->prefix.size();
      packed_node->flags |=
          (prefix_len << PackedBPTrieNode::kPrefixLenShift) & PackedBPTrieNode::kPrefixLenMask;

    } else if (node->prefix.size() < 256) {
      packed_node->flags |= PackedBPTrieNode::kHasPrefixLen8;
      if (!dst->pack_u8(BATT_CHECKED_CAST(u8, node->prefix.size()))) {
        return nullptr;
      }

    } else {
      packed_node->flags |= PackedBPTrieNode::kHasPrefixLen16;
      if (!dst->pack_u16(BATT_CHECKED_CAST(u16, node->prefix.size()))) {
        return nullptr;
      }
    }

    if (node->left) {
      packed_node->flags |= PackedBPTrieNode::kHasLeft;

      PackedBPTrieNodePointer* p_left_packed = dst->pack_record<PackedBPTrieNodePointer>();
      if (!p_left_packed) {
        return nullptr;
      }
      queue.emplace_back(node->left, p_left_packed);
    }

    if (node->right) {
      packed_node->flags |= PackedBPTrieNode::kHasRight;

      PackedBPTrieNodePointer* p_right_packed = dst->pack_record<PackedBPTrieNodePointer>();
      if (!p_right_packed) {
        return nullptr;
      }
      queue.emplace_back(node->right, p_right_packed);
    }

    if (!dst->pack_raw_data(node->prefix.data(), node->prefix.size())) {
      return nullptr;
    }

    if (p_pointer) {
      p_pointer->reset(packed_node, dst);
    }
  }

  return packed_root;
}

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
struct PrefixLen<kEnablePivot, kEnablePivotPos16, 3>  //
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
              : (((kFlags & binary_prefix_trie::Base::kHasPrefixLen8) != 0) ? 8 : 3),
          //
          /*kEnableLeft=*/(kFlags& binary_prefix_trie::Base::kHasLeft) != 0,   //
          /*kEnableRight=*/(kFlags& binary_prefix_trie::Base::kHasRight) != 0  //
          > {
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
R PackedBPTrieNode::with_dynamic_layout(Fn&& fn) const noexcept
{
  return batt::static_dispatch<u8, 0, binary_prefix_trie::Base::kLayoutMask>(
      (this->flags & binary_prefix_trie::Base::kLayoutMask), [this, &fn](auto layout_flags) {
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

#endif  // LLFS_TRIE_HPP
