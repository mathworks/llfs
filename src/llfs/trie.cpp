//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/trie.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const BPTrieNode& node)
{
  usize size = sizeof(PackedBPTrieNode) + node.prefix().size();

  // pivot
  //
  if (node.left() || node.right()) {
    size += 1;

    // pivot_pos
    //
    if (node.pivot_pos() >= 256) {
      size += 2;
    } else {
      size += 1;
    }
  }

  // prefix_len
  //
  if (node.prefix().size() > PackedBPTrieNode::kPrefixLenTinyMax) {
    if (node.prefix().size() >= 256) {
      size += 2;
    } else {
      size += 1;
    }
  }

  // left
  //
  if (node.left()) {
    size += sizeof(PackedBPTrieNodePointer) + packed_sizeof(*node.left());
  }

  // right
  //
  if (node.right()) {
    size += sizeof(PackedBPTrieNodePointer) + packed_sizeof(*node.right());
  }

  return size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBPTrieNode* pack_object_to(const BPTrieNode& root, PackedBPTrieNode* packed_root,
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

    if (node->left() || node->right()) {
      if (!dst->pack_u8(node->pivot())) {
        return nullptr;
      }

      const i64 pivot_pos = node->pivot_pos();

      if (pivot_pos >= 256) {
        packed_node->flags |= PackedBPTrieNode::kHasPivotPos16;
        if (!dst->pack_u16(BATT_CHECKED_CAST(u16, pivot_pos))) {
          return nullptr;
        }
      } else {
        if (!dst->pack_u8(BATT_CHECKED_CAST(u8, pivot_pos))) {
          return nullptr;
        }
      }
    }

    const std::string_view& prefix = node->prefix();
    const usize prefix_len = prefix.size();

    if (prefix_len <= PackedBPTrieNode::kPrefixLenTinyMax) {
      packed_node->flags |=
          (prefix_len << PackedBPTrieNode::kPrefixLenShift) & PackedBPTrieNode::kPrefixLenMask;

      BATT_CHECK_EQ(prefix_len, packed_node->prefix_len());

    } else if (prefix_len < 256) {
      packed_node->flags |= PackedBPTrieNode::kPrefixLenIsU8;
      if (!dst->pack_u8(BATT_CHECKED_CAST(u8, prefix_len))) {
        return nullptr;
      }

    } else {
      packed_node->flags |= PackedBPTrieNode::kPrefixLenIsU16;
      if (!dst->pack_u16(BATT_CHECKED_CAST(u16, prefix_len))) {
        return nullptr;
      }
    }

    const BPTrieNode* left = node->left();

    if (left) {
      packed_node->flags |= PackedBPTrieNode::kHasLeft;

      PackedBPTrieNodePointer* p_left_packed = dst->pack_record<PackedBPTrieNodePointer>();
      if (!p_left_packed) {
        return nullptr;
      }
      queue.emplace_back(left, p_left_packed);
    }

    const BPTrieNode* right = node->right();

    if (right) {
      packed_node->flags |= PackedBPTrieNode::kHasRight;

      PackedBPTrieNodePointer* p_right_packed = dst->pack_record<PackedBPTrieNodePointer>();
      if (!p_right_packed) {
        return nullptr;
      }
      queue.emplace_back(right, p_right_packed);
    }

    if (!dst->pack_raw_data(prefix.data(), prefix_len)) {
      return nullptr;
    }

    if (p_pointer) {
      p_pointer->reset(packed_node, dst);
    }
  }

  return packed_root;
}

}  //namespace llfs
