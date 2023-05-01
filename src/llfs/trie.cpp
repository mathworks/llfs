//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/trie.hpp>
//

#include <bitset>

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

namespace binary_prefix_trie {

struct Rec {
  usize id;
  usize priority;
  const BPTrieNode* node;
  PackedBPTrieNodePointer* p_pointer;
};

inline bool operator<(const Rec& l, const Rec& r)
{
  return l.priority < r.priority || (l.priority == r.priority && (l.id > r.id));
}

auto get_left_id(usize id) -> usize
{
  return id * 2;
};

auto get_right_id(usize id) -> usize
{
  return id * 2 + 1;
};

auto get_depth(usize id) -> usize
{
  return batt::log2_floor(id);
};

auto get_priority(usize parent_id, usize child_id) -> usize
{
  return __builtin_clz(get_depth(parent_id) ^ get_depth(child_id));
};

}  //namespace binary_prefix_trie

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBPTrieNode* pack_object_to(const BPTrieNode& root, PackedBPTrieNode* packed_root,
                                       llfs::DataPacker* dst)
{
  bool first = true;

  if (PackedBPTrieNode::use_van_emde_boas_layout()) {
    using binary_prefix_trie::get_depth;
    using binary_prefix_trie::get_left_id;
    using binary_prefix_trie::get_priority;
    using binary_prefix_trie::get_right_id;
    using binary_prefix_trie::Rec;

    std::vector<Rec> heap{Rec{
        .id = 1,
        .priority = 64,
        .node = &root,
        .p_pointer = nullptr,
    }};

    while (!heap.empty()) {
      std::pop_heap(heap.begin(), heap.end());

      Rec next = heap.back();
      heap.pop_back();

      auto node = next.node;
      auto p_pointer = next.p_pointer;

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

        const usize left_id = get_left_id(next.id);
        const usize left_priority = get_priority(next.id, left_id);

        heap.emplace_back(Rec{
            .id = left_id,
            .priority = left_priority,
            .node = left,
            .p_pointer = p_left_packed,
        });

        std::push_heap(heap.begin(), heap.end());
      }

      const BPTrieNode* right = node->right();

      if (right) {
        packed_node->flags |= PackedBPTrieNode::kHasRight;

        PackedBPTrieNodePointer* p_right_packed = dst->pack_record<PackedBPTrieNodePointer>();
        if (!p_right_packed) {
          return nullptr;
        }

        const usize right_id = get_right_id(next.id);
        const usize right_priority = get_priority(next.id, right_id);

        heap.emplace_back(Rec{
            .id = right_id,
            .priority = right_priority,
            .node = right,
            .p_pointer = p_right_packed,
        });

        std::push_heap(heap.begin(), heap.end());
      }

      if (!dst->pack_raw_data(prefix.data(), prefix_len)) {
        return nullptr;
      }

      if (p_pointer) {
        p_pointer->reset(packed_node, dst);
      }
    }

  } else {
    std::deque<std::pair<const BPTrieNode*, PackedBPTrieNodePointer*>> queue;

    queue.emplace_back(&root, nullptr);

    while (!queue.empty()) {
      const auto& [node, p_pointer] = queue.front();
      queue.pop_front();

      BATT_CHECK_NOT_NULLPTR(node);

      if (node->left_) {
        BATT_CHECK_NOT_NULLPTR(node->right_);
      } else {
        BATT_CHECK_EQ(node->right_, nullptr);
      }

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
  }
  return packed_root;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//

/*
new BPTrie layout:

first byte (hdr): ttssssss

ssssss = prefix len (6 bits, max 63)

tt = 00: leaf (terminal) node
         <hdr:1><prefix:*>
     01: prefix w/continuation node
         <hdr:1><prefix:*><hdr:1>...
     10: inner (parent) node, with 1-byte pivot_pos + child offset ptrs
         <hdr:1><pivot:1><pivot_pos:1><left_offset:1><right_offset:1><prefix:*>
     11: inner (parent) node, with 2-byte pivot_pos + child offset ptrs
         <hdr:1><pivot:1><pivot_pos:2><left_offset:2><right_offset:2><prefix:*>

 */

namespace binary_prefix_trie {

struct FastRec {
  usize id;
  usize priority;
  const BPTrieNode* node;
  PackedPointer<u8, little_u16>* p_pointer;
};

inline bool operator<(const FastRec& l, const FastRec& r)
{
  return l.priority < r.priority || (l.priority == r.priority && (l.id > r.id));
}

struct ParentLayout {
  ParentLayout(const ParentLayout&) = delete;
  ParentLayout& operator=(const ParentLayout&) = delete;

  little_u16 pivot_pos;
  PackedPointer<u8, little_u16> left;
  PackedPointer<u8, little_u16> right;
  u8 pivot;
};

BATT_STATIC_ASSERT_EQ(sizeof(ParentLayout), 7);

struct DstImpl {
  u8* begin_;
  u8* end_;

  u8* buffer_begin() const noexcept
  {
    return this->begin_;
  }

  u8* buffer_end() const noexcept
  {
    return this->end_;
  }

  bool contains(void* raw_ptr) const noexcept
  {
    u8* ptr = (u8*)raw_ptr;
    return ptr >= this->begin_ && ptr < this->end_;
  }
};

}  //namespace binary_prefix_trie

usize build_fast_packed_trie(const BPTrieNode* root, u8* const dst_begin, u8* const dst_end)
{
  using binary_prefix_trie::DstImpl;
  using binary_prefix_trie::FastRec;
  using binary_prefix_trie::get_depth;
  using binary_prefix_trie::get_left_id;
  using binary_prefix_trie::get_priority;
  using binary_prefix_trie::get_right_id;
  using binary_prefix_trie::ParentLayout;

  u8* dst = dst_begin;
  const DstImpl dst_impl{dst_begin, dst_end};

  std::vector<FastRec> heap{FastRec{
      .id = 1,
      .priority = 64,
      .node = root,
      .p_pointer = nullptr,
  }};

  while (!heap.empty()) {
    std::pop_heap(heap.begin(), heap.end());

    FastRec next = heap.back();
    heap.pop_back();

    auto node = next.node;
    auto p_pointer = next.p_pointer;

    if (p_pointer) {
      p_pointer->reset(dst, &dst_impl);
      BATT_CHECK_EQ(dst, p_pointer->get());
    }

    // Encode prefix.
    {
      const u8 mask = (node->left_) ? 0x80 : 0x00;

      const u8* prefix_data = (const u8*)node->prefix_.data();
      usize prefix_len = node->prefix_.size();
      while (prefix_len >= 127) {
        if (dst + 128 > dst_end) {
          return 0;
        }
        *dst = 0x7f | mask;
        ++dst;
        std::memcpy(dst, prefix_data, 127);
        prefix_data += 127;
        prefix_len -= 127;
        dst += 127;
      }

      if (dst + 1 > dst_end) {
        return 0;
      }
      *dst = (prefix_len & 0x7f) | mask;
      ++dst;

      if (prefix_len) {
        if (dst + prefix_len > dst_end) {
          return 0;
        }
        std::memcpy(dst, prefix_data, prefix_len);
        dst += prefix_len;
      }
    }

    if (!node->left_) {
      BATT_CHECK_EQ(node->right_, nullptr);

    } else {
      BATT_CHECK_NOT_NULLPTR(node->right_);

      if (dst + sizeof(ParentLayout) > dst_end) {
        return 0;
      }

      auto* parent = (ParentLayout*)dst;
      dst += sizeof(ParentLayout);

      parent->pivot = node->pivot_;
      parent->pivot_pos = BATT_CHECKED_CAST(u16, node->pivot_pos_);

      auto* p_left_packed = &parent->left;
      auto* p_right_packed = &parent->right;

      const usize left_id = get_left_id(next.id);
      const usize right_id = get_right_id(next.id);

      const usize left_priority = get_priority(next.id, left_id);
      const usize right_priority = get_priority(next.id, right_id);

      heap.emplace_back(FastRec{
          .id = left_id,
          .priority = left_priority,
          .node = node->left_,
          .p_pointer = p_left_packed,
      });

      std::push_heap(heap.begin(), heap.end());

      heap.emplace_back(FastRec{
          .id = right_id,
          .priority = right_priority,
          .node = node->right_,
          .p_pointer = p_right_packed,
      });

      std::push_heap(heap.begin(), heap.end());
    }
  }

  return dst - dst_begin;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 fast_search_trie(const u8* src, std::string_view key, batt::Interval<i64> range)
{
  using binary_prefix_trie::ParentLayout;

  for (;;) {
    u8 parent_mask = 0;

    for (;;) {
      const u8 hdr = *src;
      parent_mask = hdr & 0x80;
      const usize prefix_chunk_len = hdr & 0x7f;
      ++src;
      std::string_view prefix_chunk{(const char*)src, prefix_chunk_len};
      if (prefix_chunk_len != 0) {
        const usize key_len = key.size();
        const usize common_len = std::min(prefix_chunk_len, key_len);
        const batt::Order order = batt::compare(key.substr(0, common_len), prefix_chunk);
        if (order == batt::Order::Greater) {
          return range.upper_bound;
        }
        if (order == batt::Order::Less || key_len < prefix_chunk_len) {
          return range.lower_bound;
        }
        src += prefix_chunk_len;
        key = key.substr(prefix_chunk_len);
      }
      if (prefix_chunk_len < 127) {
        break;
      }
    }

    if (parent_mask == 0) {
      return range.lower_bound + 1;
    }

    auto* parent = (const ParentLayout*)src;
    const i64 middle = range.lower_bound + (usize)parent->pivot_pos;

    if (key.empty() || (u8)key[0] < parent->pivot) {
      src = parent->left.get();
      range.upper_bound = middle;
    } else {
      src = parent->right.get();
      range.lower_bound = middle;
    }
  }

  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_fast_trie_size(const BPTrieNode* root)
{
  using binary_prefix_trie::ParentLayout;

  const usize prefix_len = root->prefix_.size();
  usize size = prefix_len + (prefix_len + 128) / 127;
  if (root->left_) {
    BATT_CHECK_NOT_NULLPTR(root->right_);
    size += sizeof(ParentLayout);
    size += packed_fast_trie_size(root->left_);
    size += packed_fast_trie_size(root->right_);
  }
  return size;
}

}  //namespace llfs
