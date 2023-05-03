//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/trie.hpp>
//

#include <llfs/traversal_order.hpp>

#include <bitset>

namespace llfs {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SubtreeOffset>
usize packed_sizeof_bp_trie_node(const BPTrieNode* node, batt::Interval<usize> range, i64& space)
{
  if (!node) {
    return 0;
  }

  const usize prefix_len = node->prefix_.size();

  usize node_size = prefix_len + (prefix_len + PackedBPTrie::kMaxPrefixChunkLen + 1) /
                                     PackedBPTrie::kMaxPrefixChunkLen;

  usize left_subtree_size = 0;
  usize right_subtree_size = 0;

  if (node->left_) {
    BATT_CHECK_NOT_NULLPTR(node->right_);

    // pivot (always u8)
    //
    node_size += 1;

    // pivot_pos
    //
    const usize pivot_pos_size = batt::log2_ceil((usize)range.size()) / 8 + 1;
    BATT_CHECK_LT(range.size(), 1 << (pivot_pos_size * 8));
    node_size += pivot_pos_size;

    //  left, right pointers
    //
    node_size += sizeof(PackedPointer<PackedBPTrieNodeBase, SubtreeOffset>) * 2;

    space -= (i64)node_size;
    if (space > 0) {
      const usize middle = range.lower_bound + node->pivot_pos_;

      left_subtree_size = packed_sizeof_bp_trie_node<SubtreeOffset>(
          node->left_, batt::Interval<usize>{range.lower_bound, middle}, space);

      right_subtree_size = packed_sizeof_bp_trie_node<SubtreeOffset>(
          node->right_, batt::Interval<usize>{middle, range.upper_bound}, space);
    }
  } else {
    space -= (i64)node_size;
  }
  return node_size + left_subtree_size + right_subtree_size;
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const BPTrie& object)
{
  batt::Interval<usize> range{0, object.size()};

  const BPTrieNode* root = object.root();

  i64 space = 0xff;
  usize z = sizeof(PackedBPTrie) + packed_sizeof_bp_trie_node<u8>(root, range, space);
  if (space < 0) {
    space = 0xffff;
    z = sizeof(PackedBPTrie) + packed_sizeof_bp_trie_node<little_u16>(root, range, space);
    if (space < 0) {
      space = 0xffffffl;
      z = sizeof(PackedBPTrie) + packed_sizeof_bp_trie_node<little_u24>(root, range, space);
      if (space < 0) {
        space = 0xffffffffl;
        z = sizeof(PackedBPTrie) + packed_sizeof_bp_trie_node<little_u32>(root, range, space);
        BATT_CHECK_GE(space, 0);
      }
    }
  }

  return z;
}

namespace {

template <typename SubtreeOffset>
struct QueueItem {
  const BPTrieNode* node;
  PackedPointer<PackedBPTrieNodeBase, SubtreeOffset>* p_pointer;
  batt::Interval<usize> range;
};

template <typename SubtreeOffset, typename Queue>
PackedBPTrie* build_packed_trie(const BPTrie& object, PackedBPTrie* packed, DataPacker* dst)
{
  using PackedNodePointer = PackedPointer<PackedBPTrieNodeBase, SubtreeOffset>;

  const BPTrieNode* root = object.root();

  Queue queue;
  queue.push(root, nullptr, batt::Interval<usize>{0, packed->size_});

  while (!queue.empty()) {
    QueueItem next = queue.pop();

    const BPTrieNode* node = next.node;
    PackedNodePointer* p_pointer = next.p_pointer;

    PackedBPTrieNodeBase* packed_node = dst->pack_record<PackedBPTrieNodeBase>();
    if (!packed_node) {
      return nullptr;
    }

    if (p_pointer) {
      p_pointer->reset(packed_node, dst);
      BATT_CHECK_EQ(packed_node, p_pointer->get());
    }

    // Encode prefix.
    {
      const u8 mask = (node->left_) ? PackedBPTrie::kParentNodeMask : 0x00;

      const u8* prefix_data = (const u8*)node->prefix_.data();
      usize prefix_len = node->prefix_.size();

      while (prefix_len >= PackedBPTrie::kMaxPrefixChunkLen) {
        if (!dst->pack_raw_data(prefix_data, PackedBPTrie::kMaxPrefixChunkLen)) {
          return nullptr;
        }
        packed_node->header =
            (PackedBPTrie::kMaxPrefixChunkLen & PackedBPTrie::kPrefixChunkLenMask) | mask;
        prefix_data += PackedBPTrie::kMaxPrefixChunkLen;
        prefix_len -= PackedBPTrie::kMaxPrefixChunkLen;
        packed_node = dst->pack_record<PackedBPTrieNodeBase>();
        if (!packed_node) {
          return nullptr;
        }
      }

      packed_node->header = (prefix_len & PackedBPTrie::kPrefixChunkLenMask) | mask;
      if (prefix_len) {
        if (!dst->pack_raw_data(prefix_data, prefix_len)) {
          return nullptr;
        }
      }
    }

    // If leaf, continue.
    //
    if (!node->left_) {
      BATT_CHECK_EQ(node->right_, nullptr);
      continue;
    }

    BATT_CHECK_NOT_NULLPTR(node->right_);

    // Encode parent fields and push left/right subtrees.
    //
    const auto pack_parent = [&](auto parent_layout) {
      using ParentLayout = typename decltype(parent_layout)::type;

      ParentLayout* parent = dst->pack_record<ParentLayout>();
      if (!parent) {
        return parent;
      }

      parent->pivot = node->pivot_;
      parent->pivot_pos = node->pivot_pos_;

      BATT_CHECK_EQ(node->pivot_pos_, parent->pivot_pos);

      usize middle = next.range.lower_bound + node->pivot_pos_;

      queue.push(node->left_, &parent->left, batt::Interval<usize>{next.range.lower_bound, middle});
      queue.push(node->right_, &parent->right,
                 batt::Interval<usize>{middle, next.range.upper_bound});

      return parent;
    };

    if (next.range.size() <= 0xff) {
      if (!pack_parent(batt::StaticType<PackedBPTrieNodeParent<u8, SubtreeOffset>>{})) {
        return nullptr;
      }
    } else if (next.range.size() <= 0xffff) {
      if (!pack_parent(batt::StaticType<PackedBPTrieNodeParent<little_u16, SubtreeOffset>>{})) {
        return nullptr;
      }
    } else if (next.range.size() <= 0xffffffl) {
      if (!pack_parent(batt::StaticType<PackedBPTrieNodeParent<little_u24, SubtreeOffset>>{})) {
        return nullptr;
      }
    } else if (next.range.size() <= 0xffffffffl) {
      if (!pack_parent(batt::StaticType<PackedBPTrieNodeParent<little_u32, SubtreeOffset>>{})) {
        return nullptr;
      }
    }
  }

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SubtreeOffset>
PackedBPTrie* pack_trie_impl(const BPTrie& object, PackedBPTrie* packed, DataPacker* dst)
{
  switch (object.get_packed_layout()) {
    case BPTrie::PackedLayout::kBreadthFirst:
      return build_packed_trie<SubtreeOffset, BreadthFirstOrder<QueueItem<SubtreeOffset>>>(
          object, packed, dst);

    case BPTrie::PackedLayout::kVanEmdeBoas:
      return build_packed_trie<SubtreeOffset, VanEmdeBoasOrder<QueueItem<SubtreeOffset>>>(
          object, packed, dst);

    default:
      break;
  }
  return nullptr;
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBPTrie* pack_object_to(const BPTrie& object, PackedBPTrie* packed,
                                   llfs::DataPacker* dst)
{
  packed->size_ = object.size();

  const BPTrieNode* root = object.root();
  if (!root) {
    return packed;
  }

  const usize packed_byte_size = packed_sizeof(object);
  if (packed_byte_size <= 0xff) {
    packed->offset_kind_ = PackedBPTrie::kOffset8;
    return pack_trie_impl<u8>(object, packed, dst);

  } else if (packed_byte_size <= 0xffff) {
    packed->offset_kind_ = PackedBPTrie::kOffset16;
    return pack_trie_impl<little_u16>(object, packed, dst);

  } else if (packed_byte_size <= 0xffffff) {
    packed->offset_kind_ = PackedBPTrie::kOffset24;
    return pack_trie_impl<little_u24>(object, packed, dst);

  } else {
    packed->offset_kind_ = PackedBPTrie::kOffset32;
    return pack_trie_impl<little_u32>(object, packed, dst);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Interval<usize> BPTrie::find(std::string_view key) const noexcept
{
  batt::Interval<usize> range{0, this->size()};

  const BPTrieNode* node = this->root();
  if (!node) {
    return range;
  }

  for (;;) {
    const std::string_view& prefix = node->prefix_;
    const usize prefix_len = prefix.size();
    if (prefix_len != 0) {
      const usize key_len = key.size();
      const usize common_len = std::min(prefix_len, key_len);
      const batt::Order order = batt::compare(key.substr(0, common_len), prefix);

      if (order == batt::Order::Greater) {
        range.lower_bound = range.upper_bound;
        break;
      }
      if (order == batt::Order::Less || key_len < prefix_len) {
        range.upper_bound = range.lower_bound;
        break;
      }
    }

    if (!node->left_) {
      range.upper_bound = range.lower_bound + 1;
      break;
    }

    const usize middle = range.lower_bound + node->pivot_pos_;
    const u8 parent_pivot = node->pivot_;

    key = key.substr(prefix_len);

    if (key.empty() || (u8)key[0] < parent_pivot) {
      node = node->left_;
      range.upper_bound = middle;
    } else {
      node = node->right_;

      // Implement right-leaf optimization (the parent pivot is always prefix[0] in this case).
      //
      if (!node->left_) {
        if ((u8)key[0] != parent_pivot) {
          range.lower_bound = range.upper_bound;
          return range;
        }
        key = key.substr(1);
      }

      range.lower_bound = middle;
    }
  }

  return range;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string_view BPTrie::get_key(usize index, batt::SmallVecBase<char>& buffer) const noexcept
{
  buffer.clear();
  batt::Interval<usize> range{0, this->size_};
  const BPTrieNode* node = this->root_;

  for (;;) {
    if (!node) {
      break;
    }

    buffer.insert(buffer.end(), node->prefix_.data(), node->prefix_.data() + node->prefix_.size());

    const usize middle = range.lower_bound + node->pivot_pos_;

    if (index < middle) {
      node = node->left_;
      range.upper_bound = middle;
    } else {
      const char parent_pivot = (char)node->pivot_;
      node = node->right_;

      // Implement right-leaf optimization (the parent pivot is always prefix[0] in this case).
      //
      if (node && !node->left_) {
        buffer.push_back(parent_pivot);
        buffer.insert(buffer.end(), node->prefix_.data(),
                      node->prefix_.data() + node->prefix_.size());
        break;
      }

      range.lower_bound = middle;
    }
  }

  return std::string_view{buffer.data(), buffer.size()};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
namespace {

template <typename T>
struct NextSmaller;

template <>
struct NextSmaller<little_u32> : batt::StaticType<little_u24> {
};

template <>
struct NextSmaller<little_u24> : batt::StaticType<little_u16> {
};

template <>
struct NextSmaller<little_u16> : batt::StaticType<u8> {
};

template <>
struct NextSmaller<u8> : batt::StaticType<u8> {
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename PivotPosT, typename SubtreeOffset>
batt::Interval<usize> find_impl(const PackedBPTrieNodeBase* node, std::string_view& key,
                                batt::Interval<usize>& range)
{
  constexpr i64 threshold = i64{1} << (sizeof(typename NextSmaller<PivotPosT>::type) * 8);

  for (;;) {
    if (!std::is_same_v<PivotPosT, u8> && range.size() < threshold) {
      return find_impl<typename NextSmaller<PivotPosT>::type, SubtreeOffset>(node, key, range);
    }

    usize prefix_chunk_len;
    for (;;) {
      prefix_chunk_len = node->header & PackedBPTrie::kPrefixChunkLenMask;
      if (prefix_chunk_len != 0) {
        std::string_view prefix_chunk{node->prefix_, prefix_chunk_len};
        const usize key_len = key.size();
        const usize common_len = std::min(prefix_chunk_len, key_len);
        const batt::Order order = batt::compare(key.substr(0, common_len), prefix_chunk);

        if (order == batt::Order::Greater) {
          range.lower_bound = range.upper_bound;
          return range;
        }
        if (order == batt::Order::Less || key_len < prefix_chunk_len) {
          range.upper_bound = range.lower_bound;
          return range;
        }

        key = key.substr(prefix_chunk_len);

        if (prefix_chunk_len == PackedBPTrie::kMaxPrefixChunkLen) {
          node = reinterpret_cast<const PackedBPTrieNodeBase*>(&node->prefix_[prefix_chunk_len]);
          continue;
        }
      }
      break;
    }

    if ((node->header & PackedBPTrie::kParentNodeMask) == 0) {
      range.upper_bound = range.lower_bound + 1;
      break;
    }

    const auto* parent = reinterpret_cast<const PackedBPTrieNodeParent<PivotPosT, SubtreeOffset>*>(
        &node->prefix_[prefix_chunk_len]);

    const usize middle = range.lower_bound + (usize)parent->pivot_pos;
    const u8 parent_pivot = parent->pivot;

    if (key.empty() || (u8)key[0] < parent_pivot) {
      node = parent->left.get();
      range.upper_bound = middle;
    } else {
      node = parent->right.get();

      // Implement right-leaf optimization (the parent pivot is always prefix[0] in this case).
      //
      if ((node->header & PackedBPTrie::kParentNodeMask) == 0) {
        if ((u8)key[0] != parent_pivot) {
          range.lower_bound = range.upper_bound;
          return range;
        }
        key = key.substr(1);
      }

      range.lower_bound = middle;
    }
  }

  return range;
}
}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Interval<usize> PackedBPTrie::find(std::string_view key) const noexcept
{
  batt::Interval<usize> range{0, this->size()};

  if (range.empty()) {
    return range;
  }

  const PackedBPTrieNodeBase* node = this->root();

  switch (this->offset_kind_) {
    case PackedBPTrie::kOffset8:
      return find_impl<u8, u8>(node, key, range);

    case PackedBPTrie::kOffset16:
      return find_impl<little_u16, little_u16>(node, key, range);

    case PackedBPTrie::kOffset24:
      return find_impl<little_u24, little_u24>(node, key, range);

    case PackedBPTrie::kOffset32:
      return find_impl<little_u32, little_u32>(node, key, range);
  }

  BATT_PANIC() << "Bad offset kind: " << (int)this->offset_kind_;
  BATT_UNREACHABLE();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

namespace {

template <typename PivotPosT, typename SubtreeOffset>
std::string_view get_key_impl(const PackedBPTrieNodeBase* node, usize index,
                              batt::SmallVecBase<char>& buffer, batt::Interval<usize>& range)
{
  constexpr i64 threshold = i64{1} << (sizeof(typename NextSmaller<PivotPosT>::type) * 8);

  for (;;) {
    if (!std::is_same_v<PivotPosT, u8> && range.size() < threshold) {
      return get_key_impl<typename NextSmaller<PivotPosT>::type, SubtreeOffset>(node, index, buffer,
                                                                                range);
    }

    usize prefix_chunk_len;
    for (;;) {
      prefix_chunk_len = node->header & PackedBPTrie::kPrefixChunkLenMask;
      if (prefix_chunk_len != 0) {
        buffer.insert(buffer.end(), node->prefix_, node->prefix_ + prefix_chunk_len);

        if (prefix_chunk_len == PackedBPTrie::kMaxPrefixChunkLen) {
          node = reinterpret_cast<const PackedBPTrieNodeBase*>(&node->prefix_[prefix_chunk_len]);
          continue;
        }
      }
      break;
    }

    if ((node->header & PackedBPTrie::kParentNodeMask) == 0) {
      break;
    }

    const auto* parent = reinterpret_cast<const PackedBPTrieNodeParent<PivotPosT, SubtreeOffset>*>(
        &node->prefix_[prefix_chunk_len]);

    const usize middle = range.lower_bound + (usize)parent->pivot_pos;

    if (index < middle) {
      node = parent->left.get();
      range.upper_bound = middle;
    } else {
      const char parent_pivot = (char)parent->pivot;
      node = parent->right.get();

      // Implement right-leaf optimization (the parent pivot is always prefix[0] in this case).
      //
      if ((node->header & PackedBPTrie::kParentNodeMask) == 0) {
        buffer.push_back(parent_pivot);
      }

      range.lower_bound = middle;
    }
  }

  return std::string_view{buffer.data(), buffer.size()};
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string_view PackedBPTrie::get_key(usize index, batt::SmallVecBase<char>& buffer) const noexcept
{
  buffer.clear();
  batt::Interval<usize> range{0, this->size_};
  const PackedBPTrieNodeBase* node = this->root();

  switch (this->offset_kind_) {
    case PackedBPTrie::kOffset8:
      return get_key_impl<u8, u8>(node, index, buffer, range);

    case PackedBPTrie::kOffset16:
      return get_key_impl<little_u16, little_u16>(node, index, buffer, range);

    case PackedBPTrie::kOffset24:
      return get_key_impl<little_u24, little_u24>(node, index, buffer, range);

    case PackedBPTrie::kOffset32:
      return get_key_impl<little_u32, little_u32>(node, index, buffer, range);
  }

  BATT_PANIC() << "Bad offset kind: " << (int)this->offset_kind_;
  BATT_UNREACHABLE();
}

}  //namespace llfs
