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
usize packed_sizeof_bp_trie_node(const BPTrieNode* node)
{
  if (!node) {
    return 0;
  }

  const usize prefix_len = node->prefix_.size();

  usize size = prefix_len + (prefix_len + PackedBPTrie::kMaxPrefixChunkLen + 1) /
                                PackedBPTrie::kMaxPrefixChunkLen;

  if (node->left_) {
    BATT_CHECK_NOT_NULLPTR(node->right_);
    size += sizeof(PackedBPTrieNodeParent<little_u16, little_u16>);
    size += packed_sizeof_bp_trie_node(node->left_);
    size += packed_sizeof_bp_trie_node(node->right_);
  }
  return size;
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const BPTrie& object)
{
  return sizeof(PackedBPTrie) + packed_sizeof_bp_trie_node(object.root());
}

namespace {

using PackedNodePointer = PackedPointer<PackedBPTrieNodeBase, little_u16>;
using QueueItem = std::pair<const BPTrieNode*, PackedNodePointer*>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Queue>
PackedBPTrie* build_packed_trie(const BPTrie& object, PackedBPTrie* packed, DataPacker* dst)
{
  packed->size_ = object.size();

  const BPTrieNode* root = object.root();
  if (!root) {
    return packed;
  }

  Queue queue;
  queue.push(root, nullptr);

  while (!queue.empty()) {
    QueueItem next = queue.pop();

    const BPTrieNode* node = next.first;
    PackedNodePointer* p_pointer = next.second;

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

    if (!node->left_) {
      BATT_CHECK_EQ(node->right_, nullptr);
      continue;
    }

    BATT_CHECK_NOT_NULLPTR(node->right_);

    using ParentLayout = PackedBPTrieNodeParent<little_u16, little_u16>;

    ParentLayout* parent = dst->pack_record<ParentLayout>();
    if (!parent) {
      return nullptr;
    }

    parent->pivot = node->pivot_;
    parent->pivot_pos = BATT_CHECKED_CAST(u16, node->pivot_pos_);

    BATT_CHECK_EQ(node->pivot_pos_, parent->pivot_pos.value());

    queue.push(node->left_, &parent->left);
    queue.push(node->right_, &parent->right);
  }

  return packed;
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBPTrie* pack_object_to(const BPTrie& object, PackedBPTrie* packed,
                                   llfs::DataPacker* dst)
{
  switch (object.get_packed_layout()) {
    case BPTrie::PackedLayout::kBreadthFirst:
      return build_packed_trie<BreadthFirstOrder<QueueItem>>(object, packed, dst);

    case BPTrie::PackedLayout::kVanEmdeBoas:
      return build_packed_trie<VanEmdeBoasOrder<QueueItem>>(object, packed, dst);

    default:
      break;
  }
  return nullptr;
}

//#define BP_TRACE(expr) std::cerr << expr << std::endl
#ifndef BP_TRACE
#define BP_TRACE(expr)
#endif

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Interval<usize> BPTrie::find(std::string_view key) const noexcept
{
  batt::Interval<usize> range{0, this->size()};

  const BPTrieNode* node = this->root();
  if (!node) {
    return range;
  }

  BP_TRACE("");

  for (;;) {
    const std::string_view& prefix = node->prefix_;
    BP_TRACE(" key == " << batt::c_str_literal(key) << BATT_INSPECT(range)
                        << " prefix == " << batt::c_str_literal(prefix) << " pivot == "
                        << batt::c_str_literal(std::string_view{(const char*)&node->pivot_, 1}));

    const usize prefix_len = prefix.size();
    if (prefix_len != 0) {
      const usize key_len = key.size();
      const usize common_len = std::min(prefix_len, key_len);
      const batt::Order order = batt::compare(key.substr(0, common_len), prefix);
      BP_TRACE(BATT_INSPECT(order));

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
      BP_TRACE("(leaf)");
      range.upper_bound = range.lower_bound + 1;
      break;
    }

    const usize middle = range.lower_bound + node->pivot_pos_;

    key = key.substr(prefix_len);

    if (key.empty() || (u8)key[0] < (u8)node->pivot_) {
      BP_TRACE("(left)");
      node = node->left_;
      range.upper_bound = middle;
    } else {
      BP_TRACE("(right)");
      node = node->right_;
      range.lower_bound = middle;
    }
  }

  return range;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Interval<usize> PackedBPTrie::find(std::string_view key) const noexcept
{
  batt::Interval<usize> range{0, this->size()};

  BP_TRACE("");

  if (range.empty()) {
    return range;
  }

  const PackedBPTrieNodeBase* node = reinterpret_cast<const PackedBPTrieNodeBase*>(this + 1);

  for (;;) {
    usize prefix_chunk_len;
    for (;;) {
      prefix_chunk_len = node->header & PackedBPTrie::kPrefixChunkLenMask;
      if (prefix_chunk_len != 0) {
        std::string_view prefix_chunk{node->prefix_, prefix_chunk_len};

        BP_TRACE(" key == " << batt::c_str_literal(key) << BATT_INSPECT(range)
                            << " prefix_chunk == " << batt::c_str_literal(prefix_chunk)
                            << BATT_INSPECT(prefix_chunk_len) << " parent == "
                            << ((node->header & PackedBPTrie::kParentNodeMask) != 0));

        const usize key_len = key.size();
        const usize common_len = std::min(prefix_chunk_len, key_len);
        const batt::Order order = batt::compare(key.substr(0, common_len), prefix_chunk);
        BP_TRACE(BATT_INSPECT(order));

        if (order == batt::Order::Greater) {
          BP_TRACE("(greater)");
          range.lower_bound = range.upper_bound;
          return range;
        }
        if (order == batt::Order::Less || key_len < prefix_chunk_len) {
          BP_TRACE("(less)" << BATT_INSPECT(key_len) << BATT_INSPECT(prefix_chunk_len));
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
    BP_TRACE("(check parent)" << BATT_INSPECT((unsigned)node->header));

    if ((node->header & PackedBPTrie::kParentNodeMask) == 0) {
      range.upper_bound = range.lower_bound + 1;
      break;
    }

    const PackedBPTrieNodeParent<little_u16, little_u16>* parent =
        reinterpret_cast<const PackedBPTrieNodeParent<little_u16, little_u16>*>(
            &node->prefix_[prefix_chunk_len]);

    const usize middle = range.lower_bound + (usize)parent->pivot_pos;

    if (key.empty() || (u8)key[0] < (u8)parent->pivot) {
      BP_TRACE("(left) <" << middle);
      node = parent->left.get();
      range.upper_bound = middle;
    } else {
      BP_TRACE("(right) >=" << middle);
      node = parent->right.get();
      range.lower_bound = middle;
    }
  }

  return range;
}

}  //namespace llfs
