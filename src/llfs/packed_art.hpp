//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PACKED_ART_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/logging.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/slice.hpp>
#include <llfs/status.hpp>

#include <batteries/small_vec.hpp>

#include <type_traits>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedART {
  enum NodeKind {
    kLeaf = 0,
    kSmall = 1,
    kMedium = 2,
    kLarge = 3,
  };

  struct NodeBase {
    u8 kind;
    u8 prefix_len;

    //----- --- -- -  -  -   -

    std::string_view prefix_str() const
    {
      const char* data = ((const char*)this) - this->prefix_len;
      return std::string_view{data, this->prefix_len};
    }
  };

  struct LeafNode : NodeBase {
  };

  using Branch = PackedPointer<NodeBase, little_u32>;

  struct SmallNode : NodeBase {
    u8 branch_count;

    //----- --- -- -  -  -   -

    Slice<const u8> get_index() const
    {
      return as_slice((&this->branch_count) + 1, this->branch_count);
    }

    Slice<const Branch> get_branches() const
    {
      return as_slice((const Branch*)this->get_index().end(), this->branch_count);
    }
  };

  struct MediumNode : NodeBase {
    u8 branch_count;
    std::array<u8, 32> branch_set;
  };

  struct LargeNode : NodeBase {
    std::array<Branch, 256> branches;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename Fn>
  static void visit_branches(const LeafNode*, const Fn&)
  {
  }

  template <typename Fn>
  static void visit_branches(const SmallNode* node, const Fn& fn)
  {
    Slice<const u8> index = node->get_index();
    Slice<const Branch> branches = node->get_branches();

    for (usize i = 0; i < node->branch_count; ++i) {
      fn(index[i], branches[i].get());
    }
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename ValueT, typename GetKeyFn>
struct NthByteOrder {
  using Self = NthByteOrder;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const GetKeyFn& get_key_fn;
  usize n;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u8 get_nth_byte(u8 value) const
  {
    return value;
  }

  u8 get_nth_byte(const ValueT& value) const
  {
    std::string_view key = this->get_key_fn(value);
    if (key.size() < this->n) {
      return u8{0};
    }
    return static_cast<u8>(key[this->n]);
  }

  template <typename LeftT, typename RightT>
  bool operator()(const LeftT& left, const RightT& right) const
  {
    return this->get_nth_byte(left) < this->get_nth_byte(right);
  }
};

template <typename Iter, typename GetKeyFn>
inline StatusOr<PackedART::NodeBase*> pack_art(Iter first, Iter last, const GetKeyFn& get_key_fn,
                                               MutableBuffer& dst, usize base_prefix_len = 0)
{
  using ValueT = std::decay_t<decltype(*first)>;

  if (first == last) {
    LLFS_VLOG(1) << "pack_art({}, base_prefix=" << base_prefix_len << ")";
    return {(PackedART::NodeBase*)nullptr};
  }

  std::string_view min_key = get_key_fn(*first);
  usize size = std::distance(first, last);

  // Pack leaf.
  //
  if (size == 1) {
    std::string_view key = get_key_fn(*first);
    const usize prefix_len = key.size();
    const usize prefix_fragment_len = prefix_len - base_prefix_len;
    const usize space_needed = prefix_fragment_len + sizeof(PackedART::LeafNode*);

    if (dst.size() < space_needed) {
      return {batt::StatusCode::kOutOfRange};
    }

    std::memcpy(dst.data(), key.data() + base_prefix_len, prefix_fragment_len);
    dst += prefix_fragment_len;

    auto* leaf_node = (PackedART::LeafNode*)dst.data();
    leaf_node->kind = PackedART::kLeaf;
    leaf_node->prefix_len = prefix_fragment_len;
    dst += sizeof(PackedART::LeafNode);

    return {leaf_node};
  }

  // Calculate prefix.
  //
  std::string_view max_key = get_key_fn(*std::prev(last));

  LLFS_VLOG(1) << std::string(base_prefix_len * 4, '.') << "pack_art({"
               << batt::c_str_literal(min_key) << ".." << batt::c_str_literal(max_key)
               << "}, base_prefix[" << base_prefix_len
               << "]=" << batt::c_str_literal(min_key.substr(0, base_prefix_len)) << ")";

  BATT_CHECK_GE(min_key.size(), base_prefix_len);
  BATT_CHECK_GE(max_key.size(), base_prefix_len);

  usize prefix_len = base_prefix_len;
  {
    const char* key0 = &min_key[base_prefix_len];
    const char* key1 = &max_key[base_prefix_len];
    usize common_len = std::min(min_key.size(), max_key.size()) - base_prefix_len;
    while (common_len && *key0 == *key1) {
      ++prefix_len;
      --common_len;
      ++key0;
      ++key1;
    }
  }

  LLFS_VLOG(1) << std::string(base_prefix_len * 4, '.') << "  " << BATT_INSPECT(prefix_len);

  const u8 min_branch_byte = (prefix_len < min_key.size()) ? min_key[prefix_len] : 0;
  const u8 max_branch_byte = (prefix_len < max_key.size()) ? max_key[prefix_len] : 255;

  batt::SmallVec<std::pair<u8, Iter>, 256> pivots;

  Iter iter = first;
  pivots.emplace_back(min_branch_byte, iter);
  for (u8 next_byte = min_branch_byte; next_byte <= max_branch_byte; ++next_byte) {
    Iter next_iter = std::upper_bound(iter, last, next_byte,
                                      NthByteOrder<ValueT, GetKeyFn>{get_key_fn, prefix_len});
    if (next_iter != iter) {
      pivots.emplace_back(next_byte, next_iter);
      iter = next_iter;
    }
  }

  const usize branch_count = pivots.size() - 1;

  LLFS_VLOG(1) << std::string(base_prefix_len * 4, '.') << "  " << BATT_INSPECT(branch_count)
               << "; " << (int)min_branch_byte << " .. " << (int)max_branch_byte;

  //----- --- -- -  -  -   -
  const auto for_all_branches = [&](auto&& loop_fn) -> Status {
    for (usize i = 0; i < branch_count; ++i) {
      LLFS_VLOG(1) << std::string(base_prefix_len * 4, '.') << "  "
                   << " branch_byte=" << (int)pivots[i + 1].first;
      BATT_REQUIRE_OK(loop_fn(i, pivots[i + 1].first, pivots[i].second, pivots[i + 1].second));
    }
    return OkStatus();
  };
  //----- --- -- -  -  -   -

  if (branch_count <= 64) {
    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // SmallNode
    //
    const usize space_needed =
        prefix_len + sizeof(PackedART::SmallNode) + branch_count * (1 + sizeof(PackedART::Branch));

    if (dst.size() < space_needed) {
      return {batt::StatusCode::kOutOfRange};
    }

    std::memcpy(dst.data(), min_key.data(), prefix_len);
    dst += prefix_len;

    auto* small_node = (PackedART::SmallNode*)dst.data();
    small_node->kind = PackedART::kSmall;
    small_node->prefix_len = prefix_len;
    small_node->branch_count = branch_count;
    dst += sizeof(PackedART::SmallNode);

    Slice<u8> index = as_slice((u8*)dst.data(), branch_count);
    dst += branch_count;

    Slice<PackedART::Branch> branches = as_slice((PackedART::Branch*)dst.data(), branch_count);
    dst += (branch_count * sizeof(PackedART::Branch));

    BATT_REQUIRE_OK(for_all_branches([&](usize i, u8 branch_byte, const Iter& branch_first,
                                         const Iter& branch_last) -> Status {
      index[i] = branch_byte;

      BATT_ASSIGN_OK_RESULT(auto branch_root,
                            pack_art(branch_first, branch_last, get_key_fn, dst, prefix_len + 1));

      branches[i].reset_unsafe(branch_root);

      return OkStatus();
    }));

    return {small_node};

  } else if (branch_count < 248) {
    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Medium Node
    //
    const usize space_needed =
        prefix_len + sizeof(PackedART::MediumNode) + branch_count * sizeof(PackedART::Branch);

    if (dst.size() < space_needed) {
      return {batt::StatusCode::kOutOfRange};
    }

    std::memcpy(dst.data(), min_key.data(), prefix_len);
    dst += prefix_len;

    auto* medium_node = (PackedART::MediumNode*)dst.data();
    medium_node->kind = PackedART::kMedium;
    medium_node->prefix_len = prefix_len;
    medium_node->branch_count = branch_count;
    std::memset(medium_node->branch_set.data(), 0, sizeof(medium_node->branch_set));
    dst += sizeof(PackedART::MediumNode);

    Slice<PackedART::Branch> branches = as_slice((PackedART::Branch*)dst.data(), branch_count);
    dst += (branch_count * sizeof(PackedART::Branch));

    BATT_REQUIRE_OK(for_all_branches([&](usize i, u8 branch_byte, const Iter& branch_first,
                                         const Iter& branch_last) -> Status {
      const usize set_index = branch_byte / 8;
      const usize set_bit_i = branch_byte % 8;
      medium_node->branch_set[set_index] |= u8{1} << set_bit_i;

      BATT_ASSIGN_OK_RESULT(auto branch_root,
                            pack_art(branch_first, branch_last, get_key_fn, dst, prefix_len + 1));

      branches[i].reset_unsafe(branch_root);

      return OkStatus();
    }));

    return {medium_node};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // LargeNode
  //
  const usize space_needed = prefix_len + sizeof(PackedART::LargeNode);

  if (dst.size() < space_needed) {
    return {batt::StatusCode::kOutOfRange};
  }

  std::memcpy(dst.data(), min_key.data(), prefix_len);
  dst += prefix_len;

  auto* large_node = (PackedART::LargeNode*)dst.data();
  large_node->kind = PackedART::kLarge;
  large_node->prefix_len = prefix_len;
  dst += sizeof(PackedART::LargeNode);

  usize j = 0;

  BATT_REQUIRE_OK(
      for_all_branches([&](usize i [[maybe_unused]], u8 branch_byte, const Iter& branch_first,
                           const Iter& branch_last) -> Status {
        for (; j < branch_byte; ++j) {
          large_node->branches[j].offset = 0;
        }

        BATT_ASSIGN_OK_RESULT(auto branch_root,
                              pack_art(branch_first, branch_last, get_key_fn, dst, prefix_len + 1));

        large_node->branches[j].reset_unsafe(branch_root);
        ++j;

        return OkStatus();
      }));

  for (; j < 256; ++j) {
    large_node->branches[j].offset = 0;
  }

  return {large_node};
}

}  //namespace llfs
