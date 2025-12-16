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
    static constexpr NodeKind kKind = kLeaf;

    static usize packed_size(usize prefix_fragment_len, usize branch_count)
    {
      BATT_CHECK_EQ(branch_count, 0);

      return prefix_fragment_len + sizeof(LeafNode);
    }

    //----- --- -- -  -  -   -

    template <typename Fn>
    void visit_branches(const Fn&)
    {
    }
  };

  using Branch = PackedPointer<NodeBase, little_u32>;

  struct SmallNode : NodeBase {
    static constexpr NodeKind kKind = kSmall;

    static usize packed_size(usize prefix_fragment_len, usize branch_count)
    {
      BATT_CHECK_GT(branch_count, 1);
      BATT_CHECK_LT(branch_count, 256);

      return prefix_fragment_len + sizeof(SmallNode) + branch_count * (1 + sizeof(Branch));
    }

    //----- --- -- -  -  -   -

    u8 branch_count;

    //----- --- -- -  -  -   -

    void set_branch_count(usize n)
    {
      this->branch_count = BATT_CHECKED_CAST(u8, n);
    }

    template <typename ForAllBranchesFn>
    Status initialize_branches(ForAllBranchesFn&& for_all_branches)
    {
      Slice<u8> index = this->get_index();
      Slice<Branch> branches = this->get_branches();

      BATT_REQUIRE_OK(for_all_branches([&](usize i, u8 branch_byte,   //
                                           const Iter& branch_first,  //
                                           const Iter& branch_last) -> Status {
        index[i] = branch_byte;

        BATT_ASSIGN_OK_RESULT(auto branch_root,
                              pack_art(branch_first, branch_last, get_key_fn, dst, prefix_len + 1));

        branches[i].reset_unsafe(branch_root);

        return OkStatus();
      }));

      return OkStatus();
    }

    Slice<const u8> get_index() const
    {
      return as_slice((&this->branch_count) + 1, this->branch_count);
    }

    Slice<u8> get_index()
    {
      return as_slice((&this->branch_count) + 1, this->branch_count);
    }

    Slice<const Branch> get_branches() const
    {
      return as_slice((const Branch*)this->get_index().end(), this->branch_count);
    }

    Slice<Branch> get_branches()
    {
      return as_slice((Branch*)this->get_index().end(), this->branch_count);
    }

    template <typename Fn>
    void visit_branches(const Fn& fn)
    {
      Slice<const u8> index = this->get_index();
      Slice<const Branch> branches = this->get_branches();

      for (usize i = 0; i < this->branch_count; ++i) {
        fn(index[i], branches[i].get());
      }
    }
  };

  struct MediumNode : NodeBase {
    static constexpr NodeKind kKind = kMedium;

    static usize packed_size(usize prefix_fragment_len, usize branch_count)
    {
      BATT_CHECK_GT(branch_count, 1);

      return prefix_fragment_len + sizeof(MediumNode) + sizeof(Branch) * branch_count;
    }

    //----- --- -- -  -  -   -

    u8 branch_count;
    std::array<u8, 32> branch_set;

    //----- --- -- -  -  -   -

    void set_branch_count(usize n)
    {
      this->branch_count = BATT_CHECKED_CAST(u8, n);
    }

    template <typename ForAllBranchesFn>
    Status initialize_branches(ForAllBranchesFn&& for_all_branches)
    {
      std::memset(medium_node->branch_set.data(), 0, sizeof(medium_node->branch_set));

      Slice<Branch> branches = this->get_branches();

      BATT_REQUIRE_OK(for_all_branches([&](usize i, u8 branch_byte,   //
                                           const Iter& branch_first,  //
                                           const Iter& branch_last) -> Status {
        const usize set_index = branch_byte / 8;
        const usize set_bit_i = branch_byte % 8;

        this->branch_set[set_index] |= u8{1} << set_bit_i;

        BATT_ASSIGN_OK_RESULT(auto branch_root,
                              pack_art(branch_first, branch_last, get_key_fn, dst, prefix_len + 1));

        branches[i].reset_unsafe(branch_root);

        return OkStatus();
      }));

      return OkStatus();
    }

    Slice<const Branch> get_branches() const
    {
      return as_slice((const PackedART::Branch*)(this + 1), this->branch_count);
    }

    Slice<Branch> get_branches()
    {
      return as_slice((PackedART::Branch*)(this + 1), this->branch_count);
    }
  };

  struct LargeNode : NodeBase {
    static constexpr NodeKind kKind = kLarge;

    static usize packed_size(usize prefix_fragment_len, usize branch_count)
    {
      BATT_CHECK_GT(branch_count, 128);

      return prefix_fragment_len + sizeof(LargeNode);
    }

    //----- --- -- -  -  -   -

    std::array<Branch, 256> branches;

    //----- --- -- -  -  -   -

    void set_branch_count(usize n [[maybe_unused]])
    {
    }

    template <typename ForAllBranchesFn>
    Status initialize_branches(ForAllBranchesFn&& for_all_branches)
    {
      usize j = 0;

      BATT_REQUIRE_OK(for_all_branches([&j, this](usize /*i*/, u8 branch_byte,  //
                                                  const Iter& branch_first,     //
                                                  const Iter& branch_last) -> Status {
        for (; j < branch_byte; ++j) {
          this->branches[j].offset = 0;
        }

        BATT_ASSIGN_OK_RESULT(auto branch_root,
                              pack_art(branch_first, branch_last, get_key_fn, dst, prefix_len + 1));

        this->branches[j].reset_unsafe(branch_root);
        ++j;

        return OkStatus();
      }));

      for (; j < 256; ++j) {
        large_node->branches[j].offset = 0;
      }

      return OkStatus();
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename NodeT, typename ForAllBranchesFn>
  static StatusOr<NodeT*> pack_node(const std::string_view& prefix_fragment, usize branch_count,
                                    ForAllBranchesFn&& for_all_branches, MutableBuffer& dst,
                                    batt::StaticType<NodeT> = {})
  {
    const usize space_needed = NodeT::packed_size(prefix_fragment.size(), branch_count);

    if (dst.size() < space_needed) {
      return {batt::StatusCode::kOutOfRange};
    }

    const u8 prefix_len = BATT_CHECKED_CAST(u8, prefix_fragment.size());
    std::memcpy(dst.data(), prefix_fragment.data(), prefix_len);

    NodeT* node = (NodeT*)(dst + prefix_len).data();
    node->prefix_len = prefix_len;
    node->kind = NodeT::kKind;
    node->set_branch_count(branch_count);

    BATT_REQUIRE_OK(node->initialize_branches(BATT_FORWARD(for_all_branches)));

    dst += space_needed;

    return {node};
  }

  static usize common_prefix_len(const std::string_view& min_key,  //
                                 const std::string_view& max_key,  //
                                 usize base_prefix_len)
  {
    usize prefix_len = base_prefix_len;

    const char* key0 = &min_key[base_prefix_len];
    const char* key1 = &max_key[base_prefix_len];

    usize common_len = std::min(min_key.size(), max_key.size()) - base_prefix_len;

    while (common_len && *key0 == *key1) {
      ++prefix_len;
      --common_len;
      ++key0;
      ++key1;
    }

    return prefix_len;
  }

  template <typename Iter>
  static void find_pivots(const Iter& first, const Iter& last,  //
                          const std::string_view& min_key,      //
                          const std::string_view& max_key,      //
                          usize prefix_len,                     //
                          batt::SmallVecBase<std::pair<u8, Iter>>& pivots)
  {
    pivots.clear();

    const u8 min_branch_byte = (prefix_len < min_key.size()) ? min_key[prefix_len] : 0;
    const u8 max_branch_byte = (prefix_len < max_key.size()) ? max_key[prefix_len] : 255;

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

    return pivots.size() - 1;
  }

  template <typename Iter, typename GetKeyFn>
  class NodeBuilder
  {
   public:
    explicit NodeBuilder() noexcept
    {
    }

   private:
    Iter first_;
    Iter last_;
    const GetKeyFn& get_key_fn_;
    usize base_prefix_len_;
    std::string_view min_key_;
    std::string_view max_key_;
    usize prefix_len_;
    batt::SmallVec<std::pair<u8, Iter>, 256> pivots;
  };
};

template <typename Iter, typename GetKeyFn>
inline StatusOr<PackedART::NodeBase*> pack_art(Iter first, Iter last, const GetKeyFn& get_key_fn,
                                               MutableBuffer& dst, usize base_prefix_len = 0)
{
  using ValueT = std::decay_t<decltype(*first)>;

  if (first == last) {
    return {(PackedART::NodeBase*)nullptr};
  }

  std::string_view min_key = get_key_fn(*first);
  BATT_CHECK_GE(min_key.size(), base_prefix_len);

  usize size = std::distance(first, last);

  // Pack leaf.
  //
  if (size == 1) {
    std::string_view prefix_fragment = min_key.substr(base_prefix_len);

    return PackedART::pack_node(prefix_fragment, /*branch_count=*/0, for_all_branches, dst,
                                batt::StaticType<PackedART::LeafNode>{});
  }

  // Calculate prefix.
  //
  std::string_view max_key = get_key_fn(*std::prev(last));

  BATT_CHECK_GE(max_key.size(), base_prefix_len);

  const usize prefix_len = PackedART::common_prefix_len(min_key, max_key, base_prefix_len);

  const std::string_view prefix_fragment =
      min_key.substr(base_prefix_len, prefix_len - base_prefix_len);

  batt::SmallVec<std::pair<u8, Iter>, 256> pivots;
  const usize branch_count = find_pivots(min_key, max_key, prefix_len, pivots);

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
    return PackedART::pack_node(prefix_fragment, branch_count, for_all_branches, dst,
                                batt::StaticType<PackedART::SmallNode>{});
  }

  if (branch_count < 224) {
    return PackedART::pack_node(prefix_fragment, branch_count, for_all_branches, dst,
                                batt::StaticType<PackedART::MediumNode>{});
  }

  return PackedART::pack_node(prefix_fragment, branch_count, for_all_branches, dst,
                              batt::StaticType<PackedART::LargeNode>{});
}

}  //namespace llfs
