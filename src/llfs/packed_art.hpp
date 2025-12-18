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

    //----- --- -- -  -  -   -

    template <typename Fn>
    void visit_branches(const Fn&) const
    {
    }
  };

  using Branch = PackedPointer<NodeBase, little_u32>;

  struct SmallNode : NodeBase {
    static constexpr NodeKind kKind = kSmall;

    //----- --- -- -  -  -   -

    u8 branch_count;

    //----- --- -- -  -  -   -

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
    void visit_branches(const Fn& fn) const
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
    //----- --- -- -  -  -   -

    u8 branch_count;
    std::array<u8, 32> branch_set;

    //----- --- -- -  -  -   -

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

    //----- --- -- -  -  -   -

    std::array<Branch, 256> branches;

    //----- --- -- -  -  -   -
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename NodeT, typename ForAllBranchesFn>
  static StatusOr<NodeT*> pack_node(const std::string_view& prefix_fragment, usize branch_count,
                                    ForAllBranchesFn&& for_all_branches, MutableBuffer& dst,
                                    batt::StaticType<NodeT> = {})
  {
  }

  static usize common_prefix_len(const std::string_view& min_key,  //
                                 const std::string_view& max_key,  //
                                 usize base_prefix_len);

  template <typename Iter, typename GetKeyFn>
  class NodeBuilder
  {
   public:
    using Self = NodeBuilder;
    using ValueT = std::decay_t<decltype(*std::declval<const Iter&>())>;

    struct BranchPivot {
      u8 key_byte;
      Iter upper_bound;
    };

    explicit NodeBuilder(Iter first, Iter last, const GetKeyFn& get_key_fn,
                         usize base_prefix_len) noexcept
        : first_{first}
        , last_{last}
        , get_key_fn_{get_key_fn}
        , base_prefix_len_{base_prefix_len}
        , min_key_{(this->first_ != this->last_) ? get_key_fn(*this->first_) : ""}
        , max_key_{(this->first_ != this->last_) ? get_key_fn(*std::prev(this->last_)) : ""}
        , prefix_len_{PackedART::common_prefix_len(this->min_key_, this->max_key_, base_prefix_len)}
        , prefix_fragment_{this->min_key_.substr(base_prefix_len,
                                                 this->prefix_len_ - base_prefix_len)}
        , pivots_{}
        , branch_count_{Self::find_pivots()}
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    StatusOr<NodeBase*> build(MutableBuffer& dst)
    {
      if (this->branch_count_ == 0) {
        return {(NodeBase*)nullptr};
      }

      if (this->branch_count_ == 1) {
        return this->pack_node(batt::StaticType<LeafNode>{}, dst);
      }

      if (this->branch_count_ <= 64) {
        return this->pack_node(batt::StaticType<SmallNode>{}, dst);
      }

      if (this->branch_count_ < 224) {
        return this->pack_node(batt::StaticType<MediumNode>{}, dst);
      }

      return this->pack_node(batt::StaticType<LargeNode>{}, dst);
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    usize find_pivots()
    {
      this->pivots_.clear();

      for (Iter iter = this->first_; iter != this->last_;) {
        const std::string_view next_key = this->get_key_fn_(*iter);
        BATT_CHECK_LT(this->prefix_len_, next_key.size());

        const u8 next_key_byte = next_key[this->prefix_len_];
        const Iter next_upper_bound =
            std::upper_bound(iter, this->last_, next_key_byte,
                             NthByteOrder<ValueT, GetKeyFn>{this->get_key_fn_, this->prefix_len_});

        this->pivots_.emplace_back(next_key_byte, next_upper_bound);

        BATT_CHECK_NE(next_upper_bound, iter);
        iter = next_upper_bound;
      }

      return this->pivots_.size();
    }

    template <
        typename BranchFn /* = Status (usize branch_i, u8 branch_btye, Iter first, Iter last) */>
    Status for_each_branch(const BranchFn& branch_fn) const
    {
      Iter lower_bound = this->first_;
      usize branch_i = 0;
      for (const auto& [branch_byte, upper_bound] : this->pivots_) {
        BATT_REQUIRE_OK(branch_fn(branch_i, branch_byte, lower_bound, upper_bound));
        lower_bound = upper_bound;
        ++branch_i;
      }

      return OkStatus();
    }

    template <typename NodeT>
    StatusOr<NodeBase*> pack_node(batt::StaticType<NodeT> node_type, MutableBuffer& dst)
    {
      // Make sure we have enough space.
      //
      const usize space_needed = this->get_space_needed(node_type);
      if (dst.size() < space_needed) {
        return {batt::StatusCode::kOutOfRange};
      }

      // Copy the prefix fragment.
      //
      const u8 prefix_len = BATT_CHECKED_CAST(u8, this->prefix_fragment_.size());
      std::memcpy(dst.data(), this->prefix_fragment_.data(), prefix_len);

      NodeT* node = static_cast<NodeT*>((dst + prefix_len).data());
      {
        // Initialize NodeBase fields.
        //
        NodeBase* node_base = node;
        node_base->prefix_len = prefix_len;
        node_base->kind = NodeT::kKind;
      }

      // Consume buffer space.  IMPORTANT: we must do this before calling initialize_branches,
      // because that function will recursively pack subtrees.
      //
      dst += space_needed;

      // Initialize branches.
      //
      this->set_branch_count(node);
      BATT_REQUIRE_OK(this->initialize_branches(node, dst));

      // Success!
      //
      return {static_cast<NodeBase*>(node)};
    }

    //----- --- -- -  -  -   -

    usize get_space_needed(batt::StaticType<LeafNode>) const
    {
      return this->prefix_fragment_.size() + sizeof(LeafNode);
    }

    void set_branch_count(LeafNode* node [[maybe_unused]]) const
    {
      BATT_CHECK_EQ(this->branch_count_, 1);
    }

    Status initialize_branches(LeafNode* /*node*/, MutableBuffer& /*dst*/) const
    {
      return OkStatus();
    }

    //----- --- -- -  -  -   -

    usize get_space_needed(batt::StaticType<SmallNode>) const
    {
      // Never use this node type for more than 64 branches, since we can't use SIMD acceleration.
      //
      if (this->branch_count_ > 64) {
        return ~usize{0};
      }
      return this->prefix_fragment_.size() + sizeof(SmallNode) +
             this->branch_count_ * (/*branch key byte*/ 1 + sizeof(Branch));
    }

    void set_branch_count(SmallNode* node) const
    {
      BATT_CHECK_LE(this->branch_count_, 64);
      node->branch_count = BATT_CHECKED_CAST(u8, this->branch_count_);
    }

    Status initialize_branches(SmallNode* node, MutableBuffer& dst) const
    {
      BATT_CHECK_EQ(node->branch_count, this->branch_count_);

      Slice<u8> index = node->get_index();
      Slice<Branch> branches = node->get_branches();

      BATT_REQUIRE_OK(this->for_each_branch(
          [&index, &branches, &dst, node, this](usize i, u8 branch_byte,   //
                                                const Iter& branch_first,  //
                                                const Iter& branch_last) -> Status {
            index[i] = branch_byte;

            StatusOr<NodeBase*> branch_root = this->build_subtree(branch_first, branch_last, dst);
            BATT_REQUIRE_OK(branch_root);

            branches[i].reset_unsafe(*branch_root);

            return OkStatus();
          }));

      return OkStatus();
    }

    //----- --- -- -  -  -   -

    usize get_space_needed(batt::StaticType<MediumNode>) const
    {
      return this->prefix_fragment_.size() + sizeof(MediumNode) +
             this->branch_count_ * sizeof(Branch);
    }

    void set_branch_count(MediumNode* node) const
    {
      BATT_CHECK_LE(this->branch_count_, 256);
      node->branch_count = BATT_CHECKED_CAST(u8, this->branch_count_);
    }

    Status initialize_branches(MediumNode* node, MutableBuffer& dst) const
    {
      // Clear the bit set of branch key bytes.
      //
      std::memset(node->branch_set.data(), 0, sizeof(node->branch_set));

      Slice<Branch> branches = node->get_branches();

      BATT_REQUIRE_OK(
          this->for_each_branch([&branches, &dst, node, this](usize i, u8 branch_byte,   //
                                                              const Iter& branch_first,  //
                                                              const Iter& branch_last) -> Status {
            const usize set_index = branch_byte / 8;
            const usize set_bit_i = branch_byte % 8;

            node->branch_set[set_index] |= u8{1} << set_bit_i;

            StatusOr<NodeBase*> branch_root = this->build_subtree(branch_first, branch_last, dst);
            BATT_REQUIRE_OK(branch_root);

            branches[i].reset_unsafe(*branch_root);

            return OkStatus();
          }));

      return OkStatus();
    }

    //----- --- -- -  -  -   -

    usize get_space_needed(batt::StaticType<LargeNode>) const
    {
      return this->prefix_fragment_.size() + sizeof(LargeNode);
    }

    void set_branch_count(LargeNode* node [[maybe_unused]]) const
    {
      BATT_CHECK_LE(this->branch_count_, 256);
    }

    Status initialize_branches(LargeNode* node, MutableBuffer& dst) const
    {
      usize j = 0;

      BATT_REQUIRE_OK(
          this->for_each_branch([&j, &dst, node, this](usize /*i*/, u8 branch_byte,  //
                                                       const Iter& branch_first,     //
                                                       const Iter& branch_last) -> Status {
            for (; j < branch_byte; ++j) {
              node->branches[j].offset = 0;
            }

            StatusOr<NodeBase*> branch_root = this->build_subtree(branch_first, branch_last, dst);
            BATT_REQUIRE_OK(branch_root);

            node->branches[j].reset_unsafe(*branch_root);
            ++j;

            return OkStatus();
          }));

      for (; j < 256; ++j) {
        node->branches[j].offset = 0;
      }

      return OkStatus();
    }

    //----- --- -- -  -  -   -

    StatusOr<NodeBase*> build_subtree(const Iter& subtree_first, const Iter& subtree_last,
                                      MutableBuffer& dst) const
    {
      NodeBuilder subtree_builder{subtree_first, subtree_last, this->get_key_fn_,
                                  this->prefix_len_ + 1};

      return subtree_builder.build(dst);
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    Iter first_;
    Iter last_;
    const GetKeyFn& get_key_fn_;
    usize base_prefix_len_;
    std::string_view min_key_;
    std::string_view max_key_;
    usize prefix_len_;
    std::string_view prefix_fragment_;
    batt::SmallVec<BranchPivot, 256> pivots_;
    usize branch_count_;
  };
};

template <typename Iter, typename GetKeyFn>
inline StatusOr<PackedART::NodeBase*> pack_art(Iter first, Iter last, const GetKeyFn& get_key_fn,
                                               MutableBuffer& dst, usize base_prefix_len = 0)
{
  PackedART::NodeBuilder<Iter, GetKeyFn> root_builder{first, last, get_key_fn, base_prefix_len};

  return root_builder.build(dst);
}

}  //namespace llfs
