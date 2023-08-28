//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_VARIANT_HPP
#define LLFS_PACKED_VARIANT_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_id.hpp>
#include <llfs/seq.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/assert.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/static_dispatch.hpp>
#include <batteries/tuples.hpp>
#include <batteries/utility.hpp>

#include <limits>
#include <tuple>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename... Ts>
struct PackedVariant {
  static_assert(sizeof...(Ts) < 256,
                "PackedVariant type limit of 256 exceeded; increase the integer size of "
                "`PackedVariant::which` or use fewer types");

  using tuple_type = std::tuple<Ts...>;

  static constexpr usize kNumCases = sizeof...(Ts);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  little_u8 which;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PackedVariant(const PackedVariant&) = delete;
  PackedVariant& operator=(const PackedVariant&) = delete;

  explicit PackedVariant(unsigned n) noexcept
  {
    this->init(n);
  }

  template <typename T>
  void init(batt::StaticType<T> = {});

  void init(unsigned n)
  {
    BATT_CHECK_LT(n, kNumCases) << "PackedVariant case out-of-bounds";

    this->which = n;
  }

  template <typename Fn>
  decltype(auto) visit(Fn&& visitor) const
  {
    const void* value_ptr = this + 1;

    return this->visit_type([value_ptr, &visitor](auto static_type) mutable -> decltype(auto) {
      using T = typename decltype(static_type)::type;
      return BATT_FORWARD(visitor)(*reinterpret_cast<const T*>(value_ptr));
    });
  }

  template <typename Fn>
  decltype(auto) visit_type(Fn&& visitor) const
  {
    return batt::static_dispatch<tuple_type>(this->which.value(), BATT_FORWARD(visitor));
  }

  /** \brief If the variant case is the given type, returns a pointer to the packed T; else returns
   * nullptr.
   */
  template <typename T>
  const T* as(batt::StaticType<T>) const noexcept;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVariant<>), 1);

template <typename T>
struct IsPackedVariantImpl : std::false_type {
};

template <typename... Ts>
struct IsPackedVariantImpl<PackedVariant<Ts...>> : std::true_type {
};

template <typename T>
using IsPackedVariant = IsPackedVariantImpl<std::decay_t<T>>;

/** \brief Returns the index of `CaseT` within the list of allowed types in `VariantT`.
 */
template <typename VariantT, typename CaseT,
          typename = std::enable_if_t<IsPackedVariant<VariantT>{}>>
inline constexpr u8 index_of_type_within_packed_variant(batt::StaticType<VariantT> = {},
                                                        batt::StaticType<CaseT> = {})
{
  constexpr usize value = batt::TupleIndexOf_v<typename VariantT::tuple_type, CaseT>;

  static_assert(VariantT::kNumCases - 1 <= std::numeric_limits<u8>::max(),
                "There are too many case types for the PackedVariant type VariantT; coercing to u8 "
                "will lose information");

  static_assert(value < VariantT::kNumCases,
                "The specified CaseT is not one of the members of the PackedVariant type VariantT");

  return static_cast<u8>(value);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... Ts>
template <typename T>
void PackedVariant<Ts...>::init(batt::StaticType<T>)
{
  constexpr unsigned kWhich = index_of_type_within_packed_variant<PackedVariant<Ts...>, T>();
  this->init(kWhich);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... Ts>
template <typename T>
const T* PackedVariant<Ts...>::as(batt::StaticType<T>) const noexcept
{
  if (this->which == index_of_type_within_packed_variant<PackedVariant<Ts...>, T>()) {
    return reinterpret_cast<const T*>(this + 1);
  }
  return nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename... Ts>
usize packed_sizeof(const PackedVariant<Ts...>& var)
{
  return sizeof(PackedVariant<Ts...>) + var.visit([](const auto& value) -> usize {
    return packed_sizeof(value);
  });
}

template <typename... Ts>
batt::Status validate_packed_value(const PackedVariant<Ts...>& var, const void* buffer_data,
                                   usize buffer_size)
{
  const char* buffer_begin = static_cast<const char*>(buffer_data);
  const char* buffer_end = buffer_begin + buffer_size;
  const char* var_begin = reinterpret_cast<const char*>(&var);
  const char* var_end = reinterpret_cast<const char*>(&var) + sizeof(PackedVariant<Ts...>);

  BATT_CHECK_LE((void*)buffer_begin, (void*)var_begin);
  BATT_CHECK_LT((void*)var_begin, (void*)var_end);

  if (var_end > buffer_end) {
    return ::llfs::make_status(StatusCode::kUnpackCastVariantStructOutOfBounds);
  }

  return var.visit([&](const auto& case_instance) {
    return ::llfs_validate_packed_value_helper(case_instance,
                                               ConstBuffer{buffer_data, buffer_size});
  });
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename PackedVarT, typename T>
struct PackedVariantInstance;

template <typename... Ts, typename T>
struct PackedVariantInstance<PackedVariant<Ts...>, T> {
  using Self = PackedVariantInstance;
  using tuple_type = std::tuple<Ts...>;
  using head_type = PackedVariant<Ts...>;
  using tail_type = T;

  static_assert(std::is_same_v<tuple_type, typename PackedVariant<Ts...>::tuple_type>, "");

  static constexpr unsigned kWhich = batt::TupleIndexOf_v<tuple_type, T>;

  PackedVariant<Ts...> head;
  T tail;

  template <typename... Args, typename = batt::EnableIfNoShadow<PackedVariantInstance, Args...>>
  PackedVariantInstance(Args&&... args) noexcept : head{kWhich}
                                                 , tail{BATT_FORWARD(args)...}
  {
    BATT_STATIC_ASSERT_EQ(sizeof(PackedVariantInstance), sizeof(head_type) + sizeof(tail_type));
  }

  void init()
  {
    BATT_STATIC_ASSERT_EQ(sizeof(PackedVariantInstance), sizeof(head_type) + sizeof(tail_type));
    this->head.init(kWhich);
  }

  bool verify_case() const
  {
    return Self::kWhich == this->head.which;
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Wrapper for type `T` that causes the object to be packed as an instance of the given
// PackedVariant.
//
// NOTE: `T` is the unpacked type!
//
template <typename PackedVarT, typename T>
struct PackAsVariant;

template <typename... Ts, typename T>
struct PackAsVariant<PackedVariant<Ts...>, T> {
  T object;
};

// Returns a copy/ref of `object` (depending on whether `object` is an rvalue) that will be packed
// as a PackedVariant instance.
//
template <typename PackedVarT, typename T>
auto pack_as_variant(T&& object)
{
  return PackAsVariant<PackedVarT, T>{BATT_FORWARD(object)};
}

// Same as `pack_as_variant(object)` except the variant type is given as a `batt::StaticType`
// instead of as an explicit template parameter.
//
template <typename... Ts, typename T>
PackAsVariant<PackedVariant<Ts...>, T> pack_as_variant(batt::StaticType<PackedVariant<Ts...>>,
                                                       T&& object)
{
  return pack_as_variant<PackedVariant<Ts...>, T>(BATT_FORWARD(object));
}

template <typename... Ts, typename T>
inline usize packed_sizeof(const PackAsVariant<PackedVariant<Ts...>, T>& p)
{
  return sizeof(PackedVariant<Ts...>) + packed_sizeof(p.object);
}

template <typename... Ts, typename T, typename R = decltype(trace_refs(std::declval<const T&>()))>
inline R trace_refs(const PackAsVariant<PackedVariant<Ts...>, T>& p)
{
  return trace_refs(p.object);
}

template <typename... Ts, typename T, typename Dst>
PackedVariant<Ts...>* pack_object(const PackAsVariant<PackedVariant<Ts...>, T>& p, Dst* dst)
{
  // Allocate space for Variant header
  auto* packed_var = dst->template pack_record<PackedVariant<Ts...>>();
  if (!packed_var) {
    return nullptr;
  }

  // Set variant type now (examples: PackedEdit or PackedTabletCheckpoint)
  packed_var->init(batt::StaticType<PackedTypeFor<std::decay_t<T>>>{});

  auto* packed_case = pack_object(p.object, dst);
  if (!packed_case) {
    return nullptr;
  }

  return packed_var;
}

}  // namespace llfs

#endif  // LLFS_PACKED_VARIANT_HPP
