//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_DATA_LAYOUT_HPP
#define LLFS_DATA_LAYOUT_HPP

#include <llfs/logging.hpp>

#include <llfs/buffer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/seq.hpp>
#include <llfs/status.hpp>

#include <batteries/assert.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/static_dispatch.hpp>
#include <batteries/tuples.hpp>
#include <batteries/type_traits.hpp>

#include <boost/operators.hpp>

#include <string_view>
#include <type_traits>

namespace llfs {
template <typename T, typename Src, typename PackedType = PackedTypeFor<T>>
[[nodiscard]] StatusOr<T> read_object(Src* src, batt::StaticType<T>)
{
  const auto* packed_rec = src->template read_record<PackedType>();
  if (!packed_rec) {
    return Status{batt::StatusCode::kOutOfRange};
  }

  return unpack_object(*packed_rec, src);
}

template <typename T, typename Dst,
          typename Result = decltype(pack_object_to(
              std::declval<T>(), std::declval<PackedTypeFor<T>*>(), std::declval<Dst*>()))>
[[nodiscard]] Result pack_object(T&& obj, Dst* dst)
{
  auto* packed_rec = dst->pack_record(batt::StaticType<PackedTypeFor<T>>{});
  if (!packed_rec) {
    return Result{};
  }
  return pack_object_to(BATT_FORWARD(obj), packed_rec, dst);
}

template <typename T, typename Src,
          typename = std::enable_if_t<std::is_same_v<PackedTypeFor<T>, T>>>
StatusOr<std::reference_wrapper<const T>> unpack_object(const T& obj, Src*)
{
  return obj;
}

#define LLFS_DEFINE_INT_PACK_OBJECT_TO(type, packed_type)                                          \
  template <typename Dst>                                                                          \
  inline packed_type* pack_object_to(type from, packed_type* to, Dst*)                             \
  {                                                                                                \
    *to = from;                                                                                    \
    return to;                                                                                     \
  }                                                                                                \
                                                                                                   \
  template <typename Src>                                                                          \
  inline StatusOr<type> unpack_object(const packed_type& packed, Src*)                             \
  {                                                                                                \
    return packed.value();                                                                         \
  }

LLFS_DEFINE_INT_PACK_OBJECT_TO(i8, little_i8)
LLFS_DEFINE_INT_PACK_OBJECT_TO(i16, little_i16)
LLFS_DEFINE_INT_PACK_OBJECT_TO(i32, little_i32)
LLFS_DEFINE_INT_PACK_OBJECT_TO(i64, little_i64)

LLFS_DEFINE_INT_PACK_OBJECT_TO(u8, little_u8)
LLFS_DEFINE_INT_PACK_OBJECT_TO(u16, little_u16)
LLFS_DEFINE_INT_PACK_OBJECT_TO(u32, little_u32)
LLFS_DEFINE_INT_PACK_OBJECT_TO(u64, little_u64)

struct GetPackedSizeof {
  template <typename T>
  usize operator()(const T& val) const
  {
    return packed_sizeof(val);
  }
};

template <typename T>
inline constexpr usize packed_sizeof(batt::StaticType<T> = {})
{
  return sizeof(T);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

}  // namespace llfs

#endif  // LLFS_DATA_LAYOUT_HPP
