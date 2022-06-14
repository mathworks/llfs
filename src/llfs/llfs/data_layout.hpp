#pragma once
#ifndef LLFS_DATA_LAYOUT_HPP
#define LLFS_DATA_LAYOUT_HPP

#include <glog/logging.h>

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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PackedBytes {
  // Offset in bytes of the start of the packed data, relative to the start of
  // this record.  If this value is less than 8 (sizeof(PackedBytes)), then `data_size` is invalid;
  // in this case, the data is presumed to extend to the end of this record.
  //
  little_u24 data_offset;

  // Not used by this record.
  //
  little_u8 unused_[1];

  // The size in bytes of the packed data.  MAY BE INVALID; always use `PackedBytes::size()` when
  // reading size instead of accessing this field directly.
  //
  little_u24 data_size;

  // Reserved for future use.
  //
  little_u8 reserved_[1];

  // This struct must never be copied since that would invalidate `data_offset`.
  //
  PackedBytes(const PackedBytes&) = delete;
  PackedBytes& operator=(const PackedBytes&) = delete;

  const void* data() const
  {
    return reinterpret_cast<const u8*>(this) + this->data_offset;
  }

  usize size() const
  {
    if (this->data_offset < sizeof(PackedBytes)) {
      return sizeof(PackedBytes) - this->data_offset;
    }
    return this->data_size;
  }

  std::string_view as_str() const
  {
    return std::string_view(static_cast<const char*>(this->data()), this->size());
  }

  boost::iterator_range<const u8*> bytes_range() const
  {
    const u8* data_begin = static_cast<const u8*>(this->data());
    const u8* data_end = data_begin + this->size();
    return {data_begin, data_end};
  }
};

static_assert(sizeof(PackedBytes) == 8, "");

inline std::string_view as_str(const PackedBytes* rec)
{
  return rec->as_str();
}
inline std::string_view as_str(const PackedBytes& rec)
{
  return as_str(&rec);
}

template <typename T>
inline constexpr usize packed_sizeof(batt::StaticType<T> = {})
{
  return sizeof(T);
}

inline usize packed_sizeof_str_data(usize len)
{
  if (len <= 4) {
    return 0;
  }
  return len;
}

inline usize packed_sizeof_str(usize len)
{
  return sizeof(PackedBytes) + packed_sizeof_str_data(len);
}

inline usize packed_sizeof(const std::string_view& s)
{
  return packed_sizeof_str(s.size());
}

inline usize packed_sizeof(const PackedBytes& rec)
{
  return packed_sizeof_str(rec.size());
}

struct GetPackedSizeof {
  template <typename T>
  usize operator()(const T& val) const
  {
    return packed_sizeof(val);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedSmallPad {
  little_u8 size;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedSmallPad), 1);

struct PackedLargePad {
  little_u64 size;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLargePad), 8);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline constexpr usize packed_sizeof_varint(u64 n)
{
  return (n == 0) ? 1 : (64 - __builtin_clzll(n) + 6) / 7;
}

BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 1)) - 1), 1);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 1)) - 0), 2);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 2)) - 1), 2);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 2)) - 0), 3);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 3)) - 1), 3);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 3)) - 0), 4);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 4)) - 1), 4);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 4)) - 0), 5);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 5)) - 1), 5);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 5)) - 0), 6);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 6)) - 1), 6);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 6)) - 0), 7);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 7)) - 1), 7);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 7)) - 0), 8);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 8)) - 1), 8);
BATT_STATIC_ASSERT_EQ(packed_sizeof_varint((1ull << (7 * 8)) - 0), 9);

constexpr usize kMaxSlotHeaderSize = 5;

}  // namespace llfs

#endif  // LLFS_DATA_LAYOUT_HPP
