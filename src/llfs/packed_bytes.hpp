//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_BYTES_HPP
#define LLFS_PACKED_BYTES_HPP

#include <llfs/data_layout.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/static_assert.hpp>

#include <cstring>
#include <string>
#include <string_view>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
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

  void clear() noexcept
  {
    this->data_offset = sizeof(PackedBytes);
    this->unused_[0] = 0;
    this->data_size = 0;
    this->reserved_[0] = 0;
  }

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

BATT_STATIC_ASSERT_EQ(sizeof(PackedBytes), 8);

inline std::string_view as_str(const PackedBytes* rec)
{
  return rec->as_str();
}
inline std::string_view as_str(const PackedBytes& rec)
{
  return as_str(&rec);
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

inline usize packed_sizeof(std::string_view& s)
{
  return packed_sizeof(static_cast<const std::string_view&>(s));
}

inline usize packed_sizeof(const std::string& s)
{
  return packed_sizeof_str(s.size());
}

inline usize packed_sizeof(const PackedBytes& rec)
{
  return packed_sizeof_str(rec.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

class DataPacker;
class DataReader;

StatusOr<std::string_view> unpack_object(const PackedBytes& obj, DataReader* src);

PackedBytes* pack_object_to(const std::string_view& from, PackedBytes* to, DataPacker* dst);

template <>
struct DefinePackedTypeFor<std::string> {
  using type = PackedBytes;
};

template <>
struct DefinePackedTypeFor<std::string_view> {
  using type = PackedBytes;
};

static_assert(std::is_same_v<PackedBytes, PackedTypeFor<std::string_view>>, "");
static_assert(std::is_same_v<PackedBytes, PackedTypeFor<std::string>>, "");

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

batt::Status validate_packed_value(const PackedBytes& packed_bytes, const void* buffer_data,
                                   usize buffer_size);

}  // namespace llfs

#endif  // LLFS_PACKED_BYTES_HPP
