//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_UNPACK_CAST_HPP
#define LLFS_UNPACK_CAST_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/status.hpp>
#include <llfs/status_code.hpp>

#include <batteries/type_traits.hpp>

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Must be in the top-level namespace.
//
template <typename T>
batt::Status llfs_validate_packed_value_helper(const T& packed, const batt::ConstBuffer& buffer);

namespace boost {
namespace endian {

template <::boost::endian::order kOrder, typename T, ::llfs::usize kNBits,
          ::boost::endian::align kAlign>
::batt::Status validate_packed_value(
    const ::boost::endian::endian_buffer<kOrder, T, kNBits, kAlign>&, const void* /*buffer_data*/,
    ::llfs::usize buffer_size)
{
  using PackedT = ::boost::endian::endian_buffer<kOrder, T, kNBits, kAlign>;
  if (sizeof(PackedT) != buffer_size) {
    return ::llfs::make_status(::llfs::StatusCode::kUnpackCastWrongIntegerSize);
  }

  return ::batt::OkStatus();
}

template <::boost::endian::order kOrder, typename T, ::llfs::usize kNBits>
::batt::Status validate_packed_value(
    const ::boost::endian::endian_arithmetic<kOrder, T, kNBits>& packed, const void* buffer_data,
    ::llfs::usize buffer_size)
{
  static_assert(sizeof(char) == 1, "");

  const char* const packed_begin = reinterpret_cast<const char*>(&packed);
  const char* const buffer_begin = reinterpret_cast<const char*>(buffer_data);

  if (packed_begin < buffer_begin) {
    return ::llfs::make_status(::llfs::StatusCode::kUnpackCastIntegerOutOfBounds);
  }

  using PackedT = ::boost::endian::endian_arithmetic<kOrder, T, kNBits>;
  const ::llfs::usize packed_size = buffer_size - (packed_begin - buffer_begin);
  if (sizeof(PackedT) != packed_size) {
    return ::llfs::make_status(::llfs::StatusCode::kUnpackCastWrongIntegerSize);
  }

  return ::batt::OkStatus();
}

}  // namespace endian
}  // namespace boost

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace llfs {

template <typename T, typename DataT>
StatusOr<const T&> unpack_cast(const DataT& data, batt::StaticType<T> = {})
{
  ConstBuffer buffer = batt::as_const_buffer(data);
  if (buffer.data() == nullptr) {
    return make_status(StatusCode::kUnpackCastNullptr);
  }
  const T* packed = reinterpret_cast<const T*>(buffer.data());
  Status validation_status = ::llfs_validate_packed_value_helper(*packed, buffer);
  BATT_REQUIRE_OK(validation_status);

  return *packed;
}

template <typename PackedStructT>
batt::Status validate_packed_struct(
    const PackedStructT& packed_struct, const void* buffer_data, usize buffer_size,
    StatusCode under_code = ::llfs::StatusCode::kUnpackCastStructUnder,
    StatusCode over_code = ::llfs::StatusCode::kUnpackCastStructOver)
{
  if (static_cast<const void*>(&packed_struct) < buffer_data) {
    return ::llfs::make_status(under_code);
  }

  const void* buffer_end = static_cast<const u8*>(buffer_data) + buffer_size;
  if (static_cast<const void*>((&packed_struct) + 1) > buffer_end) {
    return ::llfs::make_status(over_code);
  }

  return batt::OkStatus();
}

}  // namespace llfs

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Must be in the top-level namespace.
//
template <typename T>
batt::Status llfs_validate_packed_value_helper(const T& packed, const batt::ConstBuffer& buffer)
{
  return validate_packed_value(packed, buffer.data(), buffer.size());
}

//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#endif  // LLFS_UNPACK_CAST_HPP
