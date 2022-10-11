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

namespace llfs {

template <boost::endian::order kOrder, typename T, usize kNBits, boost::endian::align kAlign>
Status validate_packed_value(const boost::endian::endian_buffer<kOrder, T, kNBits, kAlign>&,
                             const ConstBuffer& buffer)
{
  using PackedT = boost::endian::endian_buffer<kOrder, T, kNBits, kAlign>;
  if (sizeof(PackedT) != buffer.size()) {
    return make_status(StatusCode::kUnpackCastWrongIntegerSize);
  }

  return OkStatus();
}

template <boost::endian::order kOrder, typename T, usize kNBits>
Status validate_packed_value(const boost::endian::endian_arithmetic<kOrder, T, kNBits>&,
                             const ConstBuffer& buffer)
{
  using PackedT = boost::endian::endian_arithmetic<kOrder, T, kNBits>;
  if (sizeof(PackedT) != buffer.size()) {
    return make_status(StatusCode::kUnpackCastWrongIntegerSize);
  }

  return OkStatus();
}

template <typename T, typename DataT>
StatusOr<const T*> unpack_cast(const DataT& data, batt::StaticType<T> = {})
{
  ConstBuffer buffer = batt::as_const_buffer(data);
  if (buffer.data() == nullptr) {
    return make_status(StatusCode::kUnpackCastNullptr);
  }
  const T* packed = reinterpret_cast<const T*>(buffer.data());
  Status validation_status = validate_packed_value(*packed, buffer);
  BATT_REQUIRE_OK(validation_status);

  return packed;
}

}  // namespace llfs

#endif  // LLFS_UNPACK_CAST_HPP
