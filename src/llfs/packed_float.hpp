//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_FLOAT_HPP
#define LLFS_PACKED_FLOAT_HPP

#include <llfs/int_types.hpp>

#include <batteries/assert.hpp>
#include <batteries/static_assert.hpp>

#include <iostream>

namespace llfs {

//+++++++++++-+-+--+----- --- -- -  -  -   -
namespace float_types {

using f32 = float;
using f64 = double;

}  // namespace float_types

using namespace float_types;

// Portably encodes the passed floating-point value according to the IEEE 754 standard.
//
u32 ieee_754_encode_32(f32 v);

// Portably decodes the passed IEEE 754 encoded 32-bit value to a native floating point value.
//
f32 ieee_754_decode_32(u32 v);

// Portably encodes the passed floating-point value according to the IEEE 754 standard.
//
u64 ieee_754_encode_64(f64 v);

// Portably decodes the passed IEEE 754 encoded 64-bit value to a native floating point value.
//
f64 ieee_754_decode_64(u64 v);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class little_f32
{
 public:
  little_f32() noexcept
  {
  }

  /*implicit*/ little_f32(f32 v) noexcept : packed_bits_{ieee_754_encode_32(v)}
  {
  }

  operator f32() const noexcept
  {
    return ieee_754_decode_32(this->packed_bits_);
  }

  u32 int_value() const noexcept
  {
    return this->packed_bits_;
  }

 private:
  little_u32 packed_bits_;
};

BATT_STATIC_ASSERT_EQ(sizeof(little_f32), 4);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class big_f32
{
 public:
  big_f32() noexcept
  {
  }

  /*implicit*/ big_f32(f32 v) noexcept : packed_bits_{ieee_754_encode_32(v)}
  {
  }

  operator f32() const noexcept
  {
    return ieee_754_decode_32(this->packed_bits_);
  }

  u32 int_value() const noexcept
  {
    return this->packed_bits_;
  }

 private:
  big_u32 packed_bits_;
};

BATT_STATIC_ASSERT_EQ(sizeof(big_f32), 4);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class little_f64
{
 public:
  little_f64() noexcept
  {
  }

  /*implicit*/ little_f64(f64 v) noexcept : packed_bits_{ieee_754_encode_64(v)}
  {
  }

  operator f64() const noexcept
  {
    return ieee_754_decode_64(this->packed_bits_);
  }

  u64 int_value() const noexcept
  {
    return this->packed_bits_;
  }

 private:
  little_u64 packed_bits_;
};

BATT_STATIC_ASSERT_EQ(sizeof(little_f64), 8);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class big_f64
{
 public:
  big_f64() noexcept
  {
  }

  /*implicit*/ big_f64(f64 v) noexcept : packed_bits_{ieee_754_encode_64(v)}
  {
  }

  operator f64() const noexcept
  {
    return ieee_754_decode_64(this->packed_bits_);
  }

  u64 int_value() const noexcept
  {
    return this->packed_bits_;
  }

 private:
  big_u64 packed_bits_;
};

BATT_STATIC_ASSERT_EQ(sizeof(big_f64), 8);

}  // namespace llfs

#endif  // LLFS_PACKED_FLOAT_HPP
