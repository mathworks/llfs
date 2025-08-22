//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_float.hpp>
//

#include <batteries/type_traits.hpp>

#include <cfloat>
#include <cmath>
#include <limits>
#include <type_traits>

namespace llfs {

namespace ieee_754 {

template <typename Integer>
struct BinaryLayout;

template <>
struct BinaryLayout<u32> {
  static constexpr i32 kExponentBits = 8;
  static constexpr i32 kExponentShift = 23;
  static constexpr i32 kExponentBias = 127;
  static constexpr i32 kExponentUnbiasedMin = -127;
  static constexpr i32 kExponentUnbiasedMax = 128;
  static constexpr i32 kSignShift = 31;
  static constexpr u32 kSignMask = u32{1} << kSignShift;
  static constexpr u32 kExponentMax = (u32{1} << kExponentBits) - 1;
  static constexpr u32 kFractionMask = (u32{1} << kExponentShift) - 1;
  static constexpr f32 kSignificandMax = f32(i32{1} << kExponentShift) + 0.5;
  static constexpr u32 kPositiveInfinity = kExponentMax << kExponentShift;
  static constexpr u32 kNegativeInfinity = kPositiveInfinity | kSignMask;
};

template <>
struct BinaryLayout<u64> {
  static constexpr i32 kExponentBits = 11;
  static constexpr i32 kExponentShift = 52;
  static constexpr i32 kExponentBias = 1023;
  static constexpr i32 kExponentUnbiasedMin = -1023;
  static constexpr i32 kExponentUnbiasedMax = 1024;
  static constexpr i32 kSignShift = 63;
  static constexpr u64 kSignMask = u64{1} << kSignShift;
  static constexpr u64 kExponentMax = (u64{1} << kExponentBits) - 1;
  static constexpr u64 kFractionMask = (u64{1} << kExponentShift) - 1;
  static constexpr f64 kSignificandMax = f64(i64{1} << kExponentShift) + 0.5;
  static constexpr u64 kPositiveInfinity = kExponentMax << kExponentShift;
  static constexpr u64 kNegativeInfinity = kPositiveInfinity | kSignMask;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Integer, typename Real>
Integer encode_as(Real value)
{
  static_assert(std::is_same_v<Real, std::decay_t<Real>>);

  using Layout = BinaryLayout<Integer>;

  BATT_STATIC_ASSERT_EQ(sizeof(Integer), sizeof(Real));

  union {
    Real r;
    Integer i;
  } copy;

  copy.r = value;

  if ((std::isinf(copy.r) && !std::signbit(copy.r)) || (copy.i == Layout::kPositiveInfinity)) {
    return Layout::kPositiveInfinity;
  }

  if ((std::isinf(copy.r) && std::signbit(copy.r)) || (copy.i == Layout::kNegativeInfinity)) {
    return Layout::kNegativeInfinity;
  }

  Integer bits = 0;

  // If value is negative, set the sign bit and encode the remaining bits as though value were
  // positive.
  //
  if (value < 0.0) {
    bits |= Layout::kSignMask;
    value = -value;
  }

  if (value != 0.0) {
    i32 unbiased_exponent = 0;

    // Find the (unbiased) exponent value by repeated division/multiplication.  (Mathematically,
    // only one of these loops should execute for a given input, but we'll just let the compiler's
    // optimizer sort everything out rather than trying to hand-optimize the control flow.)
    //
    while (value >= 2.0) {
      value /= 2.0;
      unbiased_exponent += 1;
    }
    while (value < 1.0) {
      value *= 2.0;
      unbiased_exponent -= 1;
    }

    // Handle exponent out-of-bounds cases.
    //
    if (unbiased_exponent <= Layout::kExponentUnbiasedMin) {
      do {
        value /= 2.0;
        unbiased_exponent += 1;
      } while (unbiased_exponent <= Layout::kExponentUnbiasedMin);
      unbiased_exponent = Layout::kExponentUnbiasedMin;
    } else if (unbiased_exponent > Layout::kExponentUnbiasedMax) {
      unbiased_exponent = Layout::kExponentUnbiasedMax;
    } else {
      // We only need to store the fractional part.
      //
      value -= 1.0;
    }

    // Construct the final encoded value.
    //
    bits |= Integer(unbiased_exponent + Layout::kExponentBias) << Layout::kExponentShift;
    bits |= Integer(Layout::kSignificandMax * value) & Layout::kFractionMask;
  }
  return bits;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Real, typename Integer>
Real decode_as(Integer bits)
{
  BATT_STATIC_ASSERT_EQ(sizeof(Integer), sizeof(Real));

  using Layout = BinaryLayout<Integer>;

  if (bits == Layout::kPositiveInfinity) {
    return std::numeric_limits<Real>::infinity();
  }

  if (bits == Layout::kNegativeInfinity) {
    return -std::numeric_limits<Real>::infinity();
  }

  static const Real sign_multiplier[2] = {1.0, -1.0};

  const Real sign = sign_multiplier[(bits >> Layout::kSignShift) & Integer{1}];
  const Integer biased_exponent = (bits >> Layout::kExponentShift) & Layout::kExponentMax;

  // Zero is a special case.
  //
  if ((bits & Layout::kFractionMask) == 0 && biased_exponent == 0) {
    return sign * 0.0;
  }

  const Real fraction = Real(bits & Layout::kFractionMask) / Layout::kSignificandMax;

  Real significand = 1.0 + fraction;

  i32 unbiased_exponent = static_cast<i32>(biased_exponent) - Layout::kExponentBias;

  while (unbiased_exponent > 0) {
    unbiased_exponent -= 1;
    significand *= 2.0;
  }
  while (unbiased_exponent < 0) {
    unbiased_exponent += 1;
    significand /= 2.0;
  }

  return sign * significand;
}

}  // namespace ieee_754

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u32 ieee_754_encode_32(f32 value)
{
  return ieee_754::encode_as<u32>(value);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
f32 ieee_754_decode_32(u32 bits)
{
  return ieee_754::decode_as<f32>(bits);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 ieee_754_encode_64(f64 value)
{
  return ieee_754::encode_as<u64>(value);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
f64 ieee_754_decode_64(u64 bits)
{
  return ieee_754::decode_as<f64>(bits);
}

}  // namespace llfs
