//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_INTERVAL_HPP
#define LLFS_PACKED_INTERVAL_HPP

#include <llfs/buffer.hpp>
#include <llfs/data_packer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/interval.hpp>
#include <batteries/static_assert.hpp>

#include <ostream>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Generic packed representation of an interval.
 */
template <typename Traits>
struct PackedBasicInterval {
  /** \brief This class.
   */
  using Self = PackedBasicInterval;

  /** \brief The packed lower bound representation.
   */
  using lower_bound_type = PackedTypeFor<typename Traits::lower_bound_type>;

  /** \brief The packed upper bound representation.
   */
  using upper_bound_type = PackedTypeFor<typename Traits::upper_bound_type>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The lower bound of the interval.
   */
  lower_bound_type lower_bound;

  /** \brief The upper bound of the interval.
   */
  upper_bound_type upper_bound;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a function that can be printed via std::ostream to dump detailed information
   * about this object.
   */
  auto debug_dump(const void* base) const
  {
    return [base, this](std::ostream& out) {
      out << "[" << llfs::byte_distance(base, this) << ".." << llfs::byte_distance(base, this + 1)
          << "] " << ::batt::name_of<Self>() << "{.lower_bound=" << this->lower_bound.value()
          << ", .upper_bound=" << this->upper_bound.value() << ",}";
    };
  }
};

/** \brief Returns the packed size of the given interval.
 */
template <typename Traits>
constexpr usize packed_sizeof(const PackedBasicInterval<Traits>&)
{
  return sizeof(PackedBasicInterval<Traits>);
}

/** \brief Returns the packed size of the given interval.
 */
template <typename Traits>
constexpr usize packed_sizeof(const ::batt::BasicInterval<Traits>&)
{
  return sizeof(PackedBasicInterval<Traits>);
}

/** \brief Packs the given interval into the specified structure.
 */
template <typename Traits>
PackedBasicInterval<Traits>* pack_object_to(const ::batt::BasicInterval<Traits>& object,
                                            PackedBasicInterval<Traits>* packed_object,
                                            DataPacker* dst)
{
  if (!pack_object_to(object.lower_bound, &packed_object->lower_bound, dst)) {
    return nullptr;
  }
  if (!pack_object_to(object.upper_bound, &packed_object->upper_bound, dst)) {
    return nullptr;
  }

  return packed_object;
}

/** \brief Prints the given packed interval using standard mathematical notation.
 */
template <typename Traits>
std::ostream& operator<<(std::ostream& out, const PackedBasicInterval<Traits>& t)
{
  return out << Traits::left_bracket() << t.lower_bound << ", " << t.upper_bound
             << Traits::right_bracket();
}

/** \brief The packed representation of a batt::Interval<T>.
 */
template <typename T>
using PackedInterval = PackedBasicInterval<::batt::IClosedOpen<UnpackedTypeFor<T>>>;

/** \brief The packed representation of a batt::CInterval<T>.
 */
template <typename T>
using PackedCInterval = PackedBasicInterval<::batt::IClosed<UnpackedTypeFor<T>>>;

/** \brief Validates buffer bounds for the given packed interval to enable llfs::unpack_cast.
 */
template <typename Traits>
inline batt::Status validate_packed_value(const llfs::PackedBasicInterval<Traits>& packed,
                                          const void* buffer_data, std::size_t buffer_size)
{
  return validate_packed_struct(packed, buffer_data, buffer_size);
}

}  // namespace llfs

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace batt {

/** \brief Defines llfs::PackedBasicInterval to be the packed representation of batt::BasicInterval.
 */
template <typename Traits>
[[maybe_unused]] inline ::batt::StaticType<::llfs::PackedBasicInterval<Traits>>
    llfs_packed_type_for(::batt::StaticType<::batt::BasicInterval<Traits>>)
{
  return {};
}

}  // namespace batt

/** \brief Verify that ADL and overload resolution are working correctly for PackedInterval<T>.
 */
BATT_STATIC_ASSERT_TYPE_EQ(llfs::PackedTypeFor<batt::Interval<batt::u64>>,
                           llfs::PackedInterval<llfs::little_u64>);

/** \brief Verify that ADL and overload resolution are working correctly for PackedCInterval<T>.
 */
BATT_STATIC_ASSERT_TYPE_EQ(llfs::PackedTypeFor<batt::CInterval<batt::i32>>,
                           llfs::PackedCInterval<llfs::little_i32>);

#endif  // LLFS_PACKED_INTERVAL_HPP
