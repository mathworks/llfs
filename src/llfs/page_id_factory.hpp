//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ID_FACTORY_HPP
#define LLFS_PAGE_ID_FACTORY_HPP

#include <llfs/config.hpp>
#include <llfs/int_types.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_size.hpp>

#include <batteries/assert.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/math.hpp>

#include <boost/operators.hpp>

namespace llfs {

using page_device_id_int = u64;
using little_page_device_id_int = little_u64;
using big_page_device_id_int = big_u64;

using page_generation_int = u64;
using little_page_generation_int = little_u64;
using big_page_generation_int = big_u64;

constexpr u8 kPageDeviceIdShift = 64 - kPageDeviceIdBits;
constexpr page_id_int kPageDeviceIdMask = ((page_id_int{1} << kPageDeviceIdBits) - 1)
                                          << kPageDeviceIdShift;

// Utility class to construct/deconstruct page ids from device id, physical page number, and page
// generation.
//
// Page ids are intended to be single-use.  However, real devices usually expose update-in-place
// addresses for pages.  To bridge the gap, we use a page_id encoding scheme based on device id
// (which is an index into some storage pool table), physical page, and a monotonic generation
// counter.  Writes on physical devices are assumed to cause a "wear" side-effect, i.e. there is a
// maximum number of writes that can be made to the same physical location before the hardware fails
// and must be replaced.  This corresponds to the limit on the generation counter imposed by the
// scheme implemented below.
//
// A page id is a 64-bit unsigned integer encoded as follows:
//
// +-------------------------+-----------------------------------+--------------------------------+
// |<-- kPageDeviceIdBits -->|<-- log_2(Max-Generation-Count) -->|<- log_2(Physical-Page-Count) ->|
// +-------------------------+-----------------------------------+--------------------------------+
//

class PageIdFactory : public boost::equality_comparable<PageIdFactory>
{
 public:
  static constexpr usize kMinGenerationBits = 18;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageIdFactory(PageCount device_capacity, page_device_id_int page_device_id) noexcept
      : capacity_{BATT_CHECKED_CAST(page_id_int, device_capacity.value())}
      , device_id_{page_device_id}
      , device_id_prefix_{(this->device_id_ << kPageDeviceIdShift) & kPageDeviceIdMask}
  {
    BATT_CHECK_GE(sizeof(page_device_id_int) * 8 - (kPageDeviceIdBits - this->capacity_bits_),
                  kMinGenerationBits);

    BATT_CHECK_GE(this->physical_page_mask_ + 1, this->capacity_);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCount get_physical_page_count() const
  {
    return PageCount{this->capacity_};
  }

  PageId make_page_id(page_id_int physical_page, page_generation_int generation) const
  {
    BATT_CHECK_LT(physical_page, this->capacity_);
    return PageId{this->device_id_prefix_                                                 //
                  | ((u64{generation} << this->capacity_bits_) & this->generation_mask_)  //
                  | (u64{physical_page} & this->physical_page_mask_)};
  }

  PageId advance_generation(PageId id) const
  {
    return this->make_page_id(this->get_physical_page(id), this->get_generation(id) + 1);
  }

  page_id_int max_generation_count() const
  {
    return this->generation_mask_ >> this->capacity_bits_;
  }

  i64 get_physical_page(PageId id) const
  {
    return id.int_value() & this->physical_page_mask_;
  }

  page_generation_int get_generation(PageId id) const
  {
    return (id.int_value() & this->generation_mask_) >> this->capacity_bits_;
  }

  static page_device_id_int get_device_id(PageId id)
  {
    return (id.int_value() & kPageDeviceIdMask) >> kPageDeviceIdShift;
  }

  page_device_id_int get_device_id() const
  {
    return this->device_id_;
  }

  bool generation_less_than(page_generation_int first_gen, page_generation_int second_gen) const
  {
    const auto delta = second_gen - first_gen;
    return delta != 0 && delta < this->max_generation_count() / 2;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  friend inline bool operator==(const PageIdFactory& l, const PageIdFactory& r)
  {
    return l.get_physical_page_count() == r.get_physical_page_count() &&
           l.get_device_id() == r.get_device_id();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  page_id_int capacity_;
  page_device_id_int device_id_;
  page_id_int device_id_prefix_;

  // We round the capacity up by two bits and then add four so that the hex representation of a
  // page_id will always have a zero digit in between the generation and the physical page number.
  //
  static constexpr i32 kHexDigitBits = 4;
  static constexpr i32 kHexDigitBitsLog2 = 2;
  //
  u8 capacity_bits_ =
      batt::round_up_bits(kHexDigitBitsLog2, batt::log2_ceil(this->capacity_)) + kHexDigitBits;

  u64 physical_page_mask_ = (u64{1} << this->capacity_bits_) - 1;
  u64 generation_mask_ = ~(kPageDeviceIdMask | this->physical_page_mask_);
};

}  // namespace llfs

#endif  // LLFS_PAGE_ID_FACTORY_HPP
