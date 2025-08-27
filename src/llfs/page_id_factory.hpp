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
#include <batteries/utility.hpp>

#include <boost/operators.hpp>

namespace llfs {

using page_device_id_int = page_id_int;
using little_page_device_id_int = little_u64;
using big_page_device_id_int = big_u64;

using page_generation_int = page_id_int;
using little_page_generation_int = little_u64;
using big_page_generation_int = big_u64;

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
  BATT_ALWAYS_INLINE static page_device_id_int get_device_id(PageId id)
  {
    return id.device_id();
  }

  BATT_ALWAYS_INLINE static PageId change_device_id(PageId src_page_id,
                                                    page_device_id_int new_device_id)
  {
    return src_page_id.set_device_id(new_device_id);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageIdFactory(PageCount device_capacity, page_device_id_int page_device_id) noexcept
      : capacity_{BATT_CHECKED_CAST(page_id_int, device_capacity.value())}
      , device_id_{page_device_id}
      , device_id_prefix_{(this->device_id_ << kPageIdDeviceShift) & kPageIdDeviceMask}
  {
    BATT_CHECK_LT(this->device_id_, page_id_int{1} << kPageIdDeviceBits);
    BATT_CHECK_LT(this->capacity_, page_id_int{1} << kPageIdAddressBits);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCount get_physical_page_count() const
  {
    return PageCount{this->capacity_};
  }

  BATT_ALWAYS_INLINE PageId make_page_id(page_id_int physical_page, page_id_int generation) const
  {
    BATT_CHECK_LT(physical_page, this->capacity_);

    return PageId{this->device_id_prefix_                                             //
                  | ((generation << kPageIdGenerationShift) & kPageIdGenerationMask)  //
                  | ((physical_page << kPageIdAddressShift) & kPageIdAddressMask)};
  }

  PageId advance_generation(PageId id) const
  {
    return this->make_page_id(this->get_physical_page(id), this->get_generation(id) + 1);
  }

  static constexpr page_id_int max_generation_count()
  {
    return (page_id_int{1} << kPageIdGenerationBits) - 1;
  }

  BATT_ALWAYS_INLINE i64 get_physical_page(PageId page_id) const
  {
    return page_id.address();
  }

  BATT_ALWAYS_INLINE page_id_int get_generation(PageId page_id) const
  {
    return page_id.generation();
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

  bool is_same_physical_page(PageId left, PageId right) const
  {
    return PageIdFactory::get_device_id(left) == this->get_device_id() &&
           PageIdFactory::get_device_id(left) == PageIdFactory::get_device_id(right) &&
           this->get_physical_page(left) == this->get_physical_page(right);
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
  page_id_int device_id_;
  page_id_int device_id_prefix_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_ID_FACTORY_HPP
