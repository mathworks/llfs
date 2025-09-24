//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ID_HPP
#define LLFS_PAGE_ID_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>

#include <batteries/utility.hpp>

#include <iomanip>
#include <memory>
#include <ostream>

namespace llfs {

using page_id_int = u64;

constexpr page_id_int kInvalidPageId = ~page_id_int{0};

class PageBuffer;

constexpr page_id_int kPageIdDeviceMask =  //
    ((page_id_int{1} << kPageIdDeviceBits) - 1) << kPageIdDeviceShift;

constexpr page_id_int kPageIdGenerationMask =  //
    ((page_id_int{1} << kPageIdGenerationBits) - 1) << kPageIdGenerationShift;

constexpr page_id_int kPageIdAddressMask =  //
    ((page_id_int{1} << kPageIdAddressBits) - 1) << kPageIdAddressShift;

static_assert((kPageIdDeviceMask & kPageIdGenerationMask) == 0);
static_assert((kPageIdDeviceMask & kPageIdAddressMask) == 0);
static_assert((kPageIdGenerationMask & kPageIdAddressMask) == 0);
static_assert((kPageIdDeviceMask | kPageIdGenerationMask | kPageIdAddressMask) == ~u64{0});

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Identifies a Page on disk.
//
class PageId
{
 public:
  struct Hash {
    decltype(auto) operator()(const PageId& page_id) const
    {
      return std::hash<page_id_int>{}(page_id.int_value());
    }
  };

  PageId() = default;

  BATT_ALWAYS_INLINE explicit PageId(page_id_int val) noexcept : value_{val}
  {
  }

  BATT_ALWAYS_INLINE page_id_int int_value() const
  {
    return this->value_;
  }

  bool is_valid() const
  {
    return this->value_ != kInvalidPageId;
  }

  explicit operator bool() const
  {
    return this->is_valid();
  }

  u64 device_id() const
  {
    return (this->value_ & kPageIdDeviceMask) >> kPageIdDeviceShift;
  }

  u64 generation() const
  {
    return (this->value_ & kPageIdGenerationMask) >> kPageIdGenerationShift;
  }

  u64 address() const
  {
    return (this->value_ & kPageIdAddressMask) >> kPageIdAddressShift;
  }

  PageId& set_device_id_shifted(u64 new_device_id_shifted)
  {
    this->value_ = (this->value_ & ~kPageIdDeviceMask) |  //
                   (new_device_id_shifted & kPageIdDeviceMask);
    return *this;
  }

  PageId& set_device_id(u64 new_device_id)
  {
    return this->set_device_id_shifted(new_device_id << kPageIdDeviceShift);
  }

  PageId& set_generation(u64 new_generation)
  {
    this->value_ = (this->value_ & ~kPageIdGenerationMask) |
                   ((new_generation << kPageIdGenerationShift) & kPageIdGenerationMask);
    return *this;
  }

  PageId& set_address(u64 new_address)
  {
    this->value_ = (this->value_ & ~kPageIdAddressMask) |
                   ((new_address << kPageIdAddressShift) & kPageIdAddressMask);
    return *this;
  }

 private:
  page_id_int value_{kInvalidPageId};
};

inline usize hash_value(const PageId& page_id)
{
  return PageId::Hash{}(page_id);
}

BATT_ALWAYS_INLINE inline bool is_same_physical_page(const PageId& left, const PageId& right)
{
  static constexpr page_id_int kDeviceAndAddressMask = kPageIdDeviceMask | kPageIdAddressMask;

  return (left.int_value() & kDeviceAndAddressMask) == (right.int_value() & kDeviceAndAddressMask);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline std::ostream& operator<<(std::ostream& out, const PageId& t)
{
  return out << "page@" << std::setw(7) << std::setfill('0') << std::hex << t.int_value()
             << std::dec;
}

inline bool operator==(const PageId& l, const PageId& r)
{
  return l.int_value() == r.int_value();
}

inline bool operator!=(const PageId& l, const PageId& r)
{
  return !(l == r);
}

inline bool operator<(const PageId& l, const PageId& r)
{
  return l.int_value() < r.int_value();
}

inline bool operator>(const PageId& l, const PageId& r)
{
  return r < l;
}

inline bool operator<=(const PageId& l, const PageId& r)
{
  return !(r < l);
}

inline bool operator>=(const PageId& l, const PageId& r)
{
  return !(l < r);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline PageId get_page_id(const PageId& id)
{
  return id;
}

inline PageId get_page_id(const NoneType&)
{
  return PageId{};
}

inline page_id_int get_page_id_int(const PageId& id)
{
  return id.int_value();
}

inline page_id_int get_page_id_int(const NoneType&)
{
  return kInvalidPageId;
}

}  // namespace llfs

#endif  // LLFS_PAGE_ID_HPP
