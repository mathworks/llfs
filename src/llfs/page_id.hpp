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

#include <llfs/cache.hpp>
#include <llfs/int_types.hpp>

#include <iomanip>
#include <memory>
#include <ostream>

namespace llfs {

using page_id_int = u64;

constexpr page_id_int kInvalidPageId = ~page_id_int{0};

struct PageBuffer;

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

  explicit PageId(page_id_int val) noexcept : value_{val}
  {
  }

  page_id_int int_value() const
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

 private:
  page_id_int value_{kInvalidPageId};
};

inline usize hash_value(const PageId& page_id)
{
  return PageId::Hash{}(page_id);
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
