//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_LAYOUT_ID_HPP
#define LLFS_PAGE_LAYOUT_ID_HPP

#include <llfs/int_types.hpp>

#include <boost/operators.hpp>

#include <functional>
#include <string>
#include <string_view>

namespace llfs {

struct PageLayoutId
    : boost::totally_ordered<PageLayoutId>
    , boost::equality_comparable<PageLayoutId> {
  static constexpr usize kMaxSize = 8;

  u8 value[kMaxSize];

  template <usize kLength>
  static PageLayoutId from_str(const char (&c_str)[kLength])
  {
    static_assert(kLength <= kMaxSize + (1 /*null terminator*/));

    PageLayoutId id = PageLayoutId::min_value();
    std::memcpy(id.value, c_str, std::min(kLength, kMaxSize));

    return id;
  }

  static const PageLayoutId& min_value()
  {
    static const PageLayoutId v_ = [] {
      PageLayoutId v;
      std::memset(&v, 0, sizeof(v));
      return v;
    }();
    return v_;
  }
  static const PageLayoutId& max_value()
  {
    static const PageLayoutId v_ = [] {
      PageLayoutId v;
      std::memset(&v, ~u8{0}, sizeof(v));
      return v;
    }();
    return v_;
  }

  struct Hash {
    decltype(auto) operator()(const PageLayoutId& tag) const
    {
      static_assert(sizeof(u64) == sizeof(tag.value), "");
      union {
        u64 i;
        PageLayoutId tag;
      } local_copy;

      local_copy.tag = tag;

      return std::hash<u64>{}(local_copy.i);
    }
  };  //namespace llfs
};

inline std::ostream& operator<<(std::ostream& out, const PageLayoutId& t)
{
  return out << std::string_view{(const char*)t.value, sizeof(t.value)};
}

static_assert(sizeof(PageLayoutId) == 8, "");

inline bool operator==(const PageLayoutId& l, const PageLayoutId& r)
{
  return std::memcmp(l.value, r.value, sizeof(l.value)) == 0;
}
inline bool operator<(const PageLayoutId& l, const PageLayoutId& r)
{
  return std::memcmp(l.value, r.value, sizeof(l.value)) < 0;
}

}  // namespace llfs

#endif  // LLFS_PAGE_LAYOUT_ID_HPP
