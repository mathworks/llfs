#pragma once
#ifndef LLFS_PAGE_SIZE_HPP
#define LLFS_PAGE_SIZE_HPP

#include <llfs/int_types.hpp>

#include <batteries/math.hpp>
#include <batteries/strong_typedef.hpp>

namespace llfs {

BATT_STRONG_TYPEDEF(u64, PageCount);
BATT_STRONG_TYPEDEF_SUPPORTS_NUMERICS(PageCount)

BATT_STRONG_TYPEDEF(u32, PageSize);
BATT_STRONG_TYPEDEF_SUPPORTS_NUMERICS(PageSize)

BATT_STRONG_TYPEDEF(u32, PageSizeLog2);
BATT_STRONG_TYPEDEF_SUPPORTS_NUMERICS(PageSizeLog2)

inline PageSizeLog2 log2_ceil(PageSize s)
{
  return PageSizeLog2{static_cast<u32>(batt::log2_ceil(static_cast<u64>(s.value())))};
}

inline usize get_page_size(const PageSize& s)
{
  return s.value();
}

inline usize get_page_size(const PageSizeLog2& s)
{
  return usize{1} << s.value();
}

struct PageSizeOrder {
  template <typename L, typename R>
  bool operator()(const L& l, const R& r) const
  {
    return get_page_size(l) < get_page_size(r);
  }
};

}  // namespace llfs

#endif  // LLFS_PAGE_SIZE_HPP
