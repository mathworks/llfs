//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_OPTIONS_HPP
#define LLFS_PAGE_CACHE_OPTIONS_HPP

#include <llfs/config.hpp>
#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>
#include <llfs/page_size.hpp>

#include <batteries/assert.hpp>
#include <batteries/math.hpp>

#include <array>

namespace llfs {

class PageCacheOptions
{
 public:
  static PageCacheOptions with_default_values();

  u64 default_log_size() const
  {
    return this->default_log_size_;
  }

  PageCacheOptions& set_max_cached_pages_per_size(PageSize page_size, usize n)
  {
    const int page_size_log2 = batt::log2_ceil(page_size);
    BATT_CHECK_EQ(page_size_log2, batt::log2_floor(page_size));
    BATT_CHECK_LT(page_size_log2, kMaxPageSizeLog2);
    this->max_cached_pages_per_size_log2[page_size_log2] = n;
    return *this;
  }

  std::array<usize, kMaxPageSizeLog2> max_cached_pages_per_size_log2;

 private:
  u64 default_log_size_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_OPTIONS_HPP
