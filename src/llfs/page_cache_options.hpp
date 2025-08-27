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
//
#include <llfs/api_types.hpp>
#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheOptions& set_byte_size(usize max_size,
                                  Optional<PageSize> default_page_size = PageSize{4096})
  {
    this->max_cache_size_bytes = MaxCacheSizeBytes{max_size};
    if (default_page_size) {
      this->cache_slot_count = SlotCount{this->max_cache_size_bytes / *default_page_size};
    }

    return *this;
  }

  SlotCount cache_slot_count;

  MaxCacheSizeBytes max_cache_size_bytes;

 private:
  u64 default_log_size_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_OPTIONS_HPP
