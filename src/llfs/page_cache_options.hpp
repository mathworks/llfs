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

#include <array>
#include <set>

namespace llfs {

class PageCacheOptions
{
 public:
  static PageCacheOptions with_default_values();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // TODO [tastolfi 2025-08-29] remove once we get rid of FileLogDevice.
  //
  u64 default_log_size() const
  {
    return this->default_log_size_;
  }

  /** \brief Specifies that for each PageDevice in the PageCache with `page_size`, a sharded
   * view (virtual) PageDevice with `shard_size`-aligned shards should be created.
   */
  PageCacheOptions& add_sharded_view(PageSize page_size, PageSize shard_size);

  /** \brief Sets the total PageCache size in bytes.
   *
   * `min_page_size` is used to calculate the number of cache slots to allocate.  It defaults to
   * kDirectIOBlockSize.
   */
  PageCacheOptions& set_byte_size(usize max_size, Optional<PageSize> min_page_size = None);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SlotCount cache_slot_count;

  MaxCacheSizeBytes max_cache_size_bytes;

  std::set<std::pair<PageSize, PageSize>> sharded_views;

 private:
  u64 default_log_size_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_OPTIONS_HPP
