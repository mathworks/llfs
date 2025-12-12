//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_CACHE_INSERT_OPTIONS_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/page_cache_overcommit.hpp>
#include <llfs/page_size.hpp>

#include <memory>

namespace llfs {

struct PageCacheInsertOptions {
  using Self = PageCacheInsertOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The size of the page being inserted (bytes); must be a power of 2.
   */
  PageSize page_size_{0};

  /** \brief The eviction priority (lower == evict first).
   */
  LruPriority lru_priority_{1};

  /** \brief Whether to allow cache over-commit for this insertion.
   */
  PageCacheOvercommit* overcommit_ = std::addressof(PageCacheOvercommit::not_allowed());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
};

}  // namespace llfs
