//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEMORY_PAGE_CACHE_HPP
#define LLFS_MEMORY_PAGE_CACHE_HPP

#include <llfs/config.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_size.hpp>

#include <batteries/async/task_scheduler.hpp>
#include <batteries/shared_ptr.hpp>

#include <memory>
#include <utility>
#include <vector>

namespace llfs {

batt::SharedPtr<PageCache> make_memory_page_cache(
    batt::TaskScheduler& scheduler, const std::vector<std::pair<PageCount, PageSize>>& arena_sizes,
    MaxRefsPerPage max_refs_per_page);

}  // namespace llfs

#endif  // LLFS_MEMORY_PAGE_CACHE_HPP
