//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_RUNTIME_OPTIONS_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_RUNTIME_OPTIONS_HPP

#include <batteries/async/task_scheduler.hpp>

namespace llfs {

struct PageAllocatorRuntimeOptions {
  batt::TaskScheduler& scheduler;
  std::string_view name;
};

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_RUNTIME_OPTIONS_HPP
