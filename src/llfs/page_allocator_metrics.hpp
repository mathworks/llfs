//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_METRICS_HPP
#define LLFS_PAGE_ALLOCATOR_METRICS_HPP

#include <llfs/metrics.hpp>

namespace llfs {

struct PageAllocatorMetrics {
  CountMetric<u64> pages_allocated{0};
  CountMetric<u64> pages_freed{0};
};

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_METRICS_HPP
