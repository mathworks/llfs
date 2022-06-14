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
