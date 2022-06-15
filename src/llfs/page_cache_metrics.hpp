//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_METRICS_HPP
#define LLFS_PAGE_CACHE_METRICS_HPP

#include <llfs/int_types.hpp>
#include <llfs/metrics.hpp>

namespace llfs {

struct PageCacheMetrics {
  CountMetric<int> get_count = 0;
  CountMetric<int> get_page_view_count = 0;
  CountMetric<int> get_read_count = 0;
  CountMetric<u64> total_bytes_written = 0;
  CountMetric<u64> total_bytes_read = 0;
  CountMetric<u64> used_bytes_written = 0;
  CountMetric<u64> node_write_count = 0;
  CountMetric<u64> leaf_write_count = 0;
  CountMetric<u64> total_write_ops = 0;
  CountMetric<u64> total_read_ops = 0;
  LatencyMetric allocate_page_alloc_latency;
  LatencyMetric allocate_page_insert_latency;
  LatencyMetric page_write_latency;
  LatencyMetric page_read_latency;
  LatencyMetric pipeline_wait_latency;
  LatencyMetric update_ref_counts_latency;
  LatencyMetric ref_count_sync_latency;
};

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_METRICS_HPP
