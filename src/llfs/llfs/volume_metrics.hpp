#pragma once
#ifndef LLFS_VOLUME_METRICS_HPP
#define LLFS_VOLUME_METRICS_HPP

#include <llfs/metrics.hpp>

namespace llfs {

class VolumeMetrics
{
 public:
  LatencyMetric prepare_slot_append_latency;
  LatencyMetric prepare_slot_sync_latency;
  LatencyMetric commit_job_latency;
  LatencyMetric commit_slot_append_latency;
  LatencyMetric commit_slot_sync_latency;

  LatencyMetric reaper_queue_wait_latency;
  LatencyMetric reaper_use_count_latency;
  LatencyMetric reaper_append_deprecated_latency;
  LatencyMetric reaper_flush_deprecated_latency;
  LatencyMetric reaper_append_removed_latency;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_METRICS_HPP
