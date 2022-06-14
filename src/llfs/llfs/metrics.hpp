#pragma once
#ifndef LLFS_METRICS_HPP
#define LLFS_METRICS_HPP

#include <turtle/util/metric_collectors.hpp>
#include <turtle/util/metric_registry.hpp>

namespace llfs {

using ::turtle_db::CountMetric;
using ::turtle_db::global_metric_registry;
using ::turtle_db::LatencyMetric;
using ::turtle_db::LatencyTimer;

#define LLFS_COLLECT_LATENCY TURTLE_DB_COLLECT_LATENCY
#define LLFS_COLLECT_LATENCY_N TURTLE_DB_COLLECT_LATENCY_N

}  // namespace llfs

#endif  // LLFS_METRICS_HPP
