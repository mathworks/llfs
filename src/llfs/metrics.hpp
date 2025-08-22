//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_METRICS_HPP
#define LLFS_METRICS_HPP

#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/metrics/metric_registry.hpp>

namespace llfs {

using ::batt::CountMetric;
using ::batt::Every2ToThe;
using ::batt::Every2ToTheConst;
using ::batt::FastCountMetric;
using ::batt::global_metric_registry;
using ::batt::LatencyMetric;
using ::batt::LatencyTimer;
using ::batt::MetricLabel;
using ::batt::MetricLabelSet;
using ::batt::MetricRegistry;
using ::batt::RateMetric;

#define LLFS_COLLECT_LATENCY BATT_COLLECT_LATENCY
#define LLFS_COLLECT_LATENCY_N BATT_COLLECT_LATENCY_N
#define LLFS_COLLECT_LATENCY_SAMPLE BATT_COLLECT_LATENCY_SAMPLE

}  // namespace llfs

#endif  // LLFS_METRICS_HPP
