//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/metrics.hpp>

#include <array>

namespace llfs {

struct PageDeviceMetrics {
  using Self = PageDeviceMetrics;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // TODO [tastolfi 2025-08-22] move to PageCacheMetrics?
  //
  std::array<CountMetric<u64>, 32 + 1> write_count_per_page_size_log2;
  std::array<CountMetric<u64>, 32 + 1> read_count_per_page_size_log2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self& instance()
  {
    static Self instance_;
    return instance_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void reset()
  {
    for (auto& metric : this->write_count_per_page_size_log2) {
      metric.reset();
    }
    for (auto& metric : this->read_count_per_page_size_log2) {
      metric.reset();
    }
  }
};

}  //namespace llfs
