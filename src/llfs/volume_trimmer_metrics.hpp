//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_TRIMMER_METRICS_HPP
#define LLFS_VOLUME_TRIMMER_METRICS_HPP

#include <llfs/config.hpp>
//
#include <llfs/metrics.hpp>

#include <ostream>

namespace llfs {

struct VolumeTrimmerMetrics {
  CountMetric<i64> prepare_grant_reserved_byte_count{0};
  CountMetric<i64> prepare_grant_released_byte_count{0};
  CountMetric<i64> commit_grant_reserved_byte_count{0};
  CountMetric<i64> commit_grant_released_byte_count{0};
  CountMetric<i64> attach_grant_reserved_byte_count{0};
  CountMetric<i64> attach_grant_released_byte_count{0};
  CountMetric<i64> ids_grant_reserved_byte_count{0};
  CountMetric<i64> ids_grant_released_byte_count{0};
  CountMetric<i64> trim_grant_reserved_byte_count{0};
  CountMetric<i64> trim_grant_released_byte_count{0};
};

inline std::ostream& operator<<(std::ostream& out, const VolumeTrimmerMetrics& t)
{
  return out << "VolumeTrimmerMetrics"                                                          //
             << "{.prepare_grant_reserved_byte_count=" << t.prepare_grant_reserved_byte_count   //
             << ", .prepare_grant_released_byte_count=" << t.prepare_grant_released_byte_count  //
             << ", .commit_grant_reserved_byte_count=" << t.commit_grant_reserved_byte_count    //
             << ", .commit_grant_released_byte_count=" << t.commit_grant_released_byte_count    //
             << ", .attach_grant_reserved_byte_count=" << t.attach_grant_reserved_byte_count    //
             << ", .attach_grant_released_byte_count=" << t.attach_grant_released_byte_count    //
             << ", .ids_grant_reserved_byte_count=" << t.ids_grant_reserved_byte_count          //
             << ", .ids_grant_released_byte_count=" << t.ids_grant_released_byte_count          //
             << ", .trim_grant_reserved_byte_count=" << t.trim_grant_reserved_byte_count        //
             << ", .trim_grant_released_byte_count=" << t.trim_grant_released_byte_count        //
             << ",}";
}

}  //namespace llfs

#endif  // LLFS_VOLUME_TRIMMER_METRICS_HPP
