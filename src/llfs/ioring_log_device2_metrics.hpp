//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE2_METRICS_HPP
#define LLFS_IORING_LOG_DEVICE2_METRICS_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/metrics.hpp>

namespace llfs {

struct IoRingLogDevice2Metrics {
  /** \brief Total number of bytes written to storage (control + data), including alignment padding.
   */
  CountMetric<u64> bytes_written{0};

  /** \brief Number of logical (data) bytes flushed to storage.  This may be greater than
   * (flush_pos - trim_pos) since it includes out-of-order ranges.
   */
  CountMetric<u64> bytes_flushed{0};

  /** \brief The maximum observed concurrent flush write operations.
   */
  CountMetric<u64> max_concurrent_writes{0};

  /** \brief The number of times burst-mode alignment truncation was calculated.
   */
  CountMetric<u64> burst_mode_checked{0};

  /** \brief The number of times that the truncated alignment was less than the observed slot upper
   * bound (this is a subset of the events counted by this->burst_mode_checked).
   */
  CountMetric<u64> burst_mode_applied{0};

  /** \brief The total number of write IOPs initiated by the driver.
   */
  CountMetric<u64> total_write_count{0};

  /** \brief The number of flush write IOPs initiated by the driver (this is a subset of the events
   * counted by this->total_write_count).
   */
  CountMetric<u64> flush_write_count{0};

  /** \brief The number of control block update IOPs initiated by the driver (this is a subset of
   * the events counted by this->total_write_count).
   */
  CountMetric<u64> control_block_write_count{0};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Exports all collectors in this object to the passed registry, adding the passed labels.
   *
   * IMPORTANT: Once export_to has been called, this->unexport_from(registry) must be called before
   * this object goes out of scope!
   */
  void export_to(MetricRegistry& registry, const MetricLabelSet& labels) noexcept;

  /** \brief Removes all previously exported collectors associated with this object from the passed
   * registry.
   */
  void unexport_from(MetricRegistry& registry) noexcept;
};

std::ostream& operator<<(std::ostream& out, const IoRingLogDevice2Metrics& t);

}  //namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_IORING_LOG_DEVICE2_METRICS_HPP
