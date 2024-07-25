//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_device2_metrics.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <batteries/operators.hpp>
#include <batteries/utility.hpp>

#include <boost/preprocessor/stringize.hpp>

namespace llfs {

BATT_OBJECT_PRINT_IMPL((), IoRingLogDevice2Metrics,
                       (bytes_written, bytes_flushed, max_concurrent_writes, burst_mode_checked,
                        burst_mode_applied, total_write_count, flush_write_count,
                        control_block_write_count))

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDevice2Metrics::export_to(MetricRegistry& registry,
                                        const MetricLabelSet& labels) noexcept
{
#define LLFS_EXPORT_METRIC_(name)                                                                  \
  registry.add(BOOST_PP_STRINGIZE(name), this->name, batt::make_copy(labels))

  LLFS_EXPORT_METRIC_(bytes_written);
  LLFS_EXPORT_METRIC_(bytes_flushed);
  LLFS_EXPORT_METRIC_(max_concurrent_writes);
  LLFS_EXPORT_METRIC_(burst_mode_checked);
  LLFS_EXPORT_METRIC_(burst_mode_applied);
  LLFS_EXPORT_METRIC_(total_write_count);
  LLFS_EXPORT_METRIC_(flush_write_count);
  LLFS_EXPORT_METRIC_(control_block_write_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingLogDevice2Metrics::unexport_from(MetricRegistry& registry) noexcept
{
  registry  //
      .remove(this->bytes_written)
      .remove(this->bytes_flushed)
      .remove(this->max_concurrent_writes)
      .remove(this->burst_mode_checked)
      .remove(this->burst_mode_applied)
      .remove(this->total_write_count)
      .remove(this->flush_write_count)
      .remove(this->control_block_write_count);
}

}  //namespace llfs

#endif  // LLFS_DISABLE_IO_URING
