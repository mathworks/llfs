//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DRIVER_OPTIONS_HPP
#define LLFS_IORING_LOG_DRIVER_OPTIONS_HPP

#include <llfs/int_types.hpp>

#include <batteries/math.hpp>
#include <batteries/stream_util.hpp>

#include <atomic>
#include <string>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Performance tuning options.
//
class IoRingLogDriverOptions
{
 public:
  using Self = IoRingLogDriverOptions;

  static IoRingLogDriverOptions with_default_values();

  static int next_id()
  {
    static std::atomic<int> n{1};
    return n.fetch_add(1);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRingLogDriverOptions() noexcept
  {
  }

  // The debug name of this log.
  //
  std::string name = batt::to_string("(anonymous log ", next_id(), ")");

  // How long to wait for a full page worth of log data before flushing to disk.
  //
  u32 page_write_buffer_delay_usec = 0;  // TODO [tastolfi 2021-06-21] remove or implement

  // How many log segments to flush in parallel.
  //
  usize queue_depth_log2 = 4;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize queue_depth() const
  {
    return usize{1} << this->queue_depth_log2;
  }

  Self& set_queue_depth(usize n)
  {
    this->queue_depth_log2 = batt::log2_ceil(n);
    BATT_CHECK_EQ(this->queue_depth(), n) << "The queue depth must be a power of 2!";
    return *this;
  }

  usize queue_depth_mask() const
  {
    return this->queue_depth() - 1;
  }

  Self& set_name(std::string_view name)
  {
    this->name = name;
    return *this;
  }
};

}  // namespace llfs

#endif  // LLFS_IORING_LOG_DRIVER_OPTIONS_HPP
