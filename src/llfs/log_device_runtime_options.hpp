//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_LOG_DEVICE_RUNTIME_OPTIONS_HPP
#define LLFS_LOG_DEVICE_RUNTIME_OPTIONS_HPP

#include <llfs/config.hpp>
//

#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>

#include <batteries/assert.hpp>
#include <batteries/stream_util.hpp>

#include <atomic>
#include <string>
#include <string_view>

namespace llfs {

struct LogDeviceRuntimeOptions {
  using Self = LogDeviceRuntimeOptions;

  static constexpr usize kDefaultFlushDelayThreshold = 1 * kMiB;
  static constexpr usize kDefaultMaxConcurrentWrites = 64;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static LogDeviceRuntimeOptions with_default_values()
  {
    return LogDeviceRuntimeOptions{};
  }

  static i32 next_id()
  {
    static std::atomic<i32> n{1};
    return n.fetch_add(1);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The debug name of this log.
  //
  std::string name = batt::to_string("(anonymous log ", Self::next_id(), ")");

  usize flush_delay_threshold = kDefaultFlushDelayThreshold;

  usize max_concurrent_writes = kDefaultMaxConcurrentWrites;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Self& set_name(std::string_view name)
  {
    this->name = name;
    return *this;
  }

  usize queue_depth() const
  {
    return this->max_concurrent_writes;
  }

  Self& set_queue_depth(usize n)
  {
    this->max_concurrent_writes = n;
    return *this;
  }

  /** \brief Sets queue depth to the smallest power of 2 that is not greater than `max_n` *and* not
   * greater than the current value of queue depth.
   *
   * `max_n` must be at least 2.
   */
  Self& limit_queue_depth(usize max_n)
  {
    BATT_CHECK_GT(max_n, 1);
    while (this->queue_depth() > max_n) {
      this->set_queue_depth(this->queue_depth() / 2);
    }
    return *this;
  }

  usize queue_depth_mask() const
  {
    return this->queue_depth() - 1;
  }
};

}  //namespace llfs

#endif  // LLFS_LOG_DEVICE_RUNTIME_OPTIONS_HPP
