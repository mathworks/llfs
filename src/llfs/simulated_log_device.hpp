//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMULATED_LOG_DEVICE_HPP
#define LLFS_SIMULATED_LOG_DEVICE_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/log_device.hpp>
#include <llfs/optional.hpp>
#include <llfs/status.hpp>

#include <memory>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SimulatedLogDevice : public LogDevice
{
 public:
  class Impl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SimulatedLogDevice(std::shared_ptr<Impl>&& impl) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u64 capacity() const override;

  // The current size of all committed data in the log.
  //
  u64 size() const override;

  // Trim the log at `slot_lower_bound`.  May not take effect immediately if there are active
  // Readers whose slot_offset is below `slot_lower_bound`.
  //
  Status trim(slot_offset_type slot_lower_bound) override;

  // Create a new reader.
  //
  std::unique_ptr<LogDevice::Reader> new_reader(Optional<slot_offset_type> slot_lower_bound,
                                                LogReadMode mode) override;

  // Returns the current active slot range for the log.  `mode` determines whether the upper bound
  // will be the flushed or committed upper bound.
  //
  SlotRange slot_range(LogReadMode mode) override;

  // There can be only one Writer at a time.
  //
  LogDevice::Writer& writer() override;

  Status close() override;

  Status sync(LogReadMode mode, SlotUpperBoundAt event) override;

 private:
  std::atomic<bool> external_close_{false};
  std::shared_ptr<Impl> impl_;
  const u64 create_step_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_HPP
