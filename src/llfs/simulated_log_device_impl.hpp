//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMULATED_LOG_DEVICE_IMPL_HPP
#define LLFS_SIMULATED_LOG_DEVICE_IMPL_HPP

#include <llfs/config.hpp>
//
#include <llfs/log_device.hpp>
#include <llfs/simulated_log_device.hpp>
#include <llfs/simulated_storage_object.hpp>
#include <llfs/slot.hpp>
#include <llfs/storage_simulation.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>

#include <map>
#include <memory>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SimulatedLogDevice::Impl : public SimulatedStorageObject
{
 public:
  class ReaderImpl;
  class WriterImpl;

  struct CommitChunk {
    static constexpr i32 kPreparedState = 0;
    static constexpr i32 kCommittedState = 1;
    static constexpr i32 kFlushedState = 2;
    static constexpr i32 kDroppedState = 3;

    Impl& impl;
    slot_offset_type slot_offset = 0;
    usize trim_size = 0;
    std::vector<char> data;
    batt::Watch<i32> state{kPreparedState};

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit CommitChunk(Impl& impl, slot_offset_type offset, usize size) noexcept
        : impl{impl}
        , slot_offset{offset}
        , data(size)
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    slot_offset_type slot_lower_bound() const noexcept
    {
      return this->slot_offset;
    }

    slot_offset_type slot_upper_bound() const noexcept
    {
      return this->slot_offset + this->data.size();
    }

    SlotRange slot_range() const noexcept
    {
      return SlotRange{this->slot_lower_bound(), this->slot_upper_bound()};
    }

    bool is_flushed() const noexcept
    {
      return this->state.get_value() == kFlushedState;
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit Impl(StorageSimulation& simulation, const std::string& name, u64 log_capacity) noexcept;

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  ~Impl() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Logs a simulation event.
   */
  template <typename... Args>
  void log_event(Args&&... args) const noexcept
  {
    this->simulation_.log_event("SimulatedLogDevice{", batt::c_str_literal(this->name_), "} ",
                                BATT_FORWARD(args)...);
  }

  /** \brief Simulates a process termination/restart by removing all non-flushed state.
   */
  void crash_and_recover(u64 simulation_step) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u64 capacity() const noexcept
  {
    return this->capacity_;
  }

  u64 size() const noexcept
  {
    return this->size_.get_value();
  }

  u64 space() const noexcept
  {
    return this->capacity() - this->size();
  }

  Status trim(slot_offset_type new_trim_pos);

  std::unique_ptr<LogDevice::Reader> new_reader(Optional<slot_offset_type> slot_lower_bound,
                                                LogReadMode mode);

  SlotRange slot_range(LogReadMode mode);

  LogDevice::Writer& writer();

  Status close();

  Status sync(LogReadMode mode, SlotUpperBoundAt event);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Updates `this->flush_pos_` to reflect the current state of `chunks_`.
   */
  void update_flush_pos();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StorageSimulation& simulation_;

  const std::string name_;

  const u64 capacity_;

  batt::Watch<u64> latest_recovery_step_{0};

  batt::Mutex<std::map<slot_offset_type, std::shared_ptr<CommitChunk>, SlotLess>> chunks_;

  batt::Watch<slot_offset_type> trim_pos_{0};

  batt::Watch<slot_offset_type> flush_pos_{0};

  batt::Watch<slot_offset_type> commit_pos_{0};

  batt::Watch<u64> size_{0};

  std::unique_ptr<WriterImpl> writer_impl_;

  batt::Watch<bool> closed_{false};
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_IMPL_HPP
