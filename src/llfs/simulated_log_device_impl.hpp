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
#include <llfs/buffer.hpp>
#include <llfs/log_device.hpp>
#include <llfs/ring_buffer.hpp>
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

/** \brief The persistent state of a simulated LogDevice.
 */
class SimulatedLogDevice::Impl : public SimulatedStorageObject
{
 public:
  class ReaderImpl;
  class WriterImpl;

  /** \brief Stores the data and maintains the flush state of a single chunk of committed data.
   *
   * Each call to LogDevice::Writer::commit for this device creates exactly one CommitChunk.
   */
  struct CommitChunk {
    //----- --- -- -  -  -   -
    // State Values
    //----- --- -- -  -  -   -

    // The initial state for the chunk; the chunk is created as a result of calling
    // LogDevice::Writer::prepare.
    //
    static constexpr i32 kPreparedState = 0;

    // LogDevice::Writer::commit has been called for this chunk; a flush operation is pending.
    //
    static constexpr i32 kCommittedState = 1;

    // The chunk has been durably flushed to the simulated device; it will be visible across
    // simulated crash/recovery operations.
    //
    static constexpr i32 kFlushedState = 2;

    // The chunk was lost after being committed (but before being flushed) due to a simulated crash.
    //
    static constexpr i32 kDroppedState = 3;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    // The Impl that created/owns this chunk.
    //
    Impl& impl;

    // The offset of this chunk in bytes from the beginning of the log.
    //
    const slot_offset_type slot_offset;

    // The number of bytes that have been trimmed from the start of this chunk.
    //
    usize trim_size = 0;

    // The chunk data.
    //
    MutableBuffer data_view;

    // The chunk state (see above).
    //
    batt::Watch<i32> state{kPreparedState};

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit CommitChunk(Impl& impl, slot_offset_type offset, usize size) noexcept
        : impl{impl}
        , slot_offset{offset}
        , data_view{resize_buffer(impl.ring_buffer_.get_mut(offset), size)}
    {
    }

    CommitChunk(const CommitChunk&) = delete;
    CommitChunk& operator=(const CommitChunk&) = delete;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief Returns the slot offset of this chunk.
     */
    slot_offset_type slot_lower_bound() const noexcept
    {
      return this->slot_offset;
    }

    /** \brief Returns one byte past the last offset occupied by this chunk in the log.
     */
    slot_offset_type slot_upper_bound() const noexcept
    {
      return this->slot_offset + this->data_view.size();
    }

    /** \brief Returns the slot range of this chunk.
     */
    SlotRange slot_range() const noexcept
    {
      return SlotRange{this->slot_lower_bound(), this->slot_upper_bound()};
    }

    /** \brief Returns true iff this chunk has been flushed.
     */
    bool is_flushed() const noexcept
    {
      return this->state.get_value() == kFlushedState;
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Creates a new simulated log impl.
   */
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

  /** \brief Returns the simulation that owns this log impl.
   */
  StorageSimulation& simulation() const noexcept
  {
    return this->simulation_;
  }

  /** \brief The maximum size (bytes) of the log.
   */
  u64 capacity() const noexcept
  {
    return this->capacity_;
  }

  /** \brief The current size of the log.
   */
  u64 size() const noexcept
  {
    return this->size_.get_value();
  }

  /** \brief The number of available bytes in the log.
   */
  u64 space() const noexcept
  {
    return this->capacity() - this->size();
  }

  /** \brief Advances the simulated log trim offset, permanently deleting all chunks between the old
   * and new trim positions.
   *
   * If the passed `device_create_step` is older than the simulation step value most recently passed
   * to `crash_and_recover`, then this function will fail with `batt::StatusCode::kClosed` and have
   * no affect on the log state.
   */
  Status trim(u64 device_create_step, slot_offset_type new_trim_pos);

  /** \brief Creates and returns a new simulated log reader.
   */
  std::unique_ptr<LogDevice::Reader> new_reader(Optional<slot_offset_type> slot_lower_bound,
                                                LogReadMode mode);

  /** \brief Returns the current valid slot range for the given read mode.
   */
  SlotRange slot_range(LogReadMode mode);

  /** \brief Returns the writer for the simulated log.
   */
  LogDevice::Writer& writer();

  /** \brief Closes the log; the "closed" status of the log is reset by `crash_and_recover`.
   *
   * If the passed `device_create_step` is older than the simulation step value most recently passed
   * to `crash_and_recover`, then this function will still return OkStatus but not change the Impl
   * state.
   */
  Status close(u64 device_create_step);

  /** \brief Blocks the caller until the given slot is committed/flushed.
   *
   * If the passed `device_create_step` is older than the simulation step value most recently passed
   * to `crash_and_recover`, then this function will fail with `batt::StatusCode::kClosed` and have
   * no affect on the log state.
   */
  Status sync(u64 device_create_step, LogReadMode mode, SlotUpperBoundAt event);

  /** \brief Returns the current commit_pos or flush_pos, depending on mode.
   */
  slot_offset_type get_slot_upper_bound(LogReadMode mode) const noexcept
  {
    if (mode == LogReadMode::kDurable) {
      return this->flush_pos_.get_value();
    } else if (mode == LogReadMode::kSpeculative) {
      return this->commit_pos_.get_value();
    }

    BATT_PANIC() << "Bad mode value: " << (int)mode;
    BATT_UNREACHABLE();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Updates `this->flush_pos_` to reflect the current state of `chunks_`.
   */
  void update_flush_pos();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The simulation that owns this device.
  //
  StorageSimulation& simulation_;

  // The name of the device, as passed in at construction time.
  //
  const std::string name_;

  // The maximum size (bytes) of the simulated log.
  //
  const u64 capacity_;

  // The storage for all committed/prepared chunks.
  //
  RingBuffer ring_buffer_;

  // The most recent value passed to `crash_and_recover` (initially 0).
  //
  batt::Watch<u64> latest_recovery_step_{0};

  // All slot data chunks, indexed by slot offset.
  //
  batt::Mutex<std::map<slot_offset_type, std::shared_ptr<CommitChunk>, SlotLess>> chunks_;

  // The current trim pos of the log.
  //
  batt::Watch<slot_offset_type> trim_pos_{0};

  // The current flush pos of the log.
  //
  batt::Watch<slot_offset_type> flush_pos_{0};

  // The current commit pos of the log.
  //
  batt::Watch<slot_offset_type> commit_pos_{0};

  // The current size (bytes) of the log; this is all committed data.
  //
  batt::Watch<u64> size_{0};

  // The simulated writer impl.
  //
  std::unique_ptr<WriterImpl> writer_impl_;

  // Reflects the current "closed" state of the device.
  //
  batt::Watch<bool> closed_{false};
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_IMPL_HPP
