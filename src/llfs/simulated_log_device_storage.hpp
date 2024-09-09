//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMULATED_LOG_DEVICE_STORAGE_HPP
#define LLFS_SIMULATED_LOG_DEVICE_STORAGE_HPP

#include <llfs/config.hpp>
//
#include <llfs/interval.hpp>
#include <llfs/packed_log_page_buffer.hpp>
#include <llfs/raw_block_file.hpp>
#include <llfs/simulated_storage_object.hpp>
#include <llfs/status.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/queue.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/shared_ptr.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>

namespace llfs {

class StorageSimulation;

class SimulatedLogDeviceStorage
{
 public:
  static i64 new_id()
  {
    static std::atomic<i64> next_{0};
    return next_.fetch_add(1);
  }

  using AlignedBlock = PackedLogPageBuffer::AlignedUnit;

  class DurableState : public SimulatedStorageObject
  {
   public:
    struct Impl {
      u64 last_recovery_step_{0};
      std::unordered_map<i64, std::unique_ptr<AlignedBlock>> blocks_;
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit DurableState(StorageSimulation& simulation, usize log_size,
                          const Interval<i64>& file_offset) noexcept
        : simulation_{simulation}
        , log_size_{log_size}
        , file_offset_{file_offset}
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    i64 id() const noexcept
    {
      return this->id_;
    }

    StorageSimulation& simulation() noexcept
    {
      return this->simulation_;
    }

    usize log_size() const noexcept
    {
      return this->log_size_;
    }

    const Interval<i64>& file_offset() const noexcept
    {
      return this->file_offset_;
    }

    bool is_initialized() const noexcept
    {
      return this->is_initialized_;
    }

    void set_initialized(bool b) noexcept
    {
      this->is_initialized_ = b;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief Simulates a process termination/restart by removing all non-flushed state.
     */
    void crash_and_recover(u64 simulation_step) override;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief Simulates writing a single block.
     *
     * If simulation is injecting errors, then this call may inject a batt::StatusCode::kInternal
     * error spuriously.
     */
    Status write_block(u64 creation_step, i64 offset, const ConstBuffer& buffer);

    /** \brief Simulates writing a single block.
     *
     * If simulation is injecting errors, then this call may inject a batt::StatusCode::kInternal
     * error spuriously.
     */
    Status read_block(u64 creation_step, i64 offset, const MutableBuffer& buffer);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    /** \brief Checks the offset and buffer size args to a block operation to make sure they are
     * valid.
     *
     * \return OkStatus() if the args are valid, or an error status indicating the problem
     * otherwise.
     */
    Status validate_args(i64 offset, usize size, const char* op_type) noexcept;

    /** \brief Abstracts the common parts of the implementation of write_block and read_block.
     *
     * The passed `op_fn` will be called on the AlignedBlock pointer corresponding to `offset`, if
     * the arguments are valid and the creation step is after the most recent crash/recover step.
     */
    template <typename BufferT, typename OpFn = void(std::unique_ptr<AlignedBlock>&)>
    Status block_op_impl(u64 creation_step, i64 offset, const BufferT& buffer, OpFn&& op_fn);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    const i64 id_{SimulatedLogDeviceStorage::new_id()};

    StorageSimulation& simulation_;

    /** \brief The maximum capacity (bytes) of the log.
     */
    const usize log_size_;

    /** \brief The valid range of offsets for this log device on the storage media.
     */
    const Interval<i64> file_offset_;

    /** \brief Whether the simulated storage media has been initialized for this device.  This is
     * set by the StorageSimulation (or other external user) code.
     */
    bool is_initialized_ = false;

    batt::Mutex<Impl> impl_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief RawBlockFile interface view of the DurableState.
   *
   * Used for initialization of the simulated media.
   */
  class RawBlockFileImpl : public RawBlockFile
  {
   public:
    explicit RawBlockFileImpl(std::shared_ptr<DurableState>&& durable_state) noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    StatusOr<i64> write_some(i64 offset, const ConstBuffer& buffer_arg) override;

    StatusOr<i64> read_some(i64 offset, const MutableBuffer& buffer_arg) override;

    StatusOr<i64> get_size() override;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    const std::shared_ptr<DurableState> durable_state_;
    const u64 creation_step_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class EphemeralState : public batt::RefCounted<EphemeralState>
  {
   public:
    // Index values for this->work_count_per_caller_ array (see below).
    //
    enum : usize {
      PUBLIC_API = 0,
      POST_TO_EVENT_LOOP,
      ASYNC_WRITE_SOME,
      NUM_CALLERS,
    };

    explicit EphemeralState(std::shared_ptr<DurableState>&& durable_state) noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    batt::TaskScheduler& get_task_scheduler() noexcept;

    //----- --- -- -  -  -   -

    i64 id() const noexcept
    {
      return this->id_;
    }

    usize log_size() const noexcept
    {
      return this->durable_state_->log_size();
    }

    const Interval<i64>& file_offset() const noexcept
    {
      return this->durable_state_->file_offset();
    }

    bool is_initialized() const noexcept
    {
      return this->durable_state_->is_initialized();
    }

    void set_initialized(bool b) noexcept
    {
      this->durable_state_->set_initialized(b);
    }

    std::unique_ptr<RawBlockFileImpl> get_raw_block_file()
    {
      return std::make_unique<RawBlockFileImpl>(batt::make_copy(this->durable_state_));
    }

    //----- --- -- -  -  -   -

    Status close();

    void on_work_started(usize caller = PUBLIC_API);

    void on_work_finished(usize caller = PUBLIC_API);

    Status run_event_loop();

    void reset_event_loop();

    void post_to_event_loop(std::function<void(StatusOr<i32>)>&& handler);

    void stop_event_loop();

    Status read_all(i64 offset, const MutableBuffer& buffer);

    void async_write_some(i64 file_offset, const ConstBuffer& data,
                          std::function<void(StatusOr<i32>)>&& handler);

    i32 work_count() const noexcept
    {
      return this->work_count_.load();
    }

    bool is_stopped() const noexcept
    {
      return this->stopped_.load();
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    void simulation_post(std::function<void()>&& fn);

    template <typename BufferT, Status (DurableState::*Op)(u64, i64, const BufferT&)>
    Status multi_block_iop(i64 offset, BufferT buffer);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    const std::shared_ptr<DurableState> durable_state_;

    StorageSimulation& simulation_;

    const u64 creation_step_;

    const i64 id_ = durable_state_->id();

    std::atomic<i32> work_count_{0};

    std::array<i32, NUM_CALLERS> work_count_per_caller_;

    batt::Queue<std::function<void()>> queue_;

    std::atomic<bool> closed_{false};

    std::atomic<bool> stopped_{false};
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class EventLoopTask
  {
   public:
    explicit EventLoopTask(SimulatedLogDeviceStorage& storage, std::string_view caller) noexcept;

    EventLoopTask(const EventLoopTask&) = delete;
    EventLoopTask& operator=(const EventLoopTask&) = delete;

    ~EventLoopTask() noexcept;

    void join();

   private:
    SimulatedLogDeviceStorage& storage_;
    std::string_view caller_;
    batt::Task task_;
    bool join_called_ = false;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SimulatedLogDeviceStorage(std::shared_ptr<DurableState>&& durable_state) noexcept
      : impl_{batt::make_shared<EphemeralState>(std::move(durable_state))}
  {
    BATT_STATIC_ASSERT_TYPE_EQ(batt::SharedPtr<EphemeralState>,
                               boost::intrusive_ptr<EphemeralState>);
  }

  SimulatedLogDeviceStorage(const SimulatedLogDeviceStorage&) = delete;
  SimulatedLogDeviceStorage& operator=(const SimulatedLogDeviceStorage&) = delete;

  SimulatedLogDeviceStorage(SimulatedLogDeviceStorage&&) = default;
  SimulatedLogDeviceStorage& operator=(SimulatedLogDeviceStorage&&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize log_size() const noexcept
  {
    return this->impl_->log_size();
  }

  bool is_initialized() const noexcept
  {
    return this->impl_->is_initialized();
  }

  void set_initialized(bool b) noexcept
  {
    this->impl_->set_initialized(b);
  }

  std::unique_ptr<SimulatedLogDeviceStorage::RawBlockFileImpl> get_raw_block_file()
  {
    return this->impl_->get_raw_block_file();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status register_fd()
  {
    return batt::OkStatus();
  }

  StatusOr<usize> register_buffers(batt::BoxedSeq<MutableBuffer>&&, bool = false) const noexcept
  {
    return {0};
  }

  Status close()
  {
    return this->impl_->close();
  }

  void on_work_started() const noexcept
  {
    this->impl_->on_work_started();
  }

  void on_work_finished() const noexcept
  {
    this->impl_->on_work_finished();
  }

  void reset_event_loop() const noexcept
  {
    this->impl_->reset_event_loop();
  }

  template <typename Handler = void(StatusOr<i32>)>
  void post_to_event_loop(Handler&& handler) const noexcept
  {
    this->impl_->post_to_event_loop(BATT_FORWARD(handler));
  }

  void stop_event_loop() const noexcept
  {
    this->impl_->stop_event_loop();
  }

  Status read_all(i64 offset, MutableBuffer buffer)
  {
    return this->impl_->read_all(offset, buffer);
  }

  template <typename Handler>
  void async_write_some(i64 file_offset, const ConstBuffer& data, Handler&& handler)
  {
    this->impl_->async_write_some(file_offset, data, BATT_FORWARD(handler));
  }

  // The buf_index arg here isn't used by the simulated impl, but it is needed by the other type
  // used to instantiate IoRingLogDriver, DefaultIoRingLogDeviceStorage.
  //
  template <typename Handler = void(StatusOr<i32>)>
  void async_write_some_fixed(i64 file_offset, const ConstBuffer& data, i32 /*buf_index*/,
                              Handler&& handler)
  {
    this->impl_->async_write_some(file_offset, data, BATT_FORWARD(handler));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  batt::SharedPtr<EphemeralState> impl_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_STORAGE_HPP
