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
#include <llfs/ioring_log_config.hpp>
#include <llfs/packed_log_page_buffer.hpp>
#include <llfs/simulated_storage_object.hpp>
#include <llfs/status.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/queue.hpp>
#include <batteries/async/watch.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>

namespace llfs {

class StorageSimulation;

class SimulatedLogDeviceStorage
{
 public:
  using AlignedBlock = PackedLogPageBuffer::AlignedUnit;

  class DurableState : public SimulatedStorageObject
  {
   public:
    struct Impl {
      u64 last_recovery_step_{0};
      std::unordered_map<i64, std::unique_ptr<AlignedBlock>> blocks_;
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit DurableState(StorageSimulation& simulation, const IoRingLogConfig& config) noexcept
        : simulation_{simulation}
        , config_{config}
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    StorageSimulation& simulation() noexcept
    {
      return this->simulation_;
    }

    const IoRingLogConfig& config() const noexcept
    {
      return this->config_;
    }

    /** \brief Simulates a process termination/restart by removing all non-flushed state.
     */
    void crash_and_recover(u64 simulation_step) override
    {
      batt::ScopedLock<Impl> locked{this->impl_};

      locked->last_recovery_step_ = simulation_step;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    Status write_block(u64 creation_step, i64 offset, const ConstBuffer& buffer)
    {
      if (offset % sizeof(AlignedBlock) != 0 || buffer.size() != sizeof(AlignedBlock)) {
        return batt::StatusCode::kInvalidArgument;
      }

      batt::ScopedLock<Impl> locked{this->impl_};

      if (creation_step < locked->last_recovery_step_) {
        return batt::StatusCode::kClosed;
      }

      auto& p_block = locked->blocks_[offset];
      if (!p_block) {
        p_block = std::make_unique<AlignedBlock>();
      }
      std::memcpy(p_block.get(), buffer.data(), buffer.size());

      return batt::OkStatus();
    }

    Status read_block(u64 creation_step, i64 offset, const MutableBuffer& buffer)
    {
      if (offset % sizeof(AlignedBlock) != 0 || buffer.size() != sizeof(AlignedBlock)) {
        return batt::StatusCode::kInvalidArgument;
      }

      batt::ScopedLock<Impl> locked{this->impl_};

      if (creation_step < locked->last_recovery_step_) {
        return batt::StatusCode::kClosed;
      }

      auto& p_block = locked->blocks_[offset];
      if (!p_block) {
        return batt::StatusCode::kNotFound;
      }
      std::memcpy(buffer.data(), p_block.get(), buffer.size());

      return batt::OkStatus();
    }

   private:
    StorageSimulation& simulation_;
    const IoRingLogConfig config_;
    batt::Mutex<Impl> impl_;
  };

  class EphemeralState
  {
   public:
    explicit EphemeralState(std::shared_ptr<DurableState>&& durable_state) noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    const IoRingLogConfig& config() const noexcept
    {
      return this->durable_state_->config();
    }

    Status close()
    {
      if (this->closed_.exchange(true)) {
        return batt::StatusCode::kClosed;
      }

      return batt::OkStatus();
    }

    void on_work_started()
    {
      this->work_count_.fetch_add(1);
    }

    void on_work_finished()
    {
      if (this->work_count_.fetch_sub(1) == 1) {
        this->queue_.poke();
      }
    }

    Status run_event_loop()
    {
      for (;;) {
        if (this->stopped_.load() || this->work_count_.load() == 0) {
          break;
        }

        StatusOr<std::function<void()>> handler = this->queue_.await_next();
        if (!handler.ok()) {
          if (handler.status() == batt::StatusCode::kPoke) {
            continue;
          }

          if (handler.status() == batt::StatusCode::kClosed) {
            break;
          }

          return handler.status();
        }

        try {
          (*handler)();
        } catch (...) {
          LLFS_LOG_WARNING() << "Simulation handler threw exception!";
        }
      }

      return batt::OkStatus();
    }

    void reset_event_loop()
    {
      this->stopped_.store(false);
    }

    void post_to_event_loop(std::function<void(StatusOr<i32>)>&& handler);

    void stop_event_loop()
    {
      this->stopped_.store(true);
      this->queue_.poke();
    }

    Status read_all(i64 offset, MutableBuffer buffer)
    {
      return this->multi_block_iop<MutableBuffer, &DurableState::read_block>(offset, buffer);
    }

    void async_write_some_fixed(i64 file_offset, const ConstBuffer& data, i32 /*buf_index*/,
                                std::function<void(StatusOr<i32>)>&& handler);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    void simulation_post(std::function<void()>&& fn);

    template <typename BufferT, Status (DurableState::*Op)(u64, i64, const BufferT&)>
    Status multi_block_iop(i64 offset, BufferT buffer)
    {
      if (this->closed_.load()) {
        return batt::StatusCode::kClosed;
      }

      std::vector<Status> block_status(buffer.size() / sizeof(AlignedBlock));
      batt::Watch<usize> blocks_remaining(block_status.size());
      std::atomic<usize> pin_count{block_status.size()};

      if (buffer.size() != block_status.size() * sizeof(AlignedBlock)) {
        return batt::StatusCode::kInvalidArgument;
      }

      usize i = 0;
      while (buffer.size() > 0) {
        this->simulation_post([this, i, offset, &block_status, &blocks_remaining, &pin_count,
                               block_buffer = BufferT{buffer.data(), sizeof(AlignedBlock)}] {
          auto on_scope_exit = batt::finally([&] {
            blocks_remaining.fetch_sub(1);
            pin_count.fetch_sub(1);
          });

          block_status[i] =
              (this->durable_state_.get()->*Op)(this->creation_step_, offset, block_buffer);
        });

        offset += sizeof(AlignedBlock);
        buffer += sizeof(AlignedBlock);
        ++i;
      }

      BATT_CHECK_OK(blocks_remaining.await_equal(0));
      while (pin_count.load() != 0) {
        batt::Task::yield();
      }

      for (const Status& status : block_status) {
        BATT_REQUIRE_OK(status);
      }

      return batt::OkStatus();
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    const std::shared_ptr<DurableState> durable_state_;

    StorageSimulation& simulation_;

    const u64 creation_step_;

    std::atomic<i32> work_count_{0};

    batt::Queue<std::function<void()>> queue_;

    std::atomic<bool> closed_{false};

    std::atomic<bool> stopped_{false};
  };

  class RawBlockFileImpl : public RawBlockFile
  {
   public:
    explicit RawBlockFileImpl(std::shared_ptr<DurableState>&& durable_state) noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    StatusOr<i64> write_some(i64 offset, const ConstBuffer& buffer_arg) override
    {
      ConstBuffer buffer = buffer_arg;
      i64 n_written = 0;

      while (buffer.size() > 0) {
        BATT_REQUIRE_OK(this->durable_state_->write_block(
            this->creation_step_, offset,
            ConstBuffer{buffer.data(), std::min(buffer.size(), sizeof(AlignedBlock))}));
        offset += sizeof(AlignedBlock);
        buffer += sizeof(AlignedBlock);
        n_written += sizeof(AlignedBlock);
      }

      return n_written;
    }

    StatusOr<i64> read_some(i64 offset, const MutableBuffer& buffer_arg) override
    {
      MutableBuffer buffer = buffer_arg;
      i64 n_read = 0;

      while (buffer.size() > 0) {
        BATT_REQUIRE_OK(this->durable_state_->read_block(
            this->creation_step_, offset,
            MutableBuffer{buffer.data(), std::min(buffer.size(), sizeof(AlignedBlock))}));
        offset += sizeof(AlignedBlock);
        buffer += sizeof(AlignedBlock);
        n_read += sizeof(AlignedBlock);
      }

      return n_read;
    }

    StatusOr<i64> get_size() override
    {
      return Status{batt::StatusCode::kUnimplemented};
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    const std::shared_ptr<DurableState> durable_state_;

    const u64 creation_step_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SimulatedLogDeviceStorage(std::shared_ptr<DurableState>&& durable_state) noexcept
      : impl_{std::make_unique<EphemeralState>(std::move(durable_state))}
  {
  }

  SimulatedLogDeviceStorage(const SimulatedLogDeviceStorage&) = delete;
  SimulatedLogDeviceStorage& operator=(const SimulatedLogDeviceStorage&) = delete;

  SimulatedLogDeviceStorage(SimulatedLogDeviceStorage&&) = default;
  SimulatedLogDeviceStorage& operator=(SimulatedLogDeviceStorage&&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const IoRingLogConfig& config() const noexcept
  {
    return this->impl_->config();
  }

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

  Status run_event_loop() const noexcept
  {
    return this->impl_->run_event_loop();
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

  template <typename Handler = void(StatusOr<i32>)>
  void async_write_some_fixed(i64 file_offset, const ConstBuffer& data, i32 buf_index,
                              Handler&& handler)
  {
    this->impl_->async_write_some_fixed(file_offset, data, buf_index, BATT_FORWARD(handler));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::unique_ptr<EphemeralState> impl_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_LOG_DEVICE_STORAGE_HPP
