//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE_HPP
#define LLFS_IORING_LOG_DEVICE_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/basic_log_storage_driver.hpp>
#include <llfs/basic_ring_buffer_device.hpp>
#include <llfs/confirm.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/file_offset_ptr.hpp>
#include <llfs/ioring.hpp>
#include <llfs/ioring_log_driver_options.hpp>
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/log_device_config.hpp>
#include <llfs/metrics.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/task.hpp>

BATT_SUPPRESS("-Wunused-parameter")

#include <boost/heap/d_ary_heap.hpp>
#include <boost/heap/policies.hpp>

BATT_UNSUPPRESS()

#include <atomic>
#include <vector>

namespace llfs {

class IoRingLogFlushOp;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Physical configuration of the log.
//
struct IoRingLogConfig {
  // The in-memory (logical) size in bytes of the log.
  //
  u64 logical_size;

  // The offset in bytes of the log from the beginning of the file.
  //
  i64 physical_offset;

  // The physical size in bytes of the log.
  //
  u64 physical_size;

  // Specifies the size of a "flush block," the size of a single write operation while flushing,
  // in number of 4kb pages, log2.  For example:
  //
  //  | flush_block_pages_log2   | flush write buffer size   |
  //  |--------------------------|---------------------------|
  //  | 0                        | 4kb                       |
  //  | 1                        | 8kb                       |
  //  | 2                        | 16kb                      |
  //  | 3                        | 32kb                      |
  //  | 4                        | 64kb                      |
  //
  usize pages_per_block_log2 = 1;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static IoRingLogConfig from_packed(
      const FileOffsetPtr<const PackedLogDeviceConfig&>& packed_config)
  {
    return IoRingLogConfig{
        .logical_size = packed_config->logical_size,
        .physical_offset = packed_config.absolute_block_0_offset(),
        .physical_size = packed_config->physical_size,
        .pages_per_block_log2 = packed_config->pages_per_block_log2,
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize pages_per_block() const
  {
    return usize{1} << this->pages_per_block_log2;
  }

  usize block_size() const
  {
    return kLogPageSize * this->pages_per_block();
  }

  usize block_capacity() const
  {
    return this->block_size() - sizeof(PackedPageHeader);
  }

  usize block_count() const
  {
    BATT_CHECK_EQ(this->physical_size % this->block_size(), 0u)
        << "The physical size of the log must be a multiple of the block size!"
        << BATT_INSPECT(this->physical_size) << BATT_INSPECT(this->block_size())
        << BATT_INSPECT(this->pages_per_block_log2);

    return this->physical_size / this->block_size();
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoRingLogDriver
{
 public:
  friend class IoRingLogFlushOp;

  using AlignedUnit = std::aligned_storage_t<4 * kKiB, 512>;

  using PackedPageHeader = IoRingLogFlushOp::PackedPageHeader;

  struct PackedPageHeaderBuffer {
    union {
      AlignedUnit aligned_storage;
      PackedPageHeader header;
    };

    void clear()
    {
      std::memset(this, 0, sizeof(PackedPageHeaderBuffer));
    }

    ConstBuffer as_const_buffer() const
    {
      return ConstBuffer{this, sizeof(PackedPageHeaderBuffer)};
    }

    MutableBuffer as_mutable_buffer()
    {
      return MutableBuffer{this, sizeof(PackedPageHeaderBuffer)};
    }
  };
  static_assert(sizeof(PackedPageHeaderBuffer) == 4 * kKiB, "");

  using Config = IoRingLogConfig;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static usize disk_size_required_for_log_size(u64 logical_size, u64 block_size)
  {
    const u64 block_capacity = block_size - sizeof(PackedPageHeader);
    const u64 block_count =
        (logical_size + block_capacity - 1) / block_capacity + (1 /* for wrap-around */);

    return block_size * block_count;
  }

  using Options = IoRingLogDriverOptions;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Metrics {
    LatencyMetric flush_write_latency;
    CountMetric<u64> logical_bytes_flushed{0};
    CountMetric<u64> physical_bytes_flushed{0};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingLogDriver(LogStorageDriverContext& context, int fd, const Config& config,
                           const Options& options) noexcept;

  ~IoRingLogDriver() noexcept;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // A storage driver impl must provide these 9 methods.
  //
  Status set_trim_pos(slot_offset_type trim_pos);

  slot_offset_type get_trim_pos() const
  {
    return this->trim_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type trim_pos)
  {
    return await_slot_offset(trim_pos, this->trim_pos_);
  }

  //----

  // There is no set_flush_pos because it's up to the storage driver to flush log data and update
  // the flush pos in the background.

  slot_offset_type get_flush_pos() const
  {
    return this->flush_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type flush_pos)
  {
    return await_slot_offset(flush_pos, this->flush_pos_);
  }

  //----

  Status set_commit_pos(slot_offset_type commit_pos)
  {
    clamp_min_slot(this->commit_pos_, commit_pos);
    return OkStatus();
  }

  slot_offset_type get_commit_pos() const
  {
    return this->commit_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type commit_pos)
  {
    return await_slot_offset(commit_pos, this->commit_pos_);
  }

  //----

  Status open();

  Status close()
  {
    this->halt();
    this->join();
    return this->file_.close();
  }

  void halt();

  void join();

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  usize pages_per_block() const
  {
    return this->config_.pages_per_block();
  }

  usize block_size() const
  {
    return this->block_size_;
  }

  usize block_capacity() const
  {
    return this->block_capacity_;
  }

  usize queue_depth() const
  {
    return this->queue_depth_;
  }

 private:
  using SlotOffsetHeap = boost::heap::d_ary_heap<slot_offset_type,                   //
                                                 boost::heap::arity<2>,              //
                                                 boost::heap::compare<SlotGreater>,  //
                                                 boost::heap::mutable_<true>         //
                                                 >;
  class PollFlushPos
  {
   public:
    explicit PollFlushPos(IoRingLogDriver* this_);

    void operator()(IoRingLogDriver* this_);

    slot_offset_type get() const;

   private:
    usize next_op_index_;
    slot_offset_type local_flush_pos_;
    slot_offset_type op_upper_bound_;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  Status read_log_data();

  void start_flush_task();

  void flush_task_main();

  void poll_flush_state();

  void poll_commit_state();

  void wait_for_commit_pos(slot_offset_type last_seen);

  // void start_result_handler_task();

  // void result_handler_task_main();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  LogStorageDriverContext& context_;
  const Config config_;
  const u64 log_start_ = this->config_.physical_offset;
  const u64 log_end_ = this->config_.physical_offset + this->config_.physical_size;
  const usize block_size_ = this->config_.block_size();
  const usize block_capacity_ = this->config_.block_capacity();

  const Options options_;
  const std::string& name_ = this->options_.name;
  const usize queue_depth_ = this->options_.queue_depth();
  const usize queue_depth_mask_ = this->options_.queue_depth_mask();

  IoRing ioring_;
  IoRing::File file_;

  std::atomic<bool> halt_requested_{false};

  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> flush_pos_{0};
  batt::Watch<slot_offset_type> commit_pos_{0};

  std::vector<IoRingLogFlushOp> flush_ops_;
  SlotOffsetHeap waiting_for_commit_;
  bool commit_pos_listener_active_ = false;
  batt::HandlerMemory<128> commit_handler_memory_;
  PollFlushPos poll_flush_pos_{this};
  Metrics metrics_;
  Optional<batt::Task> flush_task_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class IoRingLogDeviceFactory : public LogDeviceFactory
{
 public:
  explicit IoRingLogDeviceFactory(int fd,
                                  const FileOffsetPtr<const PackedLogDeviceConfig&>& packed_config,
                                  const IoRingLogDriver::Options& options) noexcept
      : IoRingLogDeviceFactory{fd, IoRingLogDriver::Config::from_packed(packed_config), options}
  {
  }

  explicit IoRingLogDeviceFactory(int fd, const IoRingLogDriver::Config& config,
                                  const IoRingLogDriver::Options& options) noexcept
      : fd_{fd}
      , config_{config}
      , options_{options}
  {
  }

  ~IoRingLogDeviceFactory() noexcept
  {
    if (this->fd_ != -1) {
      ::close(this->fd_);
    }
  }

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override
  {
    auto instance = std::make_unique<BasicRingBufferDevice<IoRingLogDriver>>(
        RingBuffer::TempFile{.byte_size = this->config_.logical_size}, this->fd_, this->config_,
        this->options_);

    this->fd_ = -1;

    Status open_status = instance->open();
    BATT_REQUIRE_OK(open_status);

    auto scan_status =
        scan_fn(*instance->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable));
    BATT_REQUIRE_OK(scan_status);

    return instance;
  }

 private:
  int fd_;
  IoRingLogDriver::Config config_;
  IoRingLogDriver::Options options_;
};

// Write an empty log device to the given fd.
//
Status initialize_ioring_log_device(RawBlockFile& file, const IoRingLogDriver::Config& config,
                                    ConfirmThisWillEraseAllMyData confirm);

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_LOG_DEVICE_HPP
