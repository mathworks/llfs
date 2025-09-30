//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE2_HPP
#define LLFS_IORING_LOG_DEVICE2_HPP

#include <llfs/config.hpp>
//
#include <llfs/basic_ring_buffer_log_device.hpp>
#include <llfs/confirm.hpp>
#include <llfs/ioring_log_config2.hpp>
#include <llfs/ioring_log_device2_metrics.hpp>
#include <llfs/ioring_log_device_storage.hpp>
#include <llfs/log_device.hpp>
#include <llfs/log_device_runtime_options.hpp>
#include <llfs/packed_log_control_block2.hpp>
#include <llfs/ring_buffer.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/math.hpp>
#include <batteries/status.hpp>
#include <batteries/tuples.hpp>

#include <chrono>
#include <deque>
#include <thread>
#include <vector>

namespace llfs {

BATT_STRONG_TYPEDEF(slot_offset_type, TargetTrimPos);
BATT_STRONG_TYPEDEF(slot_offset_type, CommitPos);

/** \brief Initializes an IoRingLogDevice2 using the given storage device and configuration (which
 * includes offset within the passed device).
 */
Status initialize_log_device2(RawBlockFile& file, const IoRingLogConfig2& config,
                              ConfirmThisWillEraseAllMyData confirm);

template <typename StorageT>
class IoRingLogDriver2
{
 public:
  using Self = IoRingLogDriver2;
  using AlignedUnit = std::aligned_storage_t<kDirectIOBlockSize, kDirectIOBlockAlign>;
  using EventLoopTask = typename StorageT::EventLoopTask;
  using Metrics = IoRingLogDevice2Metrics;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr batt::StaticType<TargetTrimPos> kTargetTrimPos{};
  static constexpr batt::StaticType<CommitPos> kCommitPos{};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The size (bytes) of each preallocated completion handler memory object.
   */
  static constexpr usize kHandlerMemorySizeBytes = 160;

  using HandlerMemory = batt::HandlerMemory<kHandlerMemorySizeBytes>;

  using HandlerMemoryStorage =
      std::aligned_storage_t<sizeof(HandlerMemory), alignof(HandlerMemory)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingLogDriver2(LogStorageDriverContext& context,        //
                            const IoRingLogConfig2& config,          //
                            const LogDeviceRuntimeOptions& options,  //
                            StorageT&& storage                       //
                            ) noexcept;

  ~IoRingLogDriver2() noexcept;

  //----

  Status set_trim_pos(slot_offset_type trim_pos)
  {
    this->observed_watch_[kTargetTrimPos].set_value(trim_pos);
    BATT_REQUIRE_OK(this->await_trim_pos(trim_pos));
    return OkStatus();
  }

  slot_offset_type get_trim_pos() const
  {
    return this->trim_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->trim_pos_);
  }

  //----

  slot_offset_type get_flush_pos() const
  {
    return this->flush_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->flush_pos_);
  }

  //----

  Status set_commit_pos(slot_offset_type commit_pos)
  {
    this->observed_watch_[kCommitPos].set_value(commit_pos);
    return OkStatus();
  }

  slot_offset_type get_commit_pos() const
  {
    return this->observed_watch_[kCommitPos].get_value();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->observed_watch_[kCommitPos]);
  }

  //----

  Status open() noexcept;

  Status close();

  void halt();

  void join();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const Metrics& metrics() const noexcept
  {
    return this->metrics_;
  }

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the current value of either the TargetTrimPos or CommitPos Watch.
   */
  template <typename T>
  T observe(T) const noexcept
  {
    return T{this->observed_watch_[batt::StaticType<T>{}].get_value()};
  }

  /** \brief Reads the control block into memory and initializes member data that depend on it.
   */
  Status read_control_block();

  /** \brief Reads the entire contents of the log into the ring buffer.
   */
  Status read_log_data();

  /** \brief Forces all slot lower bound pointers to `new_trim_pos`.
   *
   * Resets:
   *  - this->trim_pos_
   *  - this->target_trim_pos_
   *
   * Should only be called during recovery.
   */
  void reset_trim_pos(slot_offset_type new_trim_pos);

  /** \brief Forces all slot upper bound pointers to `new_flush_pos`.
   *
   * Resets:
   *  - this->flush_pos_
   *  - this->commit_pos_
   *  - this->started_flush_upper_bound_
   *  - this->known_flush_pos_
   *  - this->known_flushed_commit_pos_
   *
   * Should only be called during recovery.
   */
  void reset_flush_pos(slot_offset_type new_flush_pos);

  /** \brief Checks to see what activities can be initiated to advance the state of the driver.
   *
   * - Starts flushing data from the ring buffer, if conditions are met (see start_flush)
   * - Starts updating the control block, if trim/flush are out of date
   * - Starts waiting for updates from the target trim and commit pos
   */
  void poll() noexcept;

  /** \brief Initiates an async wait on the specified watched value (either CommitPos or
   * TargetTrimPos), if there is not already a pending wait operation in progress.
   */
  template <typename T>
  void wait_for_slot_offset_change(T observed_value) noexcept;

  /** \brief Checks to see if data can be flushed, and initiates at least one async write if so.
   *
   * The following conditions must be met to initiate an async write:
   *
   *  - The current number of writes pending must be below the maximum limit
   *  - There must be some unflushed data in the ring buffer
   *  - The amount of unflushed data must be at least the flush delay threshold OR there are no
   *    writes pending currently
   *
   * If the unflushed data region spans the upper bound of the ring buffer (wrap-around), then up to
   * two writes may be initiated by this function, provided the conditions above are still met after
   * starting the first write.
   */
  void start_flush(const CommitPos observed_commit_pos);

  /** \brief Returns the passed slot range with the lower and upper bounds aligned to the nearest
   * data page boundary.
   */
  SlotRange get_aligned_range(const SlotRange& slot_range) const noexcept;

  /** \brief Returns the slot range corresponding to the trailing data page of the passed range.
   */
  SlotRange get_aligned_tail(const SlotRange& aligned_range) const noexcept;

  /** \brief Starts writing the aligned range from the ring buffer to the storage media.
   *
   * Unlike start_flush, this function unconditionally starts an async write.
   */
  void start_flush_write(const SlotRange& slot_range, const SlotRange& aligned_range) noexcept;

  /** \brief Completion handler for a completed data flush write operation.
   *
   * Will initiate another write if the tail of the aligned range is now dirty and another flush
   * write has been initiated since the write that caused this handler to be called.
   *
   * Always calls poll unless there is an error status (result).
   */
  void handle_flush_write(const SlotRange& slot_range, const SlotRange& aligned_range,
                          StatusOr<i32> result);

  /** \brief Updates this->known_flush_pos_ to include the passed flushed_range.
   *
   * Inserts the flushed range into this->flushed_ranges_ min-heap and then consumes all available
   * contiguous ranges to advance this->known_flush_pos_.
   *
   * This should be called once some slot range is known to have been successfully flushed to the
   * storage media.
   */
  void update_known_flush_pos(const SlotRange& flushed_range) noexcept;

  /** \brief Initiates a rewrite of the control block if necessary.
   *
   * The control block must be updated when the target trim pos or known flush pos becomes out of
   * sync with the last written values.
   *
   * Only one pending async write to the control block is allowed at a time.
   */
  void start_control_block_update(TargetTrimPos observed_target_trim_pos) noexcept;

  /** \brief I/O callback that handles the completion of a write to the control block.
   */
  void handle_control_block_update(StatusOr<i32> result) noexcept;

  /** \brief Handles all write errors.
   */
  void handle_write_error(Status status) noexcept;

  /** \brief Returns a wrapped handler for async writes.
   *
   * Automatically injects `this` as the first arg to `handler`, so that callers need not capture
   * this in handler itself.
   */
  template <typename HandlerFn>
  auto make_write_handler(HandlerFn&& handler);

  /** \brief Allocates an array of HandlerMemoryStorage objects, adding each to the free pool linked
   * list (this->handler_memory_pool_).
   */
  void initialize_handler_memory_pool();

  /** \brief Pops the next HandlerMemoryStorage object off this->handler_memory_pool_ and uses it to
   * construct a new HandlerMemory object, returning a pointer to the newly constructed
   * HandlerMemory.
   *
   * This function MUST only be called on the event loop thread.
   */
  HandlerMemory* alloc_handler_memory() noexcept;

  /** \brief Destructs the passed HandlerMemory object and adds its storage back to the pool.
   *
   * This function MUST only be called on the event loop thread.
   */
  void free_handler_memory(HandlerMemory* p_mem) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Diagnostic metrics for this object.
   */
  Metrics metrics_;

  /** \brief The context passed in at construction time; provides access to the ring buffer.
   */
  LogStorageDriverContext& context_;

  /** \brief The configuration of the log; passed in at construction time.
   */
  const IoRingLogConfig2 config_;

  /** \brief Runtime configuration options, such as the maximum number of concurrent writes allowed.
   */
  const LogDeviceRuntimeOptions options_;

  /** \brief Provides access to the underlying storage media.
   */
  StorageT storage_;

  /** \brief The minimum IO page size for the storage media.
   */
  const usize device_page_size_ = usize{1} << this->config_.device_page_size_log2;

  /** \brief The size to which data writes must be aligned; this is also the distance from the start
   * of the control block to the start of the log data storage area.  Typically aligns with memory
   * pages (4k by default).
   */
  const usize data_page_size_ = usize{1} << this->config_.data_alignment_log2;

  /** \brief The absolute file (media) offset of the beginning of the data region.
   */
  i64 data_begin_;

  /** \brief The absolute file (media) offset of the end (non-inclusive) of the data region.
   */
  i64 data_end_;

  /** \brief True iff storage_.on_work_started() has been called (inside open).
   */
  std::atomic<bool> work_started_{false};

  /** \brief True iff halt has been called.
   */
  std::atomic<bool> halt_requested_{false};

  /** \brief Stores the two externally modifiable Watch objects (target trim pos and commit pos).
   */
  batt::StaticTypeMap<std::tuple<TargetTrimPos, CommitPos>, batt::Watch<slot_offset_type>>
      observed_watch_;

  /** \brief Stores the wait status of the two observed watches (i.e., whether we are currently
   * waiting on updates for each watch).
   */
  batt::StaticTypeMap<std::tuple<TargetTrimPos, CommitPos>, bool> waiting_;

  /** \brief The confirmed trim position; this is set only after a successful re-write of the
   * control block.
   */
  batt::Watch<slot_offset_type> trim_pos_{0};

  /** \brief The confirmed flush position; this is set only after a successful re-write of the
   * control block.
   */
  batt::Watch<slot_offset_type> flush_pos_{0};

  /** \brief The greatest lower bound of unflushed log data.  This is compared against the commit
   * pos to see whether there is any unflushed data.
   */
  slot_offset_type unflushed_lower_bound_ = 0;

  /** \brief The least upper bound of contiguous flushed data in the log.  Updates to this value may
   * cause this->known_flushed_commit_pos_ to advance.
   */
  slot_offset_type known_flush_pos_ = 0;

  /** \brief The highest observed commit pos known to be flushed.  Updates to this value
   * should trigger an update of the control block.
   */
  slot_offset_type known_flushed_commit_pos_ = 0;

  /** \brief The trailing aligned data page for the highest-offset pending flush operation.  This is
   * used to avoid concurrently updating the same data page.
   */
  Optional<SlotRange> flush_tail_;

  /** \brief Set to true when we are re-writing the control block; used to avoid concurrent writes
   * to the control block.
   */
  bool writing_control_block_ = false;

  /** \brief The current number of pending flush writes.  This does not include writing to the
   * control block.
   */
  usize writes_pending_ = 0;

  /** \brief Buffer containing the control block structure.
   */
  std::unique_ptr<AlignedUnit[]> control_block_memory_;

  /** \brief The data buffer used to write updates to the control block; points at
   * this->control_block_memory_.
   */
  ConstBuffer control_block_buffer_;

  /** \brief Pointer to initialized control block for the log.
   */
  PackedLogControlBlock2* control_block_ = nullptr;

  /** \brief A queue of observed commit positions that have triggered an async write.
   */
  std::deque<CommitPos> observed_commit_offsets_;

  /** \brief A min-heap of confirmed flushed slot ranges; used to advance this->known_flush_pos_,
   * which in turn drives the update of the control block and this->flush_pos_.
   */
  std::vector<SlotRange> flushed_ranges_;

  /** \brief A background task running the storage event loop.
   */
  Optional<EventLoopTask> event_loop_task_;

  /** \brief Used to make sure certain operations are race-free (i.e. they only happen on the event
   * loop task thread).
   */
  std::thread::id event_thread_id_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // HandlerMemory pool.
  //----- --- -- -  -  -   -

  /** \brief Aligned storage for HandlerMemory objects.
   */
  std::unique_ptr<HandlerMemoryStorage[]> handler_memory_;

  /** \brief The head of a single-linked list of free HandlerMemoryStorage objects.
   */
  HandlerMemoryStorage* handler_memory_pool_ = nullptr;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A LogDevice impl that flushes data on durable media asynchronously using an io_uring-like
 * I/O layer.
 *
 * This template is parameterized on the StorageT type, which implements the low-level async I/O
 * methods and the async event threading model.  IoRingLogDevice2 is type alias that instantiates
 * this template with the default storage type, DefaultIoRingLogDeviceStorage.
 */
template <typename StorageT>
class BasicIoRingLogDevice2
    : public BasicRingBufferLogDevice<
          /*Impl=*/IoRingLogDriver2<StorageT>>
{
 public:
  using Super = BasicRingBufferLogDevice<
      /*Impl=*/IoRingLogDriver2<StorageT>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BasicIoRingLogDevice2(const IoRingLogConfig2& config,
                                 const LogDeviceRuntimeOptions& options,
                                 StorageT&& storage) noexcept
      : Super{RingBuffer::TempFile{.byte_size = BATT_CHECKED_CAST(usize, config.log_capacity)},
              config, options, std::move(storage)}
  {
  }

  const IoRingLogDevice2Metrics& metrics() const noexcept
  {
    return this->driver().impl().metrics();
  }
};

/** \brief A fast, durable LogDevice implementation.
 */
using IoRingLogDevice2 = BasicIoRingLogDevice2<DefaultIoRingLogDeviceStorage>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A factory that produces IoRingLogDevice2 instances of the given size.
 */
class IoRingLogDevice2Factory : public LogDeviceFactory
{
 public:
  explicit IoRingLogDevice2Factory(
      int fd, const FileOffsetPtr<const PackedLogDeviceConfig2&>& packed_config,
      const LogDeviceRuntimeOptions& options) noexcept;

  explicit IoRingLogDevice2Factory(int fd, const IoRingLogConfig2& config,
                                   const LogDeviceRuntimeOptions& options) noexcept;

  ~IoRingLogDevice2Factory() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<std::unique_ptr<IoRingLogDevice2>> open_ioring_log_device();

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  int fd_;
  IoRingLogConfig2 config_;
  LogDeviceRuntimeOptions options_;
};

}  // namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE2_HPP

#include <llfs/ioring_log_device2.ipp>
