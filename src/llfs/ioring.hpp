//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_HPP
#define LLFS_IORING_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/api_types.hpp>
#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/seq.hpp>
#include <llfs/status.hpp>
#include <llfs/system_config.hpp>

#include <batteries/async/handler.hpp>
#include <batteries/async/task.hpp>
#include <batteries/buffer.hpp>
#include <batteries/math.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/syscall_retry.hpp>

#include <boost/beast/core/buffers_range.hpp>

#include <glog/logging.h>

#include <liburing.h>

#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <atomic>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//

inline constexpr u64 disk_block_floor(u64 n)
{
  return n & ~u64{511};
}

inline constexpr u64 disk_block_ceil(u64 n)
{
  return disk_block_floor(n + 511);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoRing
{
  struct Impl {
    std::mutex mutex_;
    struct io_uring ring_;
    bool ring_init_{false};
    std::atomic<isize> work_count_{0};
    std::atomic<bool> needs_reset_{false};
    int event_fd_{-1};

    ~Impl() noexcept;
  };

 public:
  class File;

  using CompletionHandler = batt::AbstractHandler<StatusOr<i32>>;

  static StatusOr<IoRing> make_new(MaxQueueDepth entries) noexcept;

  template <typename Fn>
  struct OpHandler {
    using allocator_type = boost::asio::associated_allocator_t<Fn>;

    Fn fn_;
    usize iov_count_ = 0;
    struct iovec iov_[0];  // MUST BE LAST!

    explicit OpHandler(Fn&& fn) noexcept : fn_{BATT_FORWARD(fn)}
    {
    }

    allocator_type get_allocator() const noexcept
    {
      return boost::asio::get_associated_allocator(fn_);
    }

    template <typename... Args>
    void operator()(Args&&... args)
    {
      this->fn_(BATT_FORWARD(args)...);
    }

    // Append a buffer to the end of `this->iov_`.
    //
    // This function MUST NOT be called more times than the value `entries` passed to
    // `OpHandler::make_new` used to allocate this object.
    //
    template <typename B>
    void push_buffer(const B& buf)
    {
      struct iovec& iov = this->iov_[this->iov_count_];
      this->iov_count_ += 1;
      iov.iov_base = (void*)buf.data();
      iov.iov_len = buf.size();
    }

    // Consume `byte_count` bytes from the front of the buffers list in `this->iov_`.
    //
    void shift_buffer(usize byte_count)
    {
      struct iovec* next = this->iov_;
      struct iovec* last = this->iov_ + this->iov_count_;

      while (next != last && byte_count) {
        usize n_to_consume = std::min(next->iov_len, byte_count);
        next->iov_base = ((u8*)next->iov_base) + n_to_consume;
        next->iov_len -= n_to_consume;
        byte_count -= n_to_consume;
        ++next;
      }

      if (this->iov_ != next) {
        struct iovec* new_last = std::copy(next, last, this->iov_);
        this->iov_count_ = std::distance(this->iov_, new_last);
      }
    }
  };

  IoRing(const IoRing&) = delete;
  IoRing& operator=(const IoRing&) = delete;

  IoRing(IoRing&&) = default;
  IoRing& operator=(IoRing&&) = default;

  template <typename Handler, typename BufferSequence>
  void submit(
      BufferSequence&& buffers, Handler&& handler,
      std::function<void(struct io_uring_sqe*, OpHandler<std::decay_t<Handler>>&)>&& start_op);

  Status run();

  void reset();

  void on_work_started();

  void on_work_finished();

  template <typename Handler>
  void post(Handler&& handler)
  {
    static const std::vector<ConstBuffer> empty;

    // Submit a no-op to wake the run loop.
    //
    this->submit(empty, BATT_FORWARD(handler), [](struct io_uring_sqe* sqe, auto&&) {
      io_uring_prep_nop(sqe);
    });
  }

  void stop();

  Status register_buffers(batt::BoxedSeq<MutableBuffer>&& buffers);

 private:
  explicit IoRing(std::unique_ptr<Impl>&& impl) noexcept;

  template <typename Fn, typename BufferSequence>
  static batt::HandlerImpl</*HandlerFn=*/OpHandler<std::decay_t<Fn>>, /*Args...=*/StatusOr<i32>>*
  wrap_handler(Fn&& fn, BufferSequence&& bufs);

  void invoke_handler(struct io_uring_cqe* cqe);

  std::unique_ptr<Impl> impl_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
template <typename Fn, typename BufferSequence>
inline auto IoRing::wrap_handler(Fn&& fn, BufferSequence&& bufs)
    -> batt::HandlerImpl</*HandlerFn=*/OpHandler<std::decay_t<Fn>>, /*Args...=*/StatusOr<i32>>*
{
  auto buf_seq = boost::beast::buffers_range_ref(bufs);
  const usize buf_count = std::distance(std::begin(buf_seq), std::end(buf_seq));
  const usize extra_bytes = buf_count * sizeof(struct iovec);

  auto* op = batt::HandlerImpl</*HandlerFn=*/OpHandler<std::decay_t<Fn>>,
                               /*Args...=*/StatusOr<i32>>::make_new(BATT_FORWARD(fn), extra_bytes);
  BATT_CHECK_NOT_NULLPTR(op);

  for (const auto& buf : buf_seq) {
    op->get_fn().push_buffer(buf);
  }

  return op;
}

template <typename Handler, typename BufferSequence>
inline void IoRing::submit(
    BufferSequence&& buffers, Handler&& handler,
    std::function<void(struct io_uring_sqe*, OpHandler<std::decay_t<Handler>>&)>&& start_op)

{
  std::unique_lock<std::mutex> lock{this->impl_->mutex_};

  struct io_uring_sqe* sqe = io_uring_get_sqe(&this->impl_->ring_);
  BATT_CHECK_NOT_NULLPTR(sqe);

  auto* op_handler = wrap_handler(BATT_FORWARD(handler), BATT_FORWARD(buffers));

  BATT_STATIC_ASSERT_TYPE_EQ(decltype(op_handler->get_fn()), OpHandler<std::decay_t<Handler>>&);

  // Initiate the operation.
  //
  start_op(sqe, op_handler->get_fn());

  // Set user data.
  //
  io_uring_sqe_set_data(sqe, op_handler);

  // Increment work count; decrement in invoke_handler.
  //
  DVLOG(1) << "(submit) before; " << BATT_INSPECT(this->impl_->work_count_);
  this->on_work_started();
  DVLOG(1) << "(submit) after; " << BATT_INSPECT(this->impl_->work_count_);

  // Finally, submit the request.
  //
  BATT_CHECK_EQ(1, io_uring_submit(&this->impl_->ring_)) << std::strerror(errno);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// An IoRing with fixed-size thread pool; the thread pool and the IoRing are shut down when the last
// active copy of an original ScopedIoRing object goes out of scope.  ScopedIoRing is move-only.
//
class ScopedIoRing
{
 public:
  class Impl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Creates a new IoRing with the specified number of completion queue entries and a thread pool of
  // the specified size; returns error Status if the IoRing could not be created.
  //
  static StatusOr<ScopedIoRing> make_new(MaxQueueDepth entries, ThreadPoolSize n_threads) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ScopedIoRing(const ScopedIoRing&) = delete;
  ScopedIoRing& operator=(const ScopedIoRing&) = delete;

  ScopedIoRing(ScopedIoRing&&) = default;
  ScopedIoRing& operator=(ScopedIoRing&&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Returns true iff this object is valid; a ScopedIoRing object is valid until its value is
  // move-copied to another ScopedIoRing variable.
  //
  explicit operator bool() const
  {
    return this->impl_ != nullptr;
  }

  // Returns a reference to the IoRing contained by this; if this is invalid (see operator bool),
  // behavior is undefined.
  //
  IoRing& get() const;

  // Initiates a graceful shutdown of the IoRing and thread pool.
  //
  void halt();

  // Blocks the current task/thread until the thread pool has exited.  NOTE: this function does not
  // initiate a shutdown, see `ScopedIoRing::halt()`.
  //
  void join();

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Constructs a new ScopedIoRing object with the given Impl.
  //
  explicit ScopedIoRing(std::unique_ptr<Impl>&& impl) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Pointer to Impl which contains the state for this object.
  //
  std::unique_ptr<Impl> impl_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// The state of a ScopedIoRing; this is separate class to allow ScopedIoRing to be moved while
// keeping the address of the underyling IoRing object stable for the sake of the backing thread
// pool.
//
class ScopedIoRing::Impl
{
 public:
  // Impl is strictly non-copyable!
  //
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  // Create a new Impl with the given IoRing and a thread pool of the given size.
  //
  explicit Impl(IoRing&& io, ThreadPoolSize n_threads) noexcept;

  // Gracefully shuts down the IoRing and thread pool, waiting for all threads to join before
  // returning.
  //
  ~Impl() noexcept;

  // The thread function used by the contained thread pool; calls `IoRing::run()`
  //
  void io_thread_main();

  // Returns a reference to the IoRing.
  //
  IoRing& get();

  // Initiates graceful shutdown.
  //
  void halt();

  // Waits for shutdown to complete.
  //
  void join();

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The IoRing passed in at creation time.
  //
  IoRing io_;

  // The threads comprising the pool which processes I/O completion events.
  //
  std::vector<std::thread> threads_;

  // Flag that indicates whether halt has been called; this makes it safe to call `halt()` multiple
  // times (only the first time has any effect).
  //
  std::atomic<bool> halted_;
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_HPP
