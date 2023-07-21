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
#include <llfs/ioring_impl.hpp>
#include <llfs/status.hpp>
#include <llfs/system_config.hpp>

#include <batteries/async/task.hpp>
#include <batteries/buffer.hpp>
#include <batteries/math.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/syscall_retry.hpp>

#include <llfs/logging.hpp>

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
 public:
  class File;

  using Impl = IoRingImpl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<IoRing> make_new(MaxQueueDepth entries) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRing(const IoRing&) = delete;
  IoRing& operator=(const IoRing&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRing(IoRing&&) = default;
  IoRing& operator=(IoRing&&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status run() const noexcept
  {
    return this->impl_->run();
  }

  void reset() const noexcept
  {
    this->impl_->reset();
  }

  void on_work_started() const noexcept
  {
    this->impl_->on_work_started();
  }

  void on_work_finished() const noexcept
  {
    this->impl_->on_work_finished();
  }

  template <typename Handler>
  void post(Handler&& handler) const noexcept
  {
    this->impl_->post(BATT_FORWARD(handler));
  }

  template <typename Handler, typename BufferSequence>
  void submit(BufferSequence&& buffers, Handler&& handler,
              std::function<void(struct io_uring_sqe*, IoRingOpHandler<std::decay_t<Handler>>&)>&&
                  start_op) const noexcept
  {
    this->impl_->submit(BATT_FORWARD(buffers), BATT_FORWARD(handler), std::move(start_op));
  }

  void stop() const noexcept
  {
    this->impl_->stop();
  }

  Status register_buffers(batt::BoxedSeq<MutableBuffer>&& buffers) const noexcept
  {
    return this->impl_->register_buffers(std::move(buffers));
  }

  Status unregister_buffers() const noexcept
  {
    return this->impl_->unregister_buffers();
  }

  /** \brief Registers the given file descriptor with the io_uring in kernel space, speeding
   * performance for repeated access to the same file.
   *
   * \return the "user_fd" that should be used to achieve faster syscalls in the future.
   */
  StatusOr<i32> register_fd(i32 system_fd) const noexcept
  {
    return this->impl_->register_fd(system_fd);
  }

  /** \brief Unregisters the given "user_fd" that was previously returned by a call to
   * `IoRing::register_fd`.
   *
   * Behavior is undefined if a user_fd obtained from one IoRing is handed to another!
   */
  Status unregister_fd(i32 user_fd) const noexcept
  {
    return this->impl_->unregister_fd(user_fd);
  }

 private:
  explicit IoRing(std::unique_ptr<Impl>&& impl) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::unique_ptr<Impl> impl_;
};

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
  // move-only, default constructible
  //
  ScopedIoRing() = default;
  //
  ScopedIoRing(const ScopedIoRing&) = delete;
  ScopedIoRing& operator=(const ScopedIoRing&) = delete;
  //
  ScopedIoRing(ScopedIoRing&&) = default;
  ScopedIoRing& operator=(ScopedIoRing&&) = default;
  //
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
  const IoRing& get_io_ring() const;

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
  const IoRing& get_io_ring();

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
