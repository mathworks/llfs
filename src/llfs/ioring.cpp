//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring.hpp>
//

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>

#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <fcntl.h>
#include <cstdlib>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<IoRing> IoRing::make_new(MaxQueueDepth entries) noexcept
{
  auto impl = std::make_unique<IoRing::Impl>();

  // Create the event_fd so we can wake the ioring completion event loop.
  {
    BATT_CHECK_EQ(impl->event_fd_, -1);

    impl->event_fd_ = eventfd(0, 0);
    if (impl->event_fd_ < 0) {
      LLFS_LOG_ERROR() << "failed to create eventfd: " << std::strerror(errno);
      return batt::status_from_retval(impl->event_fd_);
    }

    BATT_CHECK_NE(impl->event_fd_, -1);
  }

  // Initialize the io_uring.
  {
    BATT_CHECK(!impl->ring_init_);

    const int retval = io_uring_queue_init(entries, &impl->ring_, /*flags=*/0);
    if (retval != 0) {
      LLFS_LOG_ERROR() << "failed io_uring_queue_init: " << std::strerror(-retval);
      return batt::status_from_retval(retval);
    }
    impl->ring_init_ = true;
  }

  // Register the event_fd with the io_uring.
  {
    BATT_CHECK_NE(impl->event_fd_, -1);

    const int retval = io_uring_register_eventfd(&impl->ring_, impl->event_fd_);
    if (retval != 0) {
      LLFS_LOG_ERROR() << "failed io_uring_register_eventfd: " << std::strerror(-retval);
      return batt::status_from_errno(-retval);
    }
  }

  return {IoRing{std::move(impl)}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::Impl::~Impl() noexcept
{
  LLFS_DVLOG(1) << "IoRing::Impl::~Impl()";

  if (this->ring_init_) {
    io_uring_queue_exit(&this->ring_);
    this->ring_init_ = false;
  }

  if (this->event_fd_ >= 0) {
    ::close(this->event_fd_);
    this->event_fd_ = -1;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::IoRing(std::unique_ptr<Impl>&& impl) noexcept : impl_{std::move(impl)}
{
  BATT_CHECK(this->impl_->ring_init_);
  BATT_CHECK_GE(this->impl_->event_fd_, 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::run() const
{
  while (this->impl_->work_count_ && !this->impl_->needs_reset_) {
    LLFS_DVLOG(1) << "IoRing::run() " << BATT_INSPECT(this->impl_->work_count_);

    // Block on the event_fd until a completion event is available.
    {
      eventfd_t v;
      LLFS_DVLOG(1) << "IoRing::run() reading eventfd";
      int retval = eventfd_read(this->impl_->event_fd_, &v);
      if (retval != 0) {
        return status_from_retval(retval);
      }
    }

    // The inner loop dequeues completion events until we would block.
    //
    for (;;) {
      struct io_uring_cqe cqe;
      {
        LLFS_DVLOG(1) << "IoRing::run() locking mutex";
        std::unique_lock<std::mutex> lock{this->impl_->mutex_};

        struct io_uring_cqe* p_cqe = nullptr;

        // Dequeue a single completion event from the ring buffer.
        //
        LLFS_DVLOG(1) << "IoRing::run() io_uring_peek_cqe";
        const int retval = io_uring_peek_cqe(&this->impl_->ring_, &p_cqe);
        if (retval == -EAGAIN) {
          LLFS_DVLOG(1) << "IoRing::run() io_uring_peek_cqe: EAGAIN";
          break;
        }
        if (retval < 0) {
          LLFS_DVLOG(1) << "IoRing::run() io_uring_peek_cqe: fail, retval=" << retval;
          Status status = status_from_retval(retval);
          LLFS_LOG_WARNING() << "io_uring_wait_cqe failed: " << status;
          return status;
        }
        BATT_CHECK_NOT_NULLPTR(p_cqe);

        cqe = *p_cqe;

        // Consume the event so the ring buffer can move on.
        //
        LLFS_DVLOG(1) << "IoRing::run() io_uring_cqe_seen";
        io_uring_cqe_seen(&this->impl_->ring_, p_cqe);
      }

      // Invoke the associated handler.  This will also decrement the activity counter.
      //
      try {
        LLFS_DVLOG(1) << "IoRing::run() invoke_handler " << BATT_INSPECT(this->impl_->work_count_);
        this->invoke_handler(&cqe);
        LLFS_DVLOG(1) << "IoRing::run() ... " << BATT_INSPECT(this->impl_->work_count_);
      } catch (...) {
        LLFS_LOG_ERROR() << "Uncaught exception";
      }
    }
  }
  LLFS_DVLOG(1) << "IoRing::run() " << BATT_INSPECT(this->impl_->work_count_) << " LEAVING";

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRing::invoke_handler(struct io_uring_cqe* cqe) const
{
  auto* handler = (CompletionHandler*)io_uring_cqe_get_data(cqe);
  BATT_CHECK_NOT_NULLPTR(handler);

  if (cqe->res < 0) {
    auto status = status_from_errno(-cqe->res);
    LLFS_VLOG(1) << "ioring op failed: " << status;
    handler->notify(status);
  } else {
    handler->notify(cqe->res);
  }
  //
  // No need to explicitly delete/free (handler->notify will take care of that).

  this->impl_->work_count_.fetch_sub(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRing::on_work_started() const
{
  this->impl_->work_count_.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRing::on_work_finished() const
{
  static const std::vector<ConstBuffer> empty;

  LLFS_DVLOG(1) << "IoRing::on_work_finished()";

  // Submit a no-op to wake the run loop.
  //
  this->submit(
      empty,
      [this](StatusOr<i32>) {
        this->impl_->work_count_.fetch_sub(1);
      },
      [](struct io_uring_sqe* sqe, auto&&) {
        io_uring_prep_nop(sqe);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRing::stop() const
{
  static const std::vector<ConstBuffer> empty;

  this->impl_->needs_reset_ = true;

  // Submit a no-op to wake the run loop.
  //
  this->submit(
      empty,
      [this](StatusOr<i32>) {
      },
      [](struct io_uring_sqe* sqe, auto&&) {
        io_uring_prep_nop(sqe);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRing::reset() const
{
  this->impl_->needs_reset_ = false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::register_buffers(BoxedSeq<MutableBuffer>&& buffers) const
{
  std::vector<struct iovec> iov = std::move(buffers) | seq::map([](const MutableBuffer& b) {
                                    struct iovec v;
                                    v.iov_base = b.data();
                                    v.iov_len = b.size();
                                    return v;
                                  }) |
                                  seq::collect_vec();
  {
    std::unique_lock<std::mutex> lock{this->impl_->mutex_};

    Status unregistered = this->unregister_buffers_with_lock(lock);
    BATT_REQUIRE_OK(unregistered);
    BATT_CHECK(!this->impl_->buffers_registered_);

    const int retval = io_uring_register_buffers(&this->impl_->ring_, iov.data(), iov.size());
    BATT_REQUIRE_OK(status_from_retval(retval));

    this->impl_->buffers_registered_ = true;
    return OkStatus();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::unregister_buffers() const
{
  std::unique_lock<std::mutex> lock{this->impl_->mutex_};

  return this->unregister_buffers_with_lock(lock);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::unregister_buffers_with_lock(const std::unique_lock<std::mutex>&) const
{
  if (this->impl_->buffers_registered_) {
    const int retval = io_uring_unregister_buffers(&this->impl_->ring_);
    BATT_REQUIRE_OK(status_from_retval(retval));

    this->impl_->buffers_registered_ = false;
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> IoRing::register_fd(i32 system_fd) const
{
  std::unique_lock<std::mutex> lock{this->impl_->mutex_};

  i32 user_fd = -1;

  if (this->impl_->free_fds_.empty()) {
    user_fd = this->impl_->registered_fds_.size();
    this->impl_->registered_fds_.push_back(system_fd);
  } else {
    user_fd = this->impl_->free_fds_.back();
    this->impl_->free_fds_.pop_back();
    BATT_CHECK_EQ(this->impl_->registered_fds_[user_fd], -1);
    this->impl_->registered_fds_[user_fd] = system_fd;
  }

  auto revert_fd_update = batt::finally([&] {
    this->free_user_fd(user_fd);
  });

  LLFS_VLOG(1) << "IoRing::register_fd() " << batt::dump_range(this->impl_->registered_fds_);

  const int retval = [&] {
    if (this->impl_->fds_registered_) {
      return io_uring_register_files_update(&this->impl_->ring_, /*off=*/0,
                                            /*files=*/this->impl_->registered_fds_.data(),
                                            /*nr_files=*/this->impl_->registered_fds_.size());
    } else {
      return io_uring_register_files(&this->impl_->ring_,
                                     /*files=*/this->impl_->registered_fds_.data(),
                                     /*nr_files=*/this->impl_->registered_fds_.size());
    }
  }();

  if (retval != 0) {
    return batt::status_from_errno(-retval);
  }

  this->impl_->fds_registered_ = true;

  revert_fd_update.cancel();

  return user_fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::unregister_fd(i32 user_fd) const
{
  BATT_CHECK_GE(user_fd, 0);

  std::unique_lock<std::mutex> lock{this->impl_->mutex_};

  BATT_CHECK(this->impl_->fds_registered_);

  const i32 system_fd = this->free_user_fd(user_fd);

  const int retval = [&] {
    if (this->impl_->registered_fds_.empty()) {
      return io_uring_unregister_files(&this->impl_->ring_);
    } else {
      return io_uring_register_files_update(&this->impl_->ring_, /*off=*/0,
                                            /*files=*/this->impl_->registered_fds_.data(),
                                            /*nr_files=*/this->impl_->registered_fds_.size());
    }
  }();

  if (retval != 0) {
    // Revert the change in the tables.
    //
    while (static_cast<usize>(user_fd) >= this->impl_->registered_fds_.size()) {
      this->impl_->registered_fds_.emplace_back(-1);
    }
    this->impl_->registered_fds_[user_fd] = system_fd;
    if (!this->impl_->free_fds_.empty() && this->impl_->free_fds_.back() == user_fd) {
      this->impl_->free_fds_.pop_back();
    }

    return batt::status_from_errno(-retval);
  }

  this->impl_->fds_registered_ = !this->impl_->registered_fds_.empty();

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 IoRing::free_user_fd(i32 user_fd) const
{
  BATT_CHECK_GE(user_fd, 0);
  BATT_CHECK_LT(static_cast<usize>(user_fd), this->impl_->registered_fds_.size());

  const i32 system_fd = this->impl_->registered_fds_[user_fd];

  if (static_cast<usize>(user_fd) + 1 == this->impl_->registered_fds_.size()) {
    this->impl_->registered_fds_.pop_back();
  } else {
    this->impl_->registered_fds_[user_fd] = -1;
    this->impl_->free_fds_.push_back(user_fd);
  }

  return system_fd;
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<ScopedIoRing> ScopedIoRing::make_new(MaxQueueDepth entries,
                                                         ThreadPoolSize n_threads) noexcept
{
  StatusOr<IoRing> io = IoRing::make_new(entries);
  BATT_REQUIRE_OK(io);

  return ScopedIoRing{std::make_unique<Impl>(std::move(*io), n_threads)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ScopedIoRing::ScopedIoRing(std::unique_ptr<Impl>&& impl) noexcept
    : impl_{std::move(impl)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const IoRing& ScopedIoRing::get_io_ring() const
{
  BATT_ASSERT_NOT_NULLPTR(this->impl_);
  return this->impl_->get_io_ring();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::halt()
{
  if (this->impl_) {
    this->impl_->halt();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::join()
{
  if (this->impl_) {
    this->impl_->join();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ScopedIoRing::Impl::Impl(IoRing&& io, ThreadPoolSize n_threads) noexcept
    : io_{std::move(io)}
    , threads_{}
    , halted_{false}
{
  // IMPORTANT: we must call on_work_started before launching threads so that `IoRing::run`
  // doesn't exit prematurely.
  //
  this->io_.on_work_started();

  for (usize i = 0; i < n_threads; ++i) {
    this->threads_.emplace_back([this] {
      this->io_thread_main();
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ScopedIoRing::Impl::~Impl() noexcept
{
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::Impl::io_thread_main()
{
  Status status = this->io_.run();
  LLFS_VLOG(1) << "ScopedIoRing::io_thread_main() exited with " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const IoRing& ScopedIoRing::Impl::get_io_ring()
{
  return this->io_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::Impl::halt()
{
  const bool halted_previously = this->halted_.exchange(true);
  if (!halted_previously) {
    this->io_.on_work_finished();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::Impl::join()
{
  for (std::thread& t : this->threads_) {
    t.join();
  }
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
