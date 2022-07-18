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

StatusOr<IoRing> IoRing::make_new(MaxQueueDepth entries) noexcept
{
  auto impl = std::make_unique<IoRing::Impl>();

  impl->event_fd_ = eventfd(0, 0);
  if (impl->event_fd_ < 0) {
    LLFS_LOG_ERROR() << "failed to create eventfd: " << std::strerror(errno);
    return batt::status_from_retval(impl->event_fd_);
  }

  int retval = io_uring_queue_init(entries, &impl->ring_, /*flags=*/0);
  if (retval != 0) {
    LLFS_LOG_ERROR() << "failed io_uring_queue_init: " << std::strerror(-retval);
    return status_from_retval(retval);
  }

  retval = io_uring_register_eventfd(&impl->ring_, impl->event_fd_);
  if (retval != 0) {
    LLFS_LOG_ERROR() << "failed io_uring_register_eventfd: " << std::strerror(-retval);
  }

  return {IoRing{std::move(impl)}};
}

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

IoRing::IoRing(std::unique_ptr<Impl>&& impl) noexcept : impl_{std::move(impl)}
{
}

Status IoRing::run()
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
        invoke_handler(&cqe);
        LLFS_DVLOG(1) << "IoRing::run() ... " << BATT_INSPECT(this->impl_->work_count_);
      } catch (...) {
        LLFS_LOG_ERROR() << "Uncaught exception";
      }
    }
  }
  LLFS_DVLOG(1) << "IoRing::run() " << BATT_INSPECT(this->impl_->work_count_) << " LEAVING";

  return OkStatus();
}

void IoRing::invoke_handler(struct io_uring_cqe* cqe)
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

void IoRing::on_work_started()
{
  this->impl_->work_count_.fetch_add(1);
}

void IoRing::on_work_finished()
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

void IoRing::stop()
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

void IoRing::reset()
{
  this->impl_->needs_reset_ = false;
}

Status IoRing::register_buffers(BoxedSeq<MutableBuffer>&& buffers)
{
  std::vector<struct iovec> iov = std::move(buffers) | seq::map([](const MutableBuffer& b) {
                                    struct iovec v;
                                    v.iov_base = b.data();
                                    v.iov_len = b.size();
                                    return v;
                                  }) |
                                  seq::collect_vec();

  const int retval = io_uring_register_buffers(&this->impl_->ring_, iov.data(), iov.size());
  return status_from_retval(retval);
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
IoRing& ScopedIoRing::get() const
{
  BATT_ASSERT_NOT_NULLPTR(this->impl_);
  return this->impl_->get();
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
  // IMPORTANT: we must call on_work_started before launching threads so that `IoRing::run` doesn't
  // exit prematurely.
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
IoRing& ScopedIoRing::Impl::get()
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
