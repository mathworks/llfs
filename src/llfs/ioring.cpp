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

#include <glog/logging.h>

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
    LOG(ERROR) << "failed to create eventfd: " << std::strerror(errno);
    return batt::status_from_retval(impl->event_fd_);
  }

  int retval = io_uring_queue_init(entries, &impl->ring_, /*flags=*/0);
  if (retval != 0) {
    LOG(ERROR) << "failed io_uring_queue_init: " << std::strerror(-retval);
    return status_from_retval(retval);
  }

  retval = io_uring_register_eventfd(&impl->ring_, impl->event_fd_);
  if (retval != 0) {
    LOG(ERROR) << "failed io_uring_register_eventfd: " << std::strerror(-retval);
  }

  return {IoRing{std::move(impl)}};
}

IoRing::Impl::~Impl() noexcept
{
  DVLOG(1) << "IoRing::Impl::~Impl()";

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
    DVLOG(1) << "IoRing::run() " << BATT_INSPECT(this->impl_->work_count_);

    // Block on the event_fd until a completion event is available.
    {
      eventfd_t v;
      DVLOG(1) << "IoRing::run() reading eventfd";
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
        DVLOG(1) << "IoRing::run() locking mutex";
        std::unique_lock<std::mutex> lock{this->impl_->mutex_};

        struct io_uring_cqe* p_cqe = nullptr;

        // Dequeue a single completion event from the ring buffer.
        //
        DVLOG(1) << "IoRing::run() io_uring_peek_cqe";
        const int retval = io_uring_peek_cqe(&this->impl_->ring_, &p_cqe);
        if (retval == -EAGAIN) {
          DVLOG(1) << "IoRing::run() io_uring_peek_cqe: EAGAIN";
          break;
        }
        if (retval < 0) {
          DVLOG(1) << "IoRing::run() io_uring_peek_cqe: fail, retval=" << retval;
          Status status = status_from_retval(retval);
          LOG(WARNING) << "io_uring_wait_cqe failed: " << status;
          return status;
        }
        BATT_CHECK_NOT_NULLPTR(p_cqe);

        cqe = *p_cqe;

        // Consume the event so the ring buffer can move on.
        //
        DVLOG(1) << "IoRing::run() io_uring_cqe_seen";
        io_uring_cqe_seen(&this->impl_->ring_, p_cqe);
      }

      // Invoke the associated handler.  This will also decrement the activity counter.
      //
      try {
        DVLOG(1) << "IoRing::run() invoke_handler " << BATT_INSPECT(this->impl_->work_count_);
        invoke_handler(&cqe);
        DVLOG(1) << "IoRing::run() ... " << BATT_INSPECT(this->impl_->work_count_);
      } catch (...) {
        LOG(ERROR) << "Uncaught exception";
      }
    }
  }
  DVLOG(1) << "IoRing::run() " << BATT_INSPECT(this->impl_->work_count_) << " LEAVING";

  return OkStatus();
}

void IoRing::invoke_handler(struct io_uring_cqe* cqe)
{
  auto* handler = (CompletionHandler*)io_uring_cqe_get_data(cqe);
  BATT_CHECK_NOT_NULLPTR(handler);

  if (cqe->res < 0) {
    auto status = status_from_errno(-cqe->res);
    VLOG(1) << "ioring op failed: " << status;
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

  DVLOG(1) << "IoRing::on_work_finished()";

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

  return ScopedIoRing{std::move(*io), n_threads};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ScopedIoRing::ScopedIoRing(IoRing&& io, ThreadPoolSize n_threads) noexcept
    : io_{std::move(io)}
    , threads_{}
    , halted_{std::make_unique<std::atomic<bool>>(false)}
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
ScopedIoRing::~ScopedIoRing() noexcept
{
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::io_thread_main()
{
  Status status = this->io_.run();
  VLOG(1) << "ScopedIoRing::io_thread_main() exited with " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::halt()
{
  if (!this->halted_) {
    return;
  }
  const bool halted_previously = this->halted_->exchange(true);
  if (!halted_previously) {
    this->io_.on_work_finished();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::join()
{
  for (std::thread& t : this->threads_) {
    t.join();
  }
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
