//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_impl.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/logging.hpp>

#include <batteries/finally.hpp>

#include <sys/eventfd.h>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto IoRingImpl::make_new(MaxQueueDepth entries) noexcept
    -> StatusOr<std::unique_ptr<IoRingImpl>>
{
  std::unique_ptr<IoRingImpl> impl{new IoRingImpl};

  // Create the event_fd so we can wake the ioring completion event loop.
  {
    BATT_CHECK_EQ(impl->event_fd_, -1);

    impl->event_fd_ = eventfd(0, /*flags=*/EFD_SEMAPHORE);
    if (impl->event_fd_ < 0) {
      LLFS_LOG_ERROR() << "failed to create eventfd: " << std::strerror(errno);
      return batt::status_from_retval(impl->event_fd_.load());
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

  return {std::move(impl)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingImpl::is_valid() const noexcept
{
  return this->ring_init_ && this->event_fd_ >= 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingImpl::~IoRingImpl() noexcept
{
  LLFS_DVLOG(1) << "IoRingImpl::~IoRingImpl()";

  const bool prior_ring_init = this->ring_init_.exchange(false);
  LLFS_VLOG(1) << "Closing IoRing context: " << BATT_INSPECT(prior_ring_init);
  if (prior_ring_init) {
    io_uring_queue_exit(&this->ring_);
  }

  const int prior_event_fd = this->event_fd_.exchange(-1);
  LLFS_VLOG(1) << "Closing IoRing event_fd: " << BATT_INSPECT(prior_event_fd);
  if (prior_event_fd >= 0) {
    ::close(prior_event_fd);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::on_work_started() noexcept
{
  this->work_count_.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::on_work_finished() noexcept
{
  LLFS_DVLOG(1) << "IoRingImpl::on_work_finished()";

  // Submit a no-op to wake the run loop.
  //
  this->submit(
      no_buffers(),

      /*handler=*/
      [this](StatusOr<i32>) {
        this->work_count_.fetch_sub(1);
      },

      /*start_op=*/
      [](struct io_uring_sqe* sqe, auto&& /*op_handler*/) {
        io_uring_prep_nop(sqe);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::run() noexcept
{
  LLFS_DVLOG(1) << "IoRingImpl::run() ENTERED";

  auto on_scope_exit = batt::finally([&] {
    LLFS_DVLOG(1) << "IoRingImpl::run() " << BATT_INSPECT(this->work_count_) << " LEAVING";
  });

  while (this->work_count_ && !this->needs_reset_) {
    LLFS_DVLOG(1) << "IoRingImpl::run() " << BATT_INSPECT(this->work_count_);

    // Block on the event_fd until a completion event is available.
    if (this->event_wait_.exchange(true) == false) {
      eventfd_t v;
      LLFS_DVLOG(1) << "IoRingImpl::run() reading eventfd";
      int retval = eventfd_read(this->event_fd_, &v);
      if (retval != 0) {
        return status_from_retval(retval);
      }

      // If we are stopping, then write to the eventfd to wake up any other threads which might be
      // blocked inside eventfd_read.
      //
      if (this->needs_reset_) {
        eventfd_write(this->event_fd_, v);
        continue;
      }
    } else {
      std::unique_lock<std::mutex> lock{this->mutex_};
    }

    // The inner loop dequeues completion events until we would block.
    //
    for (;;) {
      struct io_uring_cqe cqe;
      {
        LLFS_DVLOG(1) << "IoRingImpl::run() locking mutex";
        std::unique_lock<std::mutex> lock{this->mutex_};

        struct io_uring_cqe* p_cqe = nullptr;

        // Dequeue a single completion event from the ring buffer.
        //
        LLFS_DVLOG(1) << "IoRingImpl::run() io_uring_peek_cqe";
        const int retval = io_uring_peek_cqe(&this->ring_, &p_cqe);
        if (retval == -EAGAIN) {
          LLFS_DVLOG(1) << "IoRingImpl::run() io_uring_peek_cqe: EAGAIN";
          break;
        }
        if (retval < 0) {
          LLFS_DVLOG(1) << "IoRingImpl::run() io_uring_peek_cqe: fail, retval=" << retval;
          //
          Status status = status_from_retval(retval);
          //
          LLFS_LOG_WARNING() << "io_uring_wait_cqe failed: " << status;
          return status;
        }
        BATT_CHECK_NOT_NULLPTR(p_cqe);

        cqe = *p_cqe;

        // Consume the event so the ring buffer can move on.
        //
        LLFS_DVLOG(1) << "IoRingImpl::run() io_uring_cqe_seen";
        //
        io_uring_cqe_seen(&this->ring_, p_cqe);
      }

      // Invoke the associated handler.  This will also decrement the activity counter.
      //
      try {
        LLFS_DVLOG(1) << "IoRingImpl::run() invoke_handler " << BATT_INSPECT(this->work_count_);
        //
        this->invoke_handler(&cqe);
        //
        LLFS_DVLOG(1) << "IoRingImpl::run() ... " << BATT_INSPECT(this->work_count_);
      } catch (...) {
        LLFS_LOG_ERROR() << "Uncaught exception";
      }
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::stop() noexcept
{
  this->needs_reset_.store(true);

  // Submit a no-op to wake the run loop.
  //
  this->submit(
      no_buffers(),
      [](StatusOr<i32>) {
      },
      [](struct io_uring_sqe* sqe, auto&&) {
        io_uring_prep_nop(sqe);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::reset() noexcept
{
  this->needs_reset_.store(false);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::invoke_handler(struct io_uring_cqe* cqe) noexcept
{
  auto* handler = (CompletionHandler*)io_uring_cqe_get_data(cqe);
  BATT_CHECK_NOT_NULLPTR(handler);

  if (cqe->res < 0) {
    Status status = status_from_errno(-cqe->res);
    LLFS_VLOG(1) << "ioring op failed: " << status;
    handler->notify(status);
  } else {
    handler->notify(cqe->res);
  }
  //
  // No need to explicitly delete/free (handler->notify will take care of that).

  this->work_count_.fetch_sub(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> IoRingImpl::register_buffers(BoxedSeq<MutableBuffer>&& buffers,
                                             bool update) noexcept
{
  std::unique_lock<std::mutex> lock{this->mutex_};

  // First unregister buffers if necessary.
  //
  if (this->buffers_registered_) {
    std::vector<struct iovec> saved_buffers = std::move(this->registered_buffers_);
    //
    Status unregistered = this->unregister_buffers_with_lock(lock);
    BATT_REQUIRE_OK(unregistered);
    //
    this->registered_buffers_ = std::move(saved_buffers);

    BATT_CHECK(!this->buffers_registered_);
  } else {
    BATT_CHECK(this->registered_buffers_.empty());
  }

  // If update is true, then we are appending the passed buffers to the existing ones; otherwise, we
  // are replacing the list.
  //
  if (!update) {
    this->registered_buffers_.clear();
  }
  const usize update_index = this->registered_buffers_.size();

  // Convert the passed sequence to struct iovec.
  //
  std::move(buffers) | seq::map([](const MutableBuffer& b) {
    struct iovec v;
    v.iov_base = b.data();
    v.iov_len = b.size();
    return v;
  }) | seq::emplace_back(&this->registered_buffers_);

  // Register the buffers!
  //
  const int retval = io_uring_register_buffers(&this->ring_, this->registered_buffers_.data(),
                                               this->registered_buffers_.size());
  if (retval < 0) {
    this->registered_buffers_.clear();
  }
  BATT_REQUIRE_OK(status_from_retval(retval));

  this->buffers_registered_ = true;

  return update_index;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::unregister_buffers() noexcept
{
  std::unique_lock<std::mutex> lock{this->mutex_};

  return this->unregister_buffers_with_lock(lock);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::unregister_buffers_with_lock(const std::unique_lock<std::mutex>&) noexcept
{
  if (this->buffers_registered_) {
    const int retval = io_uring_unregister_buffers(&this->ring_);
    BATT_REQUIRE_OK(status_from_retval(retval));

    this->buffers_registered_ = false;
    this->registered_buffers_.clear();
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> IoRingImpl::register_fd(i32 system_fd) noexcept
{
  std::unique_lock<std::mutex> lock{this->mutex_};

  i32 user_fd = -1;

  if (this->free_fds_.empty()) {
    user_fd = this->registered_fds_.size();
    this->registered_fds_.push_back(system_fd);
  } else {
    user_fd = this->free_fds_.back();
    this->free_fds_.pop_back();
    BATT_CHECK_EQ(this->registered_fds_[user_fd], -1);
    this->registered_fds_[user_fd] = system_fd;
  }

  auto revert_fd_update = batt::finally([&] {
    this->free_user_fd(user_fd);
  });

  LLFS_VLOG(1) << "IoRingImpl::register_fd() " << batt::dump_range(this->registered_fds_);

  const int retval = [&] {
    if (this->fds_registered_) {
      return io_uring_register_files_update(&this->ring_, /*off=*/0,
                                            /*files=*/this->registered_fds_.data(),
                                            /*nr_files=*/this->registered_fds_.size());
    } else {
      return io_uring_register_files(&this->ring_,
                                     /*files=*/this->registered_fds_.data(),
                                     /*nr_files=*/this->registered_fds_.size());
    }
  }();

  if (retval != 0) {
    return batt::status_from_errno(-retval);
  }

  this->fds_registered_ = true;

  revert_fd_update.cancel();

  return user_fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::unregister_fd(i32 user_fd) noexcept
{
  BATT_CHECK_GE(user_fd, 0);

  std::unique_lock<std::mutex> lock{this->mutex_};

  BATT_CHECK(this->fds_registered_);

  const i32 system_fd = this->free_user_fd(user_fd);

  const int retval = [&] {
    if (this->registered_fds_.empty()) {
      return io_uring_unregister_files(&this->ring_);
    } else {
      return io_uring_register_files_update(&this->ring_, /*off=*/0,
                                            /*files=*/this->registered_fds_.data(),
                                            /*nr_files=*/this->registered_fds_.size());
    }
  }();

  if (retval != 0) {
    // Revert the change in the tables.
    //
    while (static_cast<usize>(user_fd) >= this->registered_fds_.size()) {
      this->registered_fds_.emplace_back(-1);
    }
    this->registered_fds_[user_fd] = system_fd;
    if (!this->free_fds_.empty() && this->free_fds_.back() == user_fd) {
      this->free_fds_.pop_back();
    }

    return batt::status_from_errno(-retval);
  }

  this->fds_registered_ = !this->registered_fds_.empty();

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 IoRingImpl::free_user_fd(i32 user_fd) noexcept
{
  BATT_CHECK_GE(user_fd, 0);
  BATT_CHECK_LT(static_cast<usize>(user_fd), this->registered_fds_.size());

  const i32 system_fd = this->registered_fds_[user_fd];

  if (static_cast<usize>(user_fd) + 1 == this->registered_fds_.size()) {
    this->registered_fds_.pop_back();
  } else {
    this->registered_fds_[user_fd] = -1;
    this->free_fds_.push_back(user_fd);
  }

  return system_fd;
}

}  //namespace llfs

#endif  // LLFS_DISABLE_IO_URING
