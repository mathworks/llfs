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
#include <llfs/track_fds.hpp>

#include <batteries/finally.hpp>

#include <sys/eventfd.h>

namespace llfs {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status status_from_uring_retval(int retval)
{
  if (retval != 0) {
    BATT_CHECK_LT(retval, 0);
    return batt::status_from_errno(-retval);
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> status_or_i32_from_uring_retval(int retval)
{
  if (retval < 0) {
    return batt::status_from_errno(-retval);
  }
  return {retval};
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto IoRingImpl::make_new(MaxQueueDepth entries) noexcept
    -> StatusOr<std::unique_ptr<IoRingImpl>>
{
  LLFS_VLOG(1) << "Creating new IoRingImpl";
  std::unique_ptr<IoRingImpl> impl{new IoRingImpl};

  // Create the event_fd so we can wake the ioring completion event loop.
  {
    BATT_CHECK_EQ(impl->event_fd_, -1);

    LLFS_VLOG(1) << "Creating eventfd";
    impl->event_fd_.store(maybe_track_fd(eventfd(0, /*flags=*/EFD_SEMAPHORE)));
    if (impl->event_fd_ < 0) {
      LLFS_LOG_ERROR() << "failed to create eventfd: " << std::strerror(errno);
      return batt::status_from_retval(impl->event_fd_.load());
    }

    BATT_CHECK_NE(impl->event_fd_, -1);
  }

  // Initialize the io_uring.
  {
    BATT_CHECK(!impl->ring_init_);

    LLFS_VLOG(1) << "Calling io_uring_queue_init(entries=" << entries << ")";
    const int retval = io_uring_queue_init(entries, &impl->ring_, /*flags=*/0);

    BATT_REQUIRE_OK(status_from_uring_retval(retval))
        << batt::LogLevel::kError << "failed io_uring_queue_init: " << std::strerror(-retval);

    impl->ring_init_ = true;
  }

  // Register the event_fd with the io_uring.
  {
    BATT_CHECK_NE(impl->event_fd_, -1);

    LLFS_VLOG(1) << "Calling io_uring_register_eventfd";
    const int retval = io_uring_register_eventfd(&impl->ring_, impl->event_fd_);

    BATT_REQUIRE_OK(status_from_uring_retval(retval))
        << batt::LogLevel::kError << "failed io_uring_register_eventfd: " << std::strerror(-retval);
  }

  LLFS_VLOG(1) << "IoRingImpl created";

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

  batt::invoke_all_handlers(&this->completions_,  //
                            StatusOr<i32>{::llfs::make_status(StatusCode::kIoRingShutDown)});
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

  const isize prior_count = this->work_count_.fetch_sub(1);
  if (prior_count == 1) {
    this->wake_all();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingImpl::can_run() const noexcept
{
  return this->work_count_ && !this->needs_reset_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::run() noexcept
{
  LLFS_DVLOG(1) << "IoRingImpl::run() ENTERED";

  // This is just an arbitrary limit; how many times can we iterate without successfully
  // grabbing/running a completion handler?
  //
  constexpr i64 kMaxNoopCount = 1000 * 1000 * 1000;

  auto on_scope_exit = batt::finally([this] {
    LLFS_DVLOG(1) << "IoRingImpl::run() " << BATT_INSPECT(this->work_count_) << " LEAVING";
  });

  CompletionHandler* handler = nullptr;
  i64 noop_count = 0;

  while (this->can_run()) {
    LLFS_DVLOG(1) << "IoRingImpl::run() top of loop;" << BATT_INSPECT(this->work_count_);

    // Highest priority is to invoke all completion handlers.
    //
    if (handler != nullptr) {
      noop_count = 0;
      this->invoke_handler(&handler);
      continue;
    }

    // The goal of the rest of the loop is to grab a completion handler to run.  First try the
    // direct approach...
    //
    handler = this->pop_completion();
    if (handler != nullptr) {
      noop_count = 0;
      continue;
    }

    // Once we run out of handlers, transfer more completed events from the queue.
    //  Only one thread is allowed to do the ring event wait; the rest will fall through to the
    //  `else` block below, waiting for condition_variable notification that more completions are
    //  available (or we're out of work, etc.)
    //
    if (this->event_wait_.exchange(true) == false) {
      auto on_scope_exit3 = batt::finally([this, &handler] {
        this->event_wait_.store(false);

        // If we have a handler to run, then the current thread will get back to the event_wait_
        // step (unless this->can_run() becomes false, which should have sent a notification to all
        // waiters), therefore we are not in danger of having waiting threads with no thread inside
        // eventfd_read, UNLESS handler is nullptr.  So in that case, wake up a waiting thread.
        //
        if (handler == nullptr) {
          {
            // See comment in IoRingImpl::wake_all() for an explanation of why this seemingly
            // unproductive statement is necessary.
            //
            std::unique_lock<std::mutex> queue_lock{this->queue_mutex_};
          }
          this->state_change_.notify_one();
        }
      });

      // Try to transfer at least one completion without waiting on the event_fd; if this fails
      // (returned count is 0), then we block waiting to be notified of new completion events.
      //
      BATT_ASSIGN_OK_RESULT(usize transfer_count, this->transfer_completions(&handler));
      if (transfer_count == 0) {
        BATT_REQUIRE_OK(this->wait_for_ring_event());
        BATT_REQUIRE_OK(this->transfer_completions(&handler));
      }

    } else {
      // Some other thread won the race to enter `event_wait_`; wait on `this->state_change_`.
      //
      BATT_ASSIGN_OK_RESULT(handler, this->wait_for_completions());
    }

    // Some spurious wake-ups are fine, but only up to a point; if we execute the loop enough times
    // without seeing anything interesting happen, panic.
    //
    if (handler) {
      noop_count = 0;
    } else {
      std::this_thread::yield();
      ++noop_count;

      if (noop_count > kMaxNoopCount) {
        BATT_CHECK(!this->can_run())
            << "Possible infinite loop detected!" << BATT_INSPECT(this->work_count_)
            << BATT_INSPECT(this->needs_reset_) << BATT_INSPECT(noop_count);
      }
    }
  }

  // If we are returning with a ready-to-run handler, stash it for later.
  //
  if (handler) {
    this->stash_completion(&handler);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::wait_for_ring_event()
{
  LLFS_DVLOG(1) << "IoRingImpl::wait_for_ring_event()";

  // Block on the event_fd until a completion event is available.
  //
  eventfd_t v;
  int retval = eventfd_read(this->event_fd_, &v);
  if (retval != 0) {
    return batt::status_from_retval(retval);
  }

  // If we are stopping, either because this->stop() was called or because the work count has gone
  // to zero, then write to the eventfd to wake up any other threads which might be blocked inside
  // eventfd_read.  (This propagation of the event essentially turns the call to eventfd_write in
  // IoRingImpl::wake_all() into a broadcast-to-all-threads.)
  //
  if (!this->can_run()) {
    eventfd_write(this->event_fd_, v);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingImpl::wait_for_completions() -> StatusOr<CompletionHandler*>
{
  const auto is_unblocked = [this] {       // We aren't blocked if any of:
    return !this->can_run()                //  - the run loop has been stopped
           || !this->completions_.empty()  //  - there are completions to execute
           || !this->event_wait_.load()    //  - no other thread is waiting for events
        ;
  };

  //----- --- -- -  -  -   -

  std::unique_lock<std::mutex> queue_lock{this->queue_mutex_};

  while (!is_unblocked()) {
    this->state_change_.wait(queue_lock);
  }

  return {this->pop_completion_with_lock(queue_lock)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::push_completion(CompletionHandler** handler)
{
  BATT_ASSERT_NOT_NULLPTR(handler);

  auto on_scope_exit = batt::finally([&] {
    *handler = nullptr;
  });
  {
    std::unique_lock<std::mutex> queue_lock{this->queue_mutex_};
    this->completions_.push_back(**handler);
  }
  this->state_change_.notify_one();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::stash_completion(CompletionHandler** handler)
{
  BATT_ASSERT_NOT_NULLPTR(handler);

  auto on_scope_exit = batt::finally([&] {
    *handler = nullptr;
  });
  {
    std::unique_lock<std::mutex> queue_lock{this->queue_mutex_};
    this->completions_.push_front(**handler);
  }

  // We do not notify state_change_ here because this function is only called when this->can_run()
  // is false.
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingImpl::completion_from_cqe(struct io_uring_cqe* cqe) -> CompletionHandler*
{
  auto* handler = (CompletionHandler*)io_uring_cqe_get_data(cqe);
  BATT_CHECK_NOT_NULLPTR(handler);

  if (cqe->res < 0) {
    handler->result.emplace(status_from_uring_retval(cqe->res));
  } else {
    handler->result.emplace(cqe->res);
  }

  return handler;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingImpl::pop_completion() -> CompletionHandler*
{
  std::unique_lock<std::mutex> queue_lock{this->queue_mutex_};

  return this->pop_completion_with_lock(queue_lock);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingImpl::pop_completion_with_lock(std::unique_lock<std::mutex>&) -> CompletionHandler*
{
  if (this->completions_.empty()) {
    return nullptr;
  }

  CompletionHandler& next = this->completions_.front();
  this->completions_.pop_front();
  return &next;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> IoRingImpl::transfer_completions(CompletionHandler** handler_out)
{
  static const usize kMaxCount = std::thread::hardware_concurrency();
  //
  // The rationale for this limit is that we should switch to running completion handlers once we
  // can reasonably assume that all CPUs will have something to do.

  BATT_ASSERT_NOT_NULLPTR(handler_out);

  usize count = 0;
  *handler_out = nullptr;

  std::unique_lock<std::mutex> ring_lock{this->ring_mutex_};
  for (; count < kMaxCount; ++count) {
    struct io_uring_cqe cqe;
    struct io_uring_cqe* p_cqe = nullptr;

    // Dequeue a single completion event from the ring buffer.
    //
    LLFS_DVLOG(1) << "IoRingImpl::run() io_uring_peek_cqe";
    const int retval = io_uring_peek_cqe(&this->ring_, &p_cqe);
    if (retval == -EAGAIN) {
      //
      // EAGAIN means there are no completions in the ring; we are done!

      LLFS_DVLOG(1) << "IoRingImpl::run() io_uring_peek_cqe: EAGAIN";
      break;
    }

    BATT_REQUIRE_OK(status_from_uring_retval(retval))
        << batt::LogLevel::kError << "IoRingImpl::run() io_uring_peek_cqe: fail, retval=" << retval
        << ";" << BATT_INSPECT(EAGAIN);

    BATT_CHECK_NOT_NULLPTR(p_cqe);

    // Copy the completion event data to the stack so we can signal the ring ASAP
    // (io_uring_cqe_seen).
    //
    cqe = *p_cqe;

    // Consume the event so the ring buffer can move on.
    //
    LLFS_DVLOG(1) << "IoRingImpl::run() io_uring_cqe_seen";
    //
    io_uring_cqe_seen(&this->ring_, p_cqe);

    // Save the op result in the handler and push the previous handler to the completion queue.
    //
    if (*handler_out != nullptr) {
      this->push_completion(handler_out);
    }
    *handler_out = this->completion_from_cqe(&cqe);
  }

  return {count};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::stop() noexcept
{
  this->needs_reset_.store(true);
  this->wake_all();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::reset() noexcept
{
  this->needs_reset_.store(false);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::invoke_handler(CompletionHandler** handler) noexcept
{
  auto on_scope_exit = batt::finally([handler, this] {
    *handler = nullptr;
    this->on_work_finished();
  });

  BATT_CHECK((*handler)->result);

  StatusOr<i32> result = *(*handler)->result;
  try {
    LLFS_DVLOG(1) << "IoRingImpl::run() invoke_handler " << BATT_INSPECT(this->work_count_);
    //
    (*handler)->notify(result);
    //
    LLFS_DVLOG(1) << "IoRingImpl::run() ... " << BATT_INSPECT(this->work_count_);

  } catch (...) {
    LLFS_LOG_ERROR() << "Uncaught exception";
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> IoRingImpl::register_buffers(BoxedSeq<MutableBuffer>&& buffers,
                                             bool update) noexcept
{
  std::unique_lock<std::mutex> lock{this->ring_mutex_};

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

  // If update is true, then we are appending the passed buffers to the existing ones; otherwise,
  // we are replacing the list.
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

  LLFS_VLOG(1) << BATT_INSPECT(this->registered_buffers_.size());

  if (this->registered_buffers_.empty()) {
    LLFS_LOG_ERROR() << "Attempted to register a list of 0 buffers! (Must be at least 1)";
  }

  // Register the buffers!
  //
  const int retval = io_uring_register_buffers(&this->ring_, this->registered_buffers_.data(),
                                               this->registered_buffers_.size());
  if (retval < 0) {
    this->registered_buffers_.clear();
  }
  BATT_REQUIRE_OK(status_from_uring_retval(retval));

  this->buffers_registered_ = true;

  return update_index;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::unregister_buffers() noexcept
{
  std::unique_lock<std::mutex> lock{this->ring_mutex_};

  return this->unregister_buffers_with_lock(lock);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::unregister_buffers_with_lock(const std::unique_lock<std::mutex>&) noexcept
{
  if (this->buffers_registered_) {
    const int retval = io_uring_unregister_buffers(&this->ring_);
    BATT_REQUIRE_OK(status_from_uring_retval(retval));

    this->buffers_registered_ = false;
    this->registered_buffers_.clear();
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> IoRingImpl::register_fd(i32 system_fd) noexcept
{
  std::unique_lock<std::mutex> lock{this->ring_mutex_};

  i32 user_fd = -1;

  if (this->free_fds_.empty()) {
    user_fd = this->registered_fds_.size();

    // If the registered files (fds) buffer will need to be reallocated, then first unregister it.
    //
    BATT_REQUIRE_OK(this->unregister_files_with_lock(lock));

    this->registered_fds_.push_back(system_fd);
    while (this->registered_fds_.size() < this->registered_fds_.capacity()) {
      this->free_fds_.push_back(this->registered_fds_.size());
      this->registered_fds_.push_back(-1);
    }

  } else {
    user_fd = this->free_fds_.back();
    this->free_fds_.pop_back();

    BATT_CHECK_EQ(this->registered_fds_[user_fd], -1);

    this->registered_fds_[user_fd] = system_fd;
  }

  auto revert_fd_alloc = batt::finally([&] {
    this->registered_fds_[user_fd] = -1;
    this->free_fds_.push_back(user_fd);
  });

  LLFS_VLOG(1) << "IoRingImpl::register_fd() " << batt::dump_range(this->registered_fds_)
               << BATT_INSPECT(user_fd) << "; update=" << (this->fds_registered_);

  if (this->fds_registered_) {
    BATT_ASSIGN_OK_RESULT(i32 n_files_updated,
                          status_or_i32_from_uring_retval(io_uring_register_files_update(
                              &this->ring_, /*off=*/user_fd, /*files=*/&system_fd,
                              /*nr_files=*/1)));

    BATT_CHECK_EQ(n_files_updated, 1);

  } else {
    BATT_REQUIRE_OK(status_from_uring_retval(
        io_uring_register_files(&this->ring_,
                                /*files=*/this->registered_fds_.data(),
                                /*nr_files=*/this->registered_fds_.size())));

    this->fds_registered_ = true;
  }

  revert_fd_alloc.cancel();

  return user_fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::unregister_fd(i32 user_fd) noexcept
{
  BATT_CHECK_GE(user_fd, 0);
  BATT_CHECK_LT(static_cast<usize>(user_fd), this->registered_fds_.size());

  std::unique_lock<std::mutex> lock{this->ring_mutex_};

  BATT_CHECK(this->fds_registered_);

  const i32 system_fd = this->registered_fds_[user_fd];
  if (system_fd == -1) {
    return OkStatus();
  }

  i32 invalid_fd = -1;

  BATT_ASSIGN_OK_RESULT(
      i32 n_files_updated,
      status_or_i32_from_uring_retval(io_uring_register_files_update(&this->ring_, /*off=*/user_fd,
                                                                     /*files=*/&invalid_fd,  //
                                                                     /*nr_files=*/1)));

  BATT_CHECK_EQ(n_files_updated, 1);

  this->registered_fds_[user_fd] = -1;
  this->free_fds_.push_back(user_fd);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingImpl::unregister_files_with_lock(const std::unique_lock<std::mutex>&) noexcept
{
  if (this->fds_registered_) {
    BATT_REQUIRE_OK(status_from_uring_retval(io_uring_unregister_files(&this->ring_)));
    this->fds_registered_ = false;
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingImpl::wake_all() noexcept
{
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // We must create a barrier so that the condition variable wait in wait_for_completions() will
  // always see either the changes on the current thread made prior to calling wake_all(), _or_ the
  // call to notify_all().
  //
  // wait_for_completions() is called by all threads who lose the race to call eventfd_read to wait
  // directly on the io_uring event queue.  Its primary mechanism of operation is a condition
  // variable wait:
  //
  //   std::unique_lock<std::mutex> queue_lock{this->queue_mutex_};
  //
  //   while (!is_unblocked()) {
  //     this->state_change_.wait(queue_lock);
  //   }
  //
  // `is_unblocked()` may become true if, for example, the work counter goes from 1 to 0.  In this
  // case, without the queue_lock barrier, the following interleaving of events would be possible:
  //
  //  1. [Thread A] A1: Lock queue_mutex_
  //  2. [Thread A] A2: Read work counter, observe non-zero (implies may be blocked)
  //  3. [Thread B] B1: Decrement work counter, 1 -> 0
  //  4. [Thread B] B2: Notify state_change_ condition variable
  //  5. [Thread A] A3: Condition wait on state_change_ (atomic unlock-mutex-and-wait-for-notify)
  //
  // Since the call to notify_all (B2) happens strictly before the condition wait (A3), Thread A
  // will wait indefinitely (BUG).
  //
  // With the queue_lock barrier in place (B1.5, between B1 and B2), this becomes impossible since
  // A1..A3 form a critical section, which by definition must be serialized with all other critical
  // sections (for this->queue_mutex_).  In other words, either B1.5 happens-before A1 or B1.5
  // happens-after A3:
  //
  //   - If B1.5 happens-before A1, then B1 also happens-before A2, which means A2 can't observe the
  //     pre-B1 value of the work counter.
  //   - If B1.5 happens-after A3, then B2 also happens-after A3, which means B2 will interrupt the
  //     condition wait.
  //
  {
    std::unique_lock<std::mutex> queue_lock{this->queue_mutex_};
  }
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Wake any threads inside this->wait_for_completions().
  //
  this->state_change_.notify_all();

  // Wake any thread inside this->wait_for_ring_event().
  //
  eventfd_write(this->event_fd_, 1);
}

}  //namespace llfs

#endif  // LLFS_DISABLE_IO_URING
