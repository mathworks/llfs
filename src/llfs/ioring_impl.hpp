//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_IMPL_HPP
#define LLFS_IORING_IMPL_HPP

#include <llfs/config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/api_types.hpp>
#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_op_handler.hpp>
#include <llfs/optional.hpp>
#include <llfs/seq.hpp>
#include <llfs/status.hpp>

#include <boost/beast/core/buffers_range.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/handler.hpp>
#include <batteries/static_assert.hpp>

#include <liburing.h>

#include <condition_variable>
#include <iterator>
#include <memory>
#include <mutex>

namespace llfs {
class IoRingImpl
{
 public:
  struct CompletionHandlerBase : batt::DefaultHandlerBase {
    Optional<StatusOr<i32>> result;
  };

  using CompletionHandler = batt::BasicAbstractHandler<CompletionHandlerBase, StatusOr<i32>>;

  template <typename Fn>
  using CompletionHandlerImpl =
      batt::BasicHandlerImpl</* HandlerFn= */ IoRingOpHandler<std::decay_t<Fn>>,
                             /*      Base= */ CompletionHandlerBase,
                             /*   Args...= */ StatusOr<i32>>;

  using CompletionHandlerList = batt::BasicHandlerList<CompletionHandlerBase, StatusOr<i32>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<std::unique_ptr<IoRingImpl>> make_new(MaxQueueDepth entries) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // IoRingImpl is noncopyable.
  //
  IoRingImpl(const IoRingImpl&) = delete;
  IoRingImpl& operator=(const IoRingImpl&) = delete;

  // Closes down the io_uring context and frees all resources.
  //
  ~IoRingImpl() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool is_valid() const noexcept;

  void on_work_started() noexcept;

  void on_work_finished() noexcept;

  Status run() noexcept;

  void stop() noexcept;

  void reset() noexcept;

  template <typename Handler>
  void post(Handler&& handler) noexcept;

  template <typename Handler, typename BufferSequence>
  void submit(BufferSequence&& buffers, Handler&& handler,
              std::function<void(struct io_uring_sqe*, IoRingOpHandler<std::decay_t<Handler>>&)>&&
                  start_op) noexcept;

  /** \brief Register buffers for faster I/O.
   *
   * If `update` is true, then returns the index of the first buffer in the new set, which is
   * appended to the old set.
   */
  StatusOr<usize> register_buffers(BoxedSeq<MutableBuffer>&& buffers, bool update) noexcept;

  Status unregister_buffers() noexcept;

  StatusOr<i32> register_fd(i32 system_fd) noexcept;

  Status unregister_fd(i32 user_fd) noexcept;

  bool can_run() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Wraps the passed handler `fn` as a batt::HandlerImpl (batt::AbstractHandler) which
   * can be attached as user data to an I/O request.
   */
  template <typename Fn, typename BufferSequence>
  static CompletionHandlerImpl<Fn>* wrap_handler(Fn&& fn, BufferSequence&& bufs);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Use static make_new to create instances of this class.
  //
  IoRingImpl() = default;

  Status unregister_buffers_with_lock(const std::unique_lock<std::mutex>&) noexcept;

  // Removes the specified user_fd from the registered fds table, returning the previously
  // registered system fd that was in that slot.
  //
  i32 free_user_fd(i32 user_fd) noexcept;

  /** \brief Blocks the caller until the event_fd_ is signalled.
   *
   * Should be called without holding any locks.
   */
  Status wait_for_ring_event();

  /** \brief Blocks the caller until one of the following is true:
   *
   *  - There is at least one completion in the queue (this->completions_.empty() == false)
   *  - No other thread is waiting on the ioring event (this->event_wait_ == false)
   *  - There is no work pending (this->work_count_ == 0)
   *  - stop() has been called (this->needs_reset_ == true)
   */
  StatusOr<CompletionHandler*> wait_for_completions();

  /** \brief Transfer up to `std::thread::hardware_concurrency()` completion events from the
   * ioring completion queue to our local queue (this->completions_).
   *
   * If successful, this function will wake up one or more other threads to process the
   * completions.
   */
  StatusOr<usize> transfer_completions(CompletionHandler** handler_out);

  /** \brief Converts the passed cqe to a CompletionHandler, initializing the `result` field.
   */
  CompletionHandler* completion_from_cqe(struct io_uring_cqe* cqe);

  /** \brief Pushes *handler onto the back of `this->completions_`, waking one waiter.
   *
   * Sets *handler to nullptr.
   */
  void push_completion(CompletionHandler** handler);

  /** \brief Pushes the handler onto the front of `this->completions_`; do not call notify.
   *
   * Sets *handler to nullptr.
   */
  void stash_completion(CompletionHandler** handler);

  /** \brief Tries to pop a single completed handler from the queue.
   *
   * This function never blocks; if the completion queue is empty, it just returns nullptr
   * immediately.
   *
   * \return The popped completion if there is one, nullptr otherwise.
   */
  CompletionHandler* pop_completion();

  /** \brief Same as pop_completion(), but with the queue_mutex_ lock already held.
   */
  CompletionHandler* pop_completion_with_lock(std::unique_lock<std::mutex>&);

  /** \brief Invokes and deletes the handler, decrementing work count.
   *
   * Will panic if handler->result has not been initialized.
   */
  void invoke_handler(CompletionHandler** handler) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Protects access to the io_uring context and associated data (registered_fds_, free_fds_,
  // registered_buffers_).
  //
  std::mutex ring_mutex_;

  // Protects access to the completions_ queue.
  //
  std::mutex queue_mutex_;

  // Used to signal that there are completions to process.
  //
  std::condition_variable state_change_;

  // List of handlers for completed operations.
  //
  CompletionHandlerList completions_;

  // The first thread to enter the critical section will set this flag.
  //
  std::atomic<bool> event_wait_{false};

  // The io_uring context.
  //
  struct io_uring ring_;

  // When true, indicates that `this->ring_` has been initialized.
  //
  std::atomic<bool> ring_init_{false};

  // When true, indicates that buffers have been registered with the io_uring context.
  //
  bool buffers_registered_{false};

  // When true, indicates that fds have been registered with the io_using_context.
  //
  bool fds_registered_{false};

  // The number of expected future calls to on_work_completed; when this count goes to zero, any
  // calls to this->run() will return.
  //
  std::atomic<isize> work_count_{0};

  // Set to true when work count goes to zero and run() exits; it must be set to true via a call
  // to this->reset() before new I/O events can be processed.
  //
  std::atomic<bool> needs_reset_{false};

  // The event_fd registered with the io_uring context, used to receive notification of
  // completions or other interrupts within the event processing loop.
  //
  std::atomic<int> event_fd_{-1};

  // The set of fds registered with the io_uring context.
  //
  std::vector<i32> registered_fds_;

  // TODO [tastolfi 2023-07-20] What was the idea behind this again?
  //
  std::vector<i32> free_fds_;

  // The set of registered buffers.
  //
  std::vector<struct iovec> registered_buffers_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Fn, typename BufferSequence>
/*static*/ inline auto IoRingImpl::wrap_handler(Fn&& fn, BufferSequence&& bufs)
    -> CompletionHandlerImpl<Fn>*
{
  auto buf_seq = boost::beast::buffers_range_ref(bufs);
  const usize buf_count = std::distance(std::begin(buf_seq), std::end(buf_seq));
  const usize extra_bytes = buf_count * sizeof(struct iovec);

  auto* op = CompletionHandlerImpl<Fn>::make_new(BATT_FORWARD(fn), extra_bytes);
  BATT_CHECK_NOT_NULLPTR(op);

  for (const auto& buf : buf_seq) {
    op->get_fn().push_buffer(buf);
  }

  static_assert(std::is_convertible_v<decltype(op), CompletionHandler*>);

  return op;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
inline void IoRingImpl::post(Handler&& handler) noexcept
{
  // Submit a no-op to wake the run loop.
  //
  this->submit(no_buffers(), BATT_FORWARD(handler), [](struct io_uring_sqe* sqe, auto&&) {
    io_uring_prep_nop(sqe);
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler, typename BufferSequence>
inline void IoRingImpl::submit(
    BufferSequence&& buffers, Handler&& handler,
    std::function<void(struct io_uring_sqe*, IoRingOpHandler<std::decay_t<Handler>>&)>&&
        start_op) noexcept

{
  CompletionHandlerImpl<Handler>* op_handler =
      wrap_handler(BATT_FORWARD(handler), BATT_FORWARD(buffers));

  std::unique_lock<std::mutex> lock{this->ring_mutex_};

  struct io_uring_sqe* sqe = io_uring_get_sqe(&this->ring_);
  BATT_CHECK_NOT_NULLPTR(sqe);

  BATT_STATIC_ASSERT_TYPE_EQ(decltype(op_handler->get_fn()),
                             IoRingOpHandler<std::decay_t<Handler>>&);

  // Initiate the operation.
  //
  start_op(sqe, op_handler->get_fn());

  // Set user data.
  //
  io_uring_sqe_set_data(sqe, static_cast<CompletionHandler*>(op_handler));

  // Increment work count; decrement in invoke_handler.
  //
  LLFS_DVLOG(1) << "(submit) before; " << BATT_INSPECT(this->work_count_);
  this->on_work_started();
  LLFS_DVLOG(1) << "(submit) after; " << BATT_INSPECT(this->work_count_);

  // Finally, submit the request.
  //
  BATT_CHECK_EQ(1, io_uring_submit(&this->ring_)) << std::strerror(errno);
}

}  //namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_IORING_IMPL_HPP
