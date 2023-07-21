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
#include <llfs/seq.hpp>
#include <llfs/status.hpp>

#include <boost/beast/core/buffers_range.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/handler.hpp>
#include <batteries/static_assert.hpp>

#include <liburing.h>

#include <iterator>
#include <memory>
#include <mutex>

namespace llfs {

class IoRingImpl
{
 public:
  using CompletionHandler = batt::AbstractHandler<StatusOr<i32>>;

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

  Status register_buffers(BoxedSeq<MutableBuffer>&& buffers) noexcept;

  Status unregister_buffers() noexcept;

  StatusOr<i32> register_fd(i32 system_fd) noexcept;

  Status unregister_fd(i32 user_fd) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Wraps the passed handler `fn` as a batt::HandlerImpl (batt::AbstractHandler) which
   * can be attached as user data to an I/O request.
   */
  template <typename Fn, typename BufferSequence>
  static batt::HandlerImpl</*HandlerFn=*/IoRingOpHandler<std::decay_t<Fn>>,
                           /*Args...=*/StatusOr<i32>>*
  wrap_handler(Fn&& fn, BufferSequence&& bufs);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Use static make_new to create instances of this class.
  //
  IoRingImpl() = default;

  void invoke_handler(struct io_uring_cqe* cqe) noexcept;

  Status unregister_buffers_with_lock(const std::unique_lock<std::mutex>&) noexcept;

  // Removes the specified user_fd from the registered fds table, returning the previously
  // registered system fd that was in that slot.
  //
  i32 free_user_fd(i32 user_fd) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Protects all other non-atomic data members of this class.
  //
  std::mutex mutex_;

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

  // Set to true when work count goes to zero and run() exits; it must be set to true via a call to
  // this->reset() before new I/O events can be processed.
  //
  std::atomic<bool> needs_reset_{false};

  // The event_fd registered with the io_uring context, used to receive notification of completions
  // or other interrupts within the event processing loop.
  //
  std::atomic<int> event_fd_{-1};

  // The set of fds registered with the io_uring context.
  //
  std::vector<i32> registered_fds_;

  // TODO [tastolfi 2023-07-20] What was the idea behind this again?
  //
  std::vector<i32> free_fds_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Fn, typename BufferSequence>
/*static*/ inline auto IoRingImpl::wrap_handler(Fn&& fn, BufferSequence&& bufs)
    -> batt::HandlerImpl</*HandlerFn=*/IoRingOpHandler<std::decay_t<Fn>>,
                         /*Args...=*/StatusOr<i32>>*
{
  auto buf_seq = boost::beast::buffers_range_ref(bufs);
  const usize buf_count = std::distance(std::begin(buf_seq), std::end(buf_seq));
  const usize extra_bytes = buf_count * sizeof(struct iovec);

  auto* op = batt::HandlerImpl</*HandlerFn=*/IoRingOpHandler<std::decay_t<Fn>>,
                               /*Args...=*/StatusOr<i32>>::make_new(BATT_FORWARD(fn), extra_bytes);
  BATT_CHECK_NOT_NULLPTR(op);

  for (const auto& buf : buf_seq) {
    op->get_fn().push_buffer(buf);
  }

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
  std::unique_lock<std::mutex> lock{this->mutex_};

  struct io_uring_sqe* sqe = io_uring_get_sqe(&this->ring_);
  BATT_CHECK_NOT_NULLPTR(sqe);

  auto* op_handler = wrap_handler(BATT_FORWARD(handler), BATT_FORWARD(buffers));

  BATT_STATIC_ASSERT_TYPE_EQ(decltype(op_handler->get_fn()),
                             IoRingOpHandler<std::decay_t<Handler>>&);

  // Initiate the operation.
  //
  start_op(sqe, op_handler->get_fn());

  // Set user data.
  //
  io_uring_sqe_set_data(sqe, op_handler);

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
