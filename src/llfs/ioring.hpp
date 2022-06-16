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

  static StatusOr<IoRing> make_new(usize entries) noexcept;

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

    template <typename B>
    void push_buffer(const B& buf)
    {
      struct iovec& iov = this->iov_[this->iov_count_];
      this->iov_count_ += 1;
      iov.iov_base = (void*)buf.data();
      iov.iov_len = buf.size();
    }

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
//
class IoRing::File
{
 public:
  static StatusOr<File> open(IoRing& io, std::string_view const& s) noexcept;

  explicit File(IoRing& io, int fd) noexcept;

  File(const File&) = delete;
  File& operator=(const File&) = delete;

  File(File&&) noexcept;
  File& operator=(File&&) noexcept;

  ~File() noexcept;

  template <typename MutableBufferSequence, typename Handler,
            typename = std::enable_if_t<
                !std::is_same_v<std::decay_t<MutableBufferSequence>, MutableBuffer>>>
  void async_read_some(i64 offset, MutableBufferSequence&& buffers, Handler&& handler)
  {
    DVLOG(1) << "async_read_some(mulitple buffers)";
    this->io_->submit(BATT_FORWARD(buffers), BATT_FORWARD(handler),
                      [offset, this](struct io_uring_sqe* sqe, auto& op) {
                        if (this->registered_fd_ == -1) {
                          io_uring_prep_readv(sqe, this->fd_, op.iov_, op.iov_count_, offset);
                        } else {
                          io_uring_prep_readv(sqe, this->registered_fd_, op.iov_, op.iov_count_,
                                              offset);
                          sqe->flags |= IOSQE_FIXED_FILE;
                        }
                      });
  }

  template <typename Handler>
  void async_read_some(i64 offset, const MutableBuffer& buffer, Handler&& handler)
  {
    static const std::vector<MutableBuffer> empty;

    DVLOG(1) << "async_read_some(single buffer)";
    this->io_->submit(
        empty, BATT_FORWARD(handler),
        [&buffer, offset, this](struct io_uring_sqe* sqe, auto& /*op*/) {
          if (this->registered_fd_ == -1) {
            io_uring_prep_read(sqe, this->fd_, buffer.data(), buffer.size(), offset);
            DVLOG(1) << "async_read_some - NOT registered fd " << BATT_INSPECT(int(sqe->flags));
          } else {
            DVLOG(1) << "async_read_some - registered fd";
            io_uring_prep_read(sqe, this->registered_fd_, buffer.data(), buffer.size(), offset);
            sqe->flags |= IOSQE_FIXED_FILE;
          }
        });
  }

  template <typename ConstBufferSequence, typename Handler,
            typename =
                std::enable_if_t<!std::is_same_v<std::decay_t<ConstBufferSequence>, ConstBuffer> &&
                                 !std::is_same_v<std::decay_t<ConstBufferSequence>, MutableBuffer>>>
  void async_write_some(i64 offset, ConstBufferSequence&& buffers, Handler&& handler)
  {
    this->io_->submit(BATT_FORWARD(buffers), BATT_FORWARD(handler),
                      [offset, this](struct io_uring_sqe* sqe, auto& op) {
                        if (this->registered_fd_ == -1) {
                          io_uring_prep_writev(sqe, this->fd_, op.iov_, op.iov_count_, offset);
                        } else {
                          io_uring_prep_writev(sqe, this->registered_fd_, op.iov_, op.iov_count_,
                                               offset);
                          sqe->flags |= IOSQE_FIXED_FILE;
                        }
                      });
  }

  template <typename Handler>
  void async_write_some(i64 offset, const ConstBuffer& buffer, Handler&& handler)
  {
    static const std::vector<ConstBuffer> empty;

    this->io_->submit(empty, BATT_FORWARD(handler),
                      [&buffer, offset, this](struct io_uring_sqe* sqe, auto& /*op*/) {
                        if (this->registered_fd_ == -1) {
                          io_uring_prep_write(sqe, this->fd_, buffer.data(), buffer.size(), offset);
                        } else {
                          io_uring_prep_write(sqe, this->registered_fd_, buffer.data(),
                                              buffer.size(), offset);
                          sqe->flags |= IOSQE_FIXED_FILE;
                        }
                      });
  }

  template <typename Handler>
  void async_write_some_fixed(i64 offset, const ConstBuffer& buffer, int buf_index,
                              Handler&& handler)
  {
    static const std::vector<ConstBuffer> empty;

    this->io_->submit(empty, BATT_FORWARD(handler),
                      [&buffer, buf_index, offset, this](struct io_uring_sqe* sqe, auto& /*op*/) {
                        if (this->registered_fd_ == -1) {
                          io_uring_prep_write_fixed(sqe, this->fd_, buffer.data(), buffer.size(),
                                                    offset, buf_index);
                        } else {
                          io_uring_prep_write_fixed(sqe, this->registered_fd_, buffer.data(),
                                                    buffer.size(), offset, buf_index);
                          sqe->flags |= IOSQE_FIXED_FILE;
                        }
                      });
  }

  Status write_all(i64 offset, ConstBuffer buffer)
  {
    while (buffer.size() != 0) {
      BATT_CHECK_EQ(batt::round_down_bits(12, offset), offset);
      BATT_CHECK_EQ(batt::round_down_bits(12, buffer.size()), buffer.size());
      StatusOr<i32> n_written = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
        this->async_write_some(offset, buffer, BATT_FORWARD(handler));
      });
      BATT_REQUIRE_OK(n_written);
      offset += *n_written;
      buffer += *n_written;
    }
    return batt::OkStatus();
  }

  Status read_all(i64 offset, MutableBuffer buffer)
  {
    while (buffer.size() != 0) {
      DVLOG(1) << "IoRing::File::read_all about to async_read_some; " << BATT_INSPECT(offset)
               << BATT_INSPECT((void*)buffer.data()) << BATT_INSPECT(buffer.size());
      StatusOr<i32> n_read = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
        this->async_read_some(offset, buffer, BATT_FORWARD(handler));
      });
      DVLOG(1) << BATT_INSPECT(n_read);
      BATT_REQUIRE_OK(n_read);
      offset += *n_read;
      buffer += *n_read;
    }
    return batt::OkStatus();
  }

  int release()
  {
    int released = -1;
    std::swap(released, this->fd_);
    return released;
  }

  Status register_fd()
  {
    i32 fd_array[1] = {this->fd_};
    int retval = io_uring_register_files(&this->io_->impl_->ring_, /*arg=*/fd_array,
                                         /*nr_args=*/1);
    if (retval != 0) {
      return batt::status_from_errno(-retval);
    }

    this->registered_fd_ = 0;
    return batt::OkStatus();
  }

  Status close()
  {
    const int fd = this->release();
    return batt::status_from_retval(batt::syscall_retry([&] {
      return ::close(fd);
    }));
  }

  int get_fd() const
  {
    return this->fd_;
  }

 private:
  IoRing* io_;
  int fd_ = -1;
  int registered_fd_ = -1;
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

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_HPP
