//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_FILE_HPP
#define LLFS_IORING_FILE_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoRing::File
{
 public:
  using Self = File;

  // All offsets and buffers sizes must be aligned to 2^(this value).
  //
  static constexpr i32 kBlockAlignmentLog2 = 12;

  explicit File(const IoRing& io_ring, int fd) noexcept;

  File(const File&) = delete;
  File& operator=(const File&) = delete;

  File(File&&) noexcept;
  File& operator=(File&&) noexcept;

  ~File() noexcept;

  const IoRing& get_io_ring() const
  {
    return *this->io_ring_;
  }

  // Returns the OS-native file descriptor currently owned by this object.  Returns -1 if no file is
  // open.
  //
  int get_fd() const;

  // Asynchronously reads data from the file starting at the given offset, copying read data into
  // the memory pointed to by `buffers`. Invokes `handler` from within `IoRing::run()` with error
  // status or the number of bytes successfully read.
  //
  template <typename MutableBufferSequence, typename Handler = void(StatusOr<i32>),
            typename = std::enable_if_t<
                !std::is_same_v<std::decay_t<MutableBufferSequence>, MutableBuffer>>>
  void async_read_some(i64 offset, MutableBufferSequence&& buffers, Handler&& handler);

  // Asynchronously reads data from the file starting at the given offset, copying read data into
  // the memory pointed to by `buffer`. Invokes `handler` from within `IoRing::run()` with error
  // status or the number of bytes successfully read.
  //
  template <typename Handler = void(StatusOr<i32>)>
  void async_read_some(i64 offset, const MutableBuffer& buffer, Handler&& handler);

  // Asynchronously reads data from the file starting at the given offset, copying read data into
  // the memory pointed to by `buffer`. Invokes `handler` from within `IoRing::run()` with error
  // status or the number of bytes successfully read.
  //
  template <typename Handler = void(StatusOr<i32>)>
  void async_read_some_fixed(i64 offset, const MutableBuffer& buffer, int buf_index,
                             Handler&& handler);

  // Asynchronously writes the contents of `buffers` to the file starting at the given offset.
  // Invokes `handler` from within `IoRing::run()` with error status or the number of bytes
  // successfully written.
  //
  template <typename ConstBufferSequence, typename Handler = void(StatusOr<i32>),
            typename =
                std::enable_if_t<!std::is_same_v<std::decay_t<ConstBufferSequence>, ConstBuffer> &&
                                 !std::is_same_v<std::decay_t<ConstBufferSequence>, MutableBuffer>>>
  void async_write_some(i64 offset, ConstBufferSequence&& buffers, Handler&& handler);

  // Asynchronously writes the contents of `buffer` to the file starting at the given offset.
  // Invokes `handler` from within `IoRing::run()` with error status or the number of bytes
  // successfully written.
  //
  template <typename Handler = void(StatusOr<i32>)>
  void async_write_some(i64 offset, const ConstBuffer& buffer, Handler&& handler);

  // Variant of `async_write_some` that should be used in the case where the memory pointed to by
  // `buffer` has been registered with the io_uring for faster access (as opposed to mmapping the
  // buffer memory for each IOP).
  //
  template <typename Handler = void(StatusOr<i32>)>
  void async_write_some_fixed(i64 offset, const ConstBuffer& buffer, int buf_index,
                              Handler&& handler);

  // Writes the entire contents of `buffer` to the file at the given byte `offset`.  Blocking
  // call (using batt::Task::await).
  //
  Status write_all(i64 offset, ConstBuffer buffer);

  // Writes the entire contents of `buffer` to the file at the given byte `offset`.  Blocking
  // call (using batt::Task::await).
  //
  Status write_all_fixed(i64 offset, ConstBuffer buffer, int buf_index);

  // Fills `buffer` with data read from the file at the given byte `offset`.  Blocking call
  // (using batt::Task::await).
  //
  Status read_all(i64 offset, MutableBuffer buffer);

  Status read_all_fixed(i64 offset, MutableBuffer buffer, int buf_index);

  // Releases ownership of the underlying file descriptor (fd), returning the previously owned
  // value.
  //
  int release();

  // Registers data associated with this file with the kernel to speed up operations; this file MUST
  // be the only one open for the given IoRing object when using this function!  (TODO [tastolfi
  // 2022-06-23] relax this limitation by dynamically managing the list of registered fds).
  //
  Status register_fd();

  // Unregisters the file descriptor, if it was previously registered with the ioring.
  //
  Status unregister_fd();

  // Closes the file.  Releases ownership of the file descriptor (fd) and cancels any ongoing I/O.
  //
  Status close();

  /** \brief Sets whether the file is open in raw I/O mode (default=true); this enables additional
   * buffer alignment checks.
   */
  void set_raw_io(bool on) noexcept
  {
    this->raw_io_ = on;
  }

  /** \brief Returns whether the file is open in raw I/O mode.
   */
  bool is_raw_io() const noexcept
  {
    return this->raw_io_;
  }

 private:
  const IoRing* io_ring_;
  int fd_ = -1;
  int registered_fd_ = -1;
  bool raw_io_ = true;
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename MutableBufferSequence, typename Handler, typename>
inline void IoRing::File::async_read_some(i64 offset, MutableBufferSequence&& buffers,
                                          Handler&& handler)
{
  LLFS_DVLOG(1) << "async_read_some(mulitple buffers)";
  this->io_ring_->submit(BATT_FORWARD(buffers), BATT_FORWARD(handler),
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
inline void IoRing::File::async_read_some(i64 offset, const MutableBuffer& buffer,
                                          Handler&& handler)
{
  static const std::vector<MutableBuffer> empty;

  LLFS_DVLOG(1) << "async_read_some(single buffer)";
  this->io_ring_->submit(
      empty, BATT_FORWARD(handler),
      [&buffer, offset, this](struct io_uring_sqe* sqe, auto& /*op*/) {
        if (this->registered_fd_ == -1) {
          io_uring_prep_read(sqe, this->fd_, buffer.data(), buffer.size(), offset);
          LLFS_DVLOG(1) << "async_read_some - NOT registered fd " << BATT_INSPECT(int(sqe->flags));
        } else {
          LLFS_DVLOG(1) << "async_read_some - registered fd";
          io_uring_prep_read(sqe, this->registered_fd_, buffer.data(), buffer.size(), offset);
          sqe->flags |= IOSQE_FIXED_FILE;
        }
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
inline void IoRing::File::async_read_some_fixed(i64 offset, const MutableBuffer& buffer,
                                                int buf_index, Handler&& handler)
{
  static const std::vector<MutableBuffer> empty;

  LLFS_DVLOG(1) << "async_read_some(single buffer)";
  this->io_ring_->submit(
      empty, BATT_FORWARD(handler),
      [&buffer, offset, buf_index, this](struct io_uring_sqe* sqe, auto& /*op*/) {
        if (this->registered_fd_ == -1) {
          io_uring_prep_read_fixed(sqe, this->fd_, buffer.data(), buffer.size(), offset, buf_index);
          LLFS_DVLOG(1) << "async_read_some - NOT registered fd " << BATT_INSPECT(int(sqe->flags));
        } else {
          LLFS_DVLOG(1) << "async_read_some - registered fd";
          io_uring_prep_read_fixed(sqe, this->registered_fd_, buffer.data(), buffer.size(), offset,
                                   buf_index);
          sqe->flags |= IOSQE_FIXED_FILE;
        }
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ConstBufferSequence, typename Handler, typename>
inline void IoRing::File::async_write_some(i64 offset, ConstBufferSequence&& buffers,
                                           Handler&& handler)
{
  this->io_ring_->submit(BATT_FORWARD(buffers), BATT_FORWARD(handler),
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
inline void IoRing::File::async_write_some(i64 offset, const ConstBuffer& buffer, Handler&& handler)
{
  static const std::vector<ConstBuffer> empty;

  this->io_ring_->submit(
      empty, BATT_FORWARD(handler),
      [&buffer, offset, this](struct io_uring_sqe* sqe, auto& /*op*/) {
        if (this->registered_fd_ == -1) {
          io_uring_prep_write(sqe, this->fd_, buffer.data(), buffer.size(), offset);
        } else {
          io_uring_prep_write(sqe, this->registered_fd_, buffer.data(), buffer.size(), offset);
          sqe->flags |= IOSQE_FIXED_FILE;
        }
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
inline void IoRing::File::async_write_some_fixed(i64 offset, const ConstBuffer& buffer,
                                                 int buf_index, Handler&& handler)
{
  static const std::vector<ConstBuffer> empty;

  this->io_ring_->submit(
      empty, BATT_FORWARD(handler),
      [&buffer, buf_index, offset, this](struct io_uring_sqe* sqe, auto& /*op*/) {
        if (this->registered_fd_ == -1) {
          io_uring_prep_write_fixed(sqe, this->fd_, buffer.data(), buffer.size(), offset,
                                    buf_index);
        } else {
          io_uring_prep_write_fixed(sqe, this->registered_fd_, buffer.data(), buffer.size(), offset,
                                    buf_index);
          sqe->flags |= IOSQE_FIXED_FILE;
        }
      });
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_FILE_HPP
