//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_file.hpp>
//

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <batteries/syscall_retry.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::File::File(const IoRing& io_ring, int fd) noexcept
    : io_ring_impl_{io_ring.impl_.get()}
    , fd_{fd}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::File::File(File&& that) noexcept : io_ring_impl_{that.io_ring_impl_}, fd_{that.fd_}
{
  that.fd_ = -1;
  that.registered_fd_ = -1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRing::File::operator=(File&& that) noexcept -> File&
{
  File copy{std::move(that)};

  std::swap(this->io_ring_impl_, copy.io_ring_impl_);
  std::swap(this->fd_, copy.fd_);
  std::swap(this->registered_fd_, copy.registered_fd_);

  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::File::~File() noexcept
{
  this->close().IgnoreError();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::write_all(i64 offset, ConstBuffer buffer)
{
  while (buffer.size() != 0) {
    if (this->raw_io_) {
      BATT_CHECK_EQ(batt::round_down_bits(Self::kBlockAlignmentLog2, offset), offset);
      BATT_CHECK_EQ(batt::round_down_bits(Self::kBlockAlignmentLog2, buffer.size()), buffer.size());
    }
    StatusOr<i32> n_written = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
      this->async_write_some(offset, buffer, BATT_FORWARD(handler));
    });
    BATT_REQUIRE_OK(n_written);
    offset += *n_written;
    buffer += *n_written;
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::write_all_fixed(i64 offset, ConstBuffer buffer, int buf_index)
{
  while (buffer.size() != 0) {
    if (this->raw_io_) {
      BATT_CHECK_EQ(batt::round_down_bits(Self::kBlockAlignmentLog2, offset), offset);
      BATT_CHECK_EQ(batt::round_down_bits(Self::kBlockAlignmentLog2, buffer.size()), buffer.size());
    }
    StatusOr<i32> n_written = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
      this->async_write_some_fixed(offset, buffer, buf_index, BATT_FORWARD(handler));
    });
    BATT_REQUIRE_OK(n_written);
    offset += *n_written;
    buffer += *n_written;
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::read_all(i64 offset, MutableBuffer buffer)
{
  while (buffer.size() != 0) {
    LLFS_DVLOG(1) << "IoRing::File::read_all about to async_read_some; " << BATT_INSPECT(offset)
                  << BATT_INSPECT((void*)buffer.data()) << BATT_INSPECT(buffer.size());
    StatusOr<i32> n_read = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
      this->async_read_some(offset, buffer, BATT_FORWARD(handler));
    });
    LLFS_DVLOG(1) << BATT_INSPECT(n_read);
    BATT_REQUIRE_OK(n_read);

    if (*n_read == 0) {
      LLFS_DVLOG(1) << "read_all read 0 Bytes. Returning OutOfRange Error.";
      return batt::StatusCode::kOutOfRange;
    }

    offset += *n_read;
    buffer += *n_read;
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::read_all_fixed(i64 offset, MutableBuffer buffer, int buf_index)
{
  while (buffer.size() != 0) {
    LLFS_DVLOG(1) << "IoRing::File::read_all about to async_read_some; " << BATT_INSPECT(offset)
                  << BATT_INSPECT((void*)buffer.data()) << BATT_INSPECT(buffer.size());
    StatusOr<i32> n_read = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
      this->async_read_some_fixed(offset, buffer, buf_index, BATT_FORWARD(handler));
    });
    LLFS_DVLOG(1) << BATT_INSPECT(n_read);
    BATT_REQUIRE_OK(n_read);
    offset += *n_read;
    buffer += *n_read;
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int IoRing::File::release()
{
  this->unregister_fd().IgnoreError();

  int released = -1;
  std::swap(released, this->fd_);
  return released;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::register_fd()
{
  if (this->registered_fd_ != -1) {
    return OkStatus();
  }

  StatusOr<i32> rfd = this->io_ring_impl_->register_fd(this->fd_);
  if (!rfd.ok()) {
    LLFS_LOG_ERROR() << "register_fd failed! " << BATT_INSPECT(rfd.status());
  }
  BATT_REQUIRE_OK(rfd);

  this->registered_fd_ = *rfd;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::unregister_fd()
{
  if (this->registered_fd_ == -1) {
    return OkStatus();
  }

  const int local_registered_fd = this->registered_fd_;
  this->registered_fd_ = -1;

  Status status = this->io_ring_impl_->unregister_fd(local_registered_fd);
  BATT_REQUIRE_OK(status) << batt::LogLevel::kError;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::close()
{
  const int fd = this->release();
  return batt::status_from_retval(batt::syscall_retry([&] {
    return ::close(fd);
  }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int IoRing::File::get_fd() const
{
  return this->fd_;
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
