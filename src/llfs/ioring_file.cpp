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
IoRing::File::File(IoRing& io, int fd) noexcept : io_{&io}, fd_{fd}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::File::File(File&& that) noexcept : io_{that.io_}, fd_{that.fd_}
{
  that.fd_ = -1;
  that.registered_fd_ = -1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRing::File::operator=(File&& that) noexcept -> File&
{
  File copy{std::move(that)};

  std::swap(this->io_, copy.io_);
  std::swap(this->fd_, copy.fd_);
  std::swap(this->registered_fd_, copy.registered_fd_);

  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::File::~File() noexcept
{
  if (this->fd_ != -1) {
    batt::syscall_retry([&] {
      return ::close(this->fd_);
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::write_all(i64 offset, ConstBuffer buffer)
{
  while (buffer.size() != 0) {
    BATT_CHECK_EQ(batt::round_down_bits(Self::kBlockAlignmentLog2, offset), offset);
    BATT_CHECK_EQ(batt::round_down_bits(Self::kBlockAlignmentLog2, buffer.size()), buffer.size());
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
Status IoRing::File::read_all(i64 offset, MutableBuffer buffer)
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int IoRing::File::release()
{
  int released = -1;
  std::swap(released, this->fd_);
  return released;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRing::File::register_fd()
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
