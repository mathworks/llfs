#include <llfs/ring_buffer.hpp>
//

#include <sys/mman.h>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ RingBuffer::RingBuffer(const Params& params) noexcept
    : RingBuffer{batt::case_of(
          params,
          [this](const TempFile& p) {
            return FileDescriptor{
                .fd = fileno(tmpfile()),
                .byte_size = p.byte_size,
                .byte_offset = 0,
                .truncate = true,
                .close = true,
            };
          },
          [this](const NamedFile& p) {
            int flags = O_DSYNC | O_RDWR;
            if (p.create) {
              flags |= O_CREAT;
            }
            if (p.truncate) {
              flags |= O_TRUNC;
            }
            return FileDescriptor{
                .fd = ::open(p.file_name.c_str(), flags, S_IRWXU),
                .byte_size = p.byte_size,
                .byte_offset = p.byte_offset,
                .truncate = p.truncate,
                .close = true,
            };
          },
          [this](const FileDescriptor& p) {
            return p;
          })}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ RingBuffer::RingBuffer(const FileDescriptor& desc) noexcept
    : size_{round_up_to_page_size_multiple(desc.byte_size)}
    , fd_{desc.fd}
    , close_fd_{desc.close}
{
  BATT_UNTESTED_COND(desc.byte_offset > 0);

  BATT_CHECK_NE(this->fd_, -1);

  // Size it as desired.
  //
  if (desc.truncate) {
    BATT_CHECK_NE(ftruncate(this->fd_, this->size_), -1);
  } else {
    BATT_CHECK_EQ(this->size_, desc.byte_size);
  }

  // Map a region of size_*2 into the virtual memory table.
  //
  this->memory_ = reinterpret_cast<char*>(
      mmap(NULL, this->size_ * 2, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

  BATT_CHECK_NOT_NULLPTR((void*)this->memory_);

  const auto mode = PROT_READ | PROT_WRITE;
  const auto flags = MAP_SHARED | MAP_FIXED;

  auto* mirror_0 = this->memory_;
  auto* mirror_1 = this->memory_ + this->size_;

  // Map each half of the buffer into the underlying tmp file.
  //
  BATT_CHECK_EQ(mirror_0, mmap(mirror_0, this->size_, mode, flags, this->fd_, desc.byte_offset));
  BATT_CHECK_EQ(mirror_1, mmap(mirror_1, this->size_, mode, flags, this->fd_, desc.byte_offset));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
RingBuffer::~RingBuffer() noexcept
{
  if (this->memory_ != nullptr) {
    munmap(this->memory_, this->size_);
    munmap(this->memory_ + this->size_, this->size_);
  }
  if (this->fd_ != -1 && this->close_fd_) {
    close(this->fd_);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize RingBuffer::size() const
{
  return size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int RingBuffer::file_descriptor() const
{
  return this->fd_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer RingBuffer::get_mut(usize offset)
{
  return MutableBuffer(this->memory_ + (offset % this->size_), this->size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer RingBuffer::get(usize offset) const
{
  return ConstBuffer(this->memory_ + (offset % this->size_), this->size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status RingBuffer::sync()
{
  const int retval = fsync(this->fd_);
  return batt::status_from_retval(retval);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status RingBuffer::datasync()
{
  const int retval = fdatasync(this->fd_);
  return batt::status_from_retval(retval);
}

}  // namespace llfs
