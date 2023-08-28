//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ring_buffer.hpp>
//

#include <llfs/status.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/syscall_retry.hpp>

#include <sys/mman.h>

namespace llfs {

static_assert(sizeof(char) == 1);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto RingBuffer::impl_pool() noexcept -> ImplPool&
{
  static ImplPool* instance_ = new ImplPool;
  return *instance_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto RingBuffer::ImplPool::allocate(const Params& params) noexcept -> Impl
{
  return batt::case_of(
      params,

      //----- --- -- -  -  -   -
      // Create a new temporary file and map it into our address space.
      //
      [this](const TempFile& p) -> Impl {
        {
          std::unique_lock<std::mutex> lock{this->mutex_};

          ImplNodeList& subpool = this->pool_[p.byte_size];
          if (!subpool.empty()) {
            BATT_CHECK_NE(subpool.size(), 0u);

            ImplNode& node = subpool.front();

            BATT_CHECK_NOT_NULLPTR(std::addressof(node))
                << BATT_INSPECT(subpool.size()) << BATT_INSPECT(subpool.empty());

            subpool.pop_front();
            Impl impl = std::move(node.impl);
            node.~ImplNode();

            BATT_CHECK_EQ((const void*)impl.memory_, (const void*)std::addressof(node));
            std::memset(impl.memory_, 0, sizeof(ImplNode));

            return impl;
          }
          // else - there is no buffer of the required size in the pool; fall-through...
        }

        return Impl{FileDescriptor{
            .fd = fileno(tmpfile()),
            .byte_size = p.byte_size,
            .byte_offset = 0,
            .truncate = true,
            .close = true,
            .cache_on_deallocate = true,
        }};
      },

      //----- --- -- -  -  -   -
      // Open the named file and map it as a ring buffer.
      //
      [](const NamedFile& p) -> Impl {
        int flags = O_DSYNC | O_RDWR;
        if (p.create) {
          flags |= O_CREAT;
        }
        if (p.truncate) {
          flags |= O_TRUNC;
        }
        return Impl{FileDescriptor{
            .fd = ::open(p.file_name.c_str(), flags, S_IRWXU),
            .byte_size = p.byte_size,
            .byte_offset = p.byte_offset,
            .truncate = p.truncate,
            .close = true,
            .cache_on_deallocate = false,
        }};
      },

      //----- --- -- -  -  -   -
      // Create an Impl from a pre-existing file descriptor.
      //
      [](const FileDescriptor& p) -> Impl {
        return Impl{p};
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto RingBuffer::ImplPool::deallocate(Impl&& impl) noexcept -> void
{
  BATT_CHECK_NOT_NULLPTR(impl.memory_);
  BATT_CHECK_GE(impl.size_, sizeof(ImplNode));
  BATT_CHECK(impl.cache_on_deallocate_);

  std::memset(impl.memory_, 0, impl.size_ * 2);

  ImplNode* node = new (impl.memory_) ImplNode{};
  node->impl = std::move(impl);
  {
    std::unique_lock<std::mutex> lock{this->mutex_};
    this->pool_[node->impl.size_].push_front(*node);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ RingBuffer::Impl::Impl(const FileDescriptor& desc) noexcept
    : size_{round_up_to_page_size_multiple(desc.byte_size)}
    , capacity_{this->size_}
    , fd_{desc.fd}
    , offset_within_file_{desc.byte_offset}
    , close_fd_{desc.close}
    , cache_on_deallocate_{desc.cache_on_deallocate}
{
  // Do this checked cast once so we can do a static_cast from here on.
  //
  [[maybe_unused]] const isize physical_size = BATT_CHECKED_CAST(isize, this->size_);

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

  this->update_mapped_regions();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
RingBuffer::Impl::Impl(Impl&& other) noexcept
    : size_{other.size_}
    , capacity_{other.capacity_}
    , fd_{other.fd_}
    , offset_within_file_{other.offset_within_file_}
    , close_fd_{other.close_fd_}
    , memory_{other.memory_}
    , cache_on_deallocate_{other.cache_on_deallocate_}
{
  other.size_ = 0;
  other.capacity_ = 0;
  other.fd_ = -1;
  other.offset_within_file_ = 0;
  other.close_fd_ = false;
  other.memory_ = nullptr;
  other.cache_on_deallocate_ = false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
RingBuffer::Impl::~Impl() noexcept
{
  if (this->memory_ != nullptr) {
    char* local_ptr = nullptr;
    std::swap(this->memory_, local_ptr);

    LLFS_WARN_IF_NOT_OK(batt::status_from_retval(batt::syscall_retry([&] {
      return munmap(local_ptr, this->size_);
    })));
    LLFS_WARN_IF_NOT_OK(batt::status_from_retval(batt::syscall_retry([&] {
      return munmap(local_ptr + this->size_, this->size_);
    })));
  }
  if (this->fd_ != -1 && this->close_fd_) {
    close(this->fd_);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto RingBuffer::Impl::operator=(Impl&& other) noexcept -> Impl&
{
  Impl tmp{std::move(other)};

  std::swap(this->size_, tmp.size_);
  std::swap(this->fd_, tmp.fd_);
  std::swap(this->close_fd_, tmp.close_fd_);
  std::swap(this->memory_, tmp.memory_);
  std::swap(this->cache_on_deallocate_, tmp.cache_on_deallocate_);

  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void RingBuffer::Impl::resize(usize new_size) noexcept
{
  new_size = round_up_to_page_size_multiple(new_size);

  BATT_CHECK_LE(new_size, this->capacity_);

  this->size_ = new_size;
  this->update_mapped_regions();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void RingBuffer::Impl::update_mapped_regions() noexcept
{
  const auto mode = PROT_READ | PROT_WRITE;
  const auto flags = MAP_SHARED | MAP_FIXED;

  char* mirror_0 = this->memory_;
  char* mirror_1 = this->memory_ + this->size_;

  // Map each half of the buffer into the underlying file.
  //
  BATT_CHECK_EQ(mirror_0,
                mmap(mirror_0, this->size_, mode, flags, this->fd_, this->offset_within_file_));

  BATT_CHECK_EQ(mirror_1,
                mmap(mirror_1, this->size_, mode, flags, this->fd_, this->offset_within_file_));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ RingBuffer::RingBuffer(const Params& params) noexcept
    : impl_{RingBuffer::impl_pool().allocate(params)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
RingBuffer::~RingBuffer() noexcept
{
  if (this->impl_.cache_on_deallocate_) {
    RingBuffer::impl_pool().deallocate(std::move(this->impl_));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize RingBuffer::size() const
{
  return this->impl_.size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int RingBuffer::file_descriptor() const
{
  return this->impl_.fd_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer RingBuffer::get_mut(usize offset)
{
  return MutableBuffer(this->impl_.memory_ + (offset % this->impl_.size_), this->impl_.size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer RingBuffer::get(usize offset) const
{
  return ConstBuffer(this->impl_.memory_ + (offset % this->impl_.size_), this->impl_.size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status RingBuffer::sync()
{
  const int retval = fsync(this->impl_.fd_);
  return batt::status_from_retval(retval);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status RingBuffer::datasync()
{
#if LLFS_PLATFORM_IS_LINUX
  const int retval = fdatasync(this->impl_.fd_);
  return batt::status_from_retval(retval);
#else
  return this->sync();
#endif
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::SmallVec<batt::Interval<isize>, 2> RingBuffer::physical_offsets_from_logical(
    const batt::Interval<isize>& logical_offsets)
{
  const isize physical_begin = logical_offsets.lower_bound % this->impl_.size_;
  const isize physical_end_no_wrap = physical_begin + logical_offsets.size();
  const isize physical_size = static_cast<isize>(this->impl_.size_);

  if (physical_end_no_wrap <= physical_size) {
    return {batt::Interval<isize>{physical_begin, physical_end_no_wrap}};
  } else {
    return {batt::Interval<isize>{physical_begin, physical_size},
            batt::Interval<isize>{0, physical_end_no_wrap - physical_size}};
  }
}

}  // namespace llfs
