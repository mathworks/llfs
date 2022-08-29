//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_RING_BUFFER_HPP
#define LLFS_RING_BUFFER_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/status.hpp>
#include <llfs/system_config.hpp>

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>
#include <batteries/case_of.hpp>
#include <batteries/interval.hpp>
#include <batteries/small_vec.hpp>

#include <stdio.h>
#include <unistd.h>

#include <cstddef>
#include <string>
#include <variant>

namespace llfs {

// Ring buffer implementation that uses memory mapping to mirror the physical buffer pages twice
// consecutively, so that wrapping at the end of the buffer is dealt with transparently my the MMU.
//
class RingBuffer
{
 public:
  struct TempFile {
    u64 byte_size;
  };

  struct NamedFile {
    std::string file_name;
    u64 byte_size;
    u64 byte_offset = 0;
    bool create = true;
    bool truncate = true;
  };

  struct FileDescriptor {
    int fd;
    u64 byte_size;
    u64 byte_offset = 0;
    bool truncate = true;
    bool close = false;
  };

  using Params = std::variant<TempFile, NamedFile, FileDescriptor>;

  // Create a new RingBuffer using the prescribed method.
  //
  // `params` can be one of the following types:
  //
  // TempFile{.byte_size}
  //   Create a new temp file with a unique name and the given size; the temp file is
  //   automatically deleted when this RingBuffer is destroyed (closing the file descriptor).
  //
  // NamedFile{.file_name, .byte_size, .byte_offset, .create, .truncate}
  //   Create or open the backing file at the given file name (path) and map the RingBuffer to the
  //   given byte offset/size within that file.  If `create` is true, then the create flag is set
  //   while opening the file.  If `truncate` is true, then the file is truncated at `size +
  //   offset`.
  //
  // FileDescriptor{.fd, .byte_size, .byte_offset, .truncate, .close}
  //   Map the given region of the file for which `fd` is an open descriptor.
  //
  explicit RingBuffer(const Params& params) noexcept;

  // Create a new RingBuffer backed by an existing open file.
  //
  explicit RingBuffer(const FileDescriptor& desc) noexcept;

  RingBuffer(const RingBuffer&) = delete;
  RingBuffer& operator=(const RingBuffer&) = delete;

  // Destroy the ring buffer.  Will close the file descriptor and unmap all mapped regions that were
  // backed by it.
  //
  ~RingBuffer() noexcept;

  // The total size in bytes of this buffer.
  //
  usize size() const;

  // The file descriptor of the open file backing the buffer.
  //
  int file_descriptor() const;

  // Return the contents of the buffer (mutable, read/write), shifted by the given offset.  The size
  // of the returned buffer will be equal to `this->size()`.
  //
  MutableBuffer get_mut(usize offset);

  // Return the contents of the buffer (const, read-only), shifted by the given offset.  The size of
  // the returned buffer will be equal to `this->size()`.
  //
  ConstBuffer get(usize offset) const;

  // Synchronize mapped region to backing file, including (inode) metadata.
  //
  Status sync();

  // Synchronize mapped region to backing file, **DATA ONLY** (no metadata).
  //
  Status datasync();

  /*! \brief Returns the list of physical buffer offsets for a given logical offset interval,
   * splitting the interval if necessary to account for wrap-around.
   */
  batt::SmallVec<batt::Interval<isize>, 2> physical_offsets_from_logical(
      const batt::Interval<isize>& logical_offsets);

 private:
  const usize size_;
  int fd_ = -1;
  const bool close_fd_ = false;
  char* memory_ = nullptr;
};

}  // namespace llfs

#endif  // LLFS_RING_BUFFER_HPP
