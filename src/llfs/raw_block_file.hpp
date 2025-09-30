//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_RAW_BLOCK_FILE_HPP
#define LLFS_RAW_BLOCK_FILE_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring.hpp>
#include <llfs/status.hpp>
#include <llfs/status_code.hpp>

#include <batteries/status.hpp>

namespace llfs {

// Abstracts low-level (block-aligned, unbuffered) I/O to durable storage.
//
class RawBlockFile
{
 public:
  // For the convenience of implementations; verifies the alignment and size constraints of the
  // passed buffer (and, optionally, offset).
  //
  static Status validate_buffer(const ConstBuffer& buffer, i64 offset = 0);

  // Return the least block-aligned value not less than n.
  //
  static i64 align_up(i64 n);

  // Return the greatest block-aligned value not greater than n.
  //
  static i64 align_down(i64 n);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  RawBlockFile(const RawBlockFile&) = delete;
  RawBlockFile& operator=(const RawBlockFile&) = delete;

  virtual ~RawBlockFile() = default;

  // Write `data` to the file at the specified bytes offset.  This may result in a short write, in
  // which case the caller is responsible for retrying.
  //
  // The memory pointed to by `data` must be block-aligned; `data.size()` must be a multiple of
  // kDirectIOBlockSize.
  //
  virtual StatusOr<i64> write_some(i64 offset, const ConstBuffer& data) = 0;

  // Read as much data as possible into `buffer` from the file, starting at file byte `offset`.
  // This may result in a short read, in which case the caller is responsible for retrying.
  //
  // The memory pointed to by `data` must be block-aligned; `data.size()` must be a multiple of
  // kDirectIOBlockSize.
  //
  virtual StatusOr<i64> read_some(i64 offset, const MutableBuffer& buffer) = 0;

  // Returns the current file size (i.e., the greatest valid offset plus the minimum valid data
  // size).
  //
  virtual StatusOr<i64> get_size() = 0;

  // (Optional API) Attempt to resize the backing file (as if by the POSIX `truncate` syscall).
  //
  // Returns `batt::OkStatus` on success, error status otherwise (`batt::StatusCode::kUnimplemented`
  // if not supported by this implementation, which is the default).
  //
  virtual Status truncate(i64 /*new_offset_upper_bound*/)
  {
    return batt::StatusCode::kUnimplemented;
  }

  // (Optional API) Attempt to resize the backing file (as if by the POSIX `truncate` syscall), IFF
  // the file is smaller than `minimum_size`.  This function should never shrink the file or destroy
  // data.
  //
  // Returns `batt::OkStatus` on success, error status otherwise (`batt::StatusCode::kUnimplemented`
  // if not supported by this implementation, which is the default).
  //
  virtual Status truncate_at_least(i64 /*minimum_size*/)
  {
    return batt::StatusCode::kUnimplemented;
  }

#ifndef LLFS_DISABLE_IO_URING
  //
  virtual IoRing::File* get_io_ring_file()
  {
    return nullptr;
  }
  //
#endif  // LLFS_DISABLE_IO_URING

 protected:
  RawBlockFile() = default;
};

// Write all the given data to the file.
//
Status write_all(RawBlockFile& file, i64 offset, const ConstBuffer& data);

// Fill the entire buffer by reading from the file.
//
Status read_all(RawBlockFile& file, i64 offset, const MutableBuffer& buffer);

// Generic form of write_all/read_all.
//
template <typename BufferT, typename TransferOp>
Status transfer_all(i64 offset, const BufferT& buffer, TransferOp&& transfer_some)
{
  BufferT remaining = buffer;
  while (remaining.size() != 0) {
    for (;;) {
      StatusOr<i64> n_transferred = transfer_some(offset, remaining);
      if (!n_transferred.ok() && batt::status_is_retryable(n_transferred.status())) {
        continue;
      }
      BATT_REQUIRE_OK(n_transferred);

      *n_transferred = RawBlockFile::align_down(*n_transferred);
      if (*n_transferred == 0) {
        return make_status(StatusCode::kTransferAllBadAlignment);
      }

      offset += *n_transferred;
      remaining += *n_transferred;
      break;
    }
  }
  return batt::OkStatus();
}

}  // namespace llfs

#endif  // LLFS_RAW_BLOCK_FILE_HPP
