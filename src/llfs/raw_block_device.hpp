//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_RAW_BLOCK_DEVICE_HPP
#define LLFS_RAW_BLOCK_DEVICE_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/status.hpp>

#include <batteries/status.hpp>

namespace llfs {

// Abstracts low-level (block-aligned, unbuffered) I/O to durable storage.
//
class RawBlockDevice
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

  RawBlockDevice(const RawBlockDevice&) = delete;
  RawBlockDevice& operator=(const RawBlockDevice&) = delete;

  virtual ~RawBlockDevice() = default;

  // Write `data` to the device at the specified bytes offset.  This may result in a short write, in
  // which case the caller is responsible for retrying.
  //
  // The memory pointed to by `data` must be 512-byte aligned; `data.size()` must be a multiple of
  // 512.
  //
  virtual StatusOr<i64> write_some(i64 offset, const ConstBuffer& data) = 0;

  // Read as much data as possible into `buffer` from the device, starting at device byte `offset`.
  // This may result in a short read, in which case the caller is responsible for retrying.
  //
  // The memory pointed to by `data` must be 512-byte aligned; `data.size()` must be a multiple of
  // 512.
  //
  virtual StatusOr<i64> read_some(i64 offset, const MutableBuffer& buffer) = 0;

  // Returns the current device size (i.e., the greatest valid offset plus the minimum valid data
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

 protected:
  RawBlockDevice() = default;
};

// Write all the given data to the device.
//
Status write_all(RawBlockDevice& device, i64 offset, const ConstBuffer& data);

// Fill the entire buffer by reading from the device.
//
Status read_all(RawBlockDevice& device, i64 offset, const MutableBuffer& buffer);

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

      *n_transferred = RawBlockDevice::align_down(*n_transferred);
      if (*n_transferred == 0) {
        return batt::StatusCode::kInternal;
      }

      offset += *n_transferred;
      remaining += *n_transferred;
      break;
    }
  }
  return batt::OkStatus();
}

}  // namespace llfs

#endif  // LLFS_RAW_BLOCK_DEVICE_HPP
