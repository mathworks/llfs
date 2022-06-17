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
  virtual StatusOr<i64> write_to(i64 offset, const ConstBuffer& data) = 0;

  // Read as much data as possible into `buffer` from the device, starting at device byte `offset`.
  // This may result in a short read, in which case the caller is responsible for retrying.
  //
  // The memory pointed to by `data` must be 512-byte aligned; `data.size()` must be a multiple of
  // 512.
  //
  virtual StatusOr<i64> read_from(i64 offset, const MutableBuffer& buffer) = 0;

  // (Optional API) Attempt to resize the backing file (as if by the POSIX `truncate` syscall).
  //
  // Returns `batt::OkStatus` on success, error status otherwise (`batt::StatusCode::kUnimplemented`
  // if not supported by this implementation, which is the default).
  //
  virtual Status truncate(i64 /*new_offset_upper_bound*/)
  {
    return batt::StatusCode::kUnimplemented;
  }

 protected:
  RawBlockDevice() = default;
};

}  // namespace llfs

#endif  // LLFS_RAW_BLOCK_DEVICE_HPP
