//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_LOG_STORAGE_DRIVER_CONTEXT_HPP
#define LLFS_LOG_STORAGE_DRIVER_CONTEXT_HPP

#include <llfs/ring_buffer.hpp>

#include <atomic>

namespace llfs {

struct LogStorageDriverContext {
  explicit LogStorageDriverContext(const RingBuffer::Params& params) noexcept : buffer_{params}
  {
  }

  std::atomic<bool> closed_{false};
  RingBuffer buffer_;
};

}  // namespace llfs

#endif  // LLFS_LOG_STORAGE_DRIVER_CONTEXT_HPP
