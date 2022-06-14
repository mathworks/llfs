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
