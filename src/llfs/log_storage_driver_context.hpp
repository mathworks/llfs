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

#include <llfs/config.hpp>
//
#include <llfs/ring_buffer.hpp>
#include <llfs/status.hpp>

#include <batteries/async/mutex.hpp>

#include <atomic>

namespace llfs {

struct LogStorageDriverContext {
  explicit LogStorageDriverContext(const RingBuffer::Params& params) noexcept : buffer_{params}
  {
  }

  /** \brief Updates the stored error status with the passed value.
   */
  void update_error_status(Status status) noexcept
  {
    batt::ScopedLock<Status> locked{this->error_status_};

    locked->Update(status);
  }

  /** \brief Sets the stored error status to the passed value.
   */
  void set_error_status(Status status) noexcept
  {
    batt::ScopedLock<Status> locked{this->error_status_};

    *locked = status;
  }

  /** \brief Returns the stored error status.
   */
  Status get_error_status() noexcept
  {
    batt::ScopedLock<Status> locked{this->error_status_};

    return *locked;
  }

  std::atomic<bool> closed_{false};
  batt::Mutex<Status> error_status_{OkStatus()};
  RingBuffer buffer_;
};

}  // namespace llfs

#endif  // LLFS_LOG_STORAGE_DRIVER_CONTEXT_HPP
