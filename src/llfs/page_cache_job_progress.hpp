//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_JOB_PROGRESS_HPP
#define LLFS_PAGE_CACHE_JOB_PROGRESS_HPP

#include <batteries/assert.hpp>

#include <ostream>

namespace llfs {

enum struct PageCacheJobProgress {
  kPending,
  kDurable,
  kCancelled,
};

inline bool is_terminal_state(PageCacheJobProgress t)
{
  switch (t) {
    case PageCacheJobProgress::kPending:
      return false;
    case PageCacheJobProgress::kDurable:
      return true;
    case PageCacheJobProgress::kCancelled:
      return true;
  }
  BATT_PANIC() << "bad value for PageCacheJobProgress: " << (int)t;
  BATT_UNREACHABLE();
}

inline std::ostream& operator<<(std::ostream& out, PageCacheJobProgress t)
{
  switch (t) {
    case PageCacheJobProgress::kPending:
      return out << "Pending";
    case PageCacheJobProgress::kDurable:
      return out << "Durable";
    case PageCacheJobProgress::kCancelled:
      return out << "Cancelled";
  }
  return out << "Unknown";
}

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_JOB_PROGRESS_HPP
