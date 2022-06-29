//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CONFIG_HPP
#define LLFS_CONFIG_HPP

#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/strong_typedef.hpp>

#include <atomic>

namespace llfs {

// Set to 1 to disable crc generation for pages.
//
#define LLFS_DISABLE_PAGE_CRC 1

// Uncomment to disable support for io_uring.
//
//#define LLFS_DISABLE_IO_URING

// The number of bits in a page_id int allocated to page device.
//
constexpr usize kPageDeviceIdBits = 24;

// The queue discipline for page pool allocation.
//
enum PageAllocPolicy { kFirstInFirstOut, kFirstInLastOut };
constexpr PageAllocPolicy kPageAllocPolicy = kFirstInLastOut;

// The maximum number of page ref indirections possible, so we can bound the size of the
// PageRecycler's state machine.
//
constexpr usize kMaxPageRefDepth = 32;

// The maximum size of a page, log_2.
//
constexpr u8 kMaxPageSizeLog2 = 32;

// The default page size for a PageVolume/PageDevice.
//
constexpr u32 kDefaultPageSize = 4 * kKiB;

BATT_STRONG_TYPEDEF(u64, MaxRefsPerPage);

// The default limit for PageAllocator attachments.
//
constexpr unsigned kDefaultMaxPoolAttachments = 32;

// ** FOR TESTING ONLY **
//
// Suppress ERROR/WARNING level output for expected errors while running unit tests.
//
inline std::atomic<bool>& suppress_log_output_for_test()
{
  static std::atomic<bool> value_{false};
  return value_;
}

// The device-level log page size and max atomic write size.
//
constexpr usize kLogPageSize = 4 * kKiB;
constexpr usize kLogAtomicWriteSize = 4 * kKiB;
constexpr usize kLogAtomicWriteBits = 12;

BATT_STATIC_ASSERT_EQ(usize{1} << kLogAtomicWriteBits, kLogAtomicWriteSize);

// The maximum number of page buffers of a given size to cache (in order to avoid/reduce heap
// allocation).
//
constexpr usize kPageBufferPoolSize = 256;

// The number of size-based sub-pools to maintain in the page buffer pool/cache; each level's buffer
// size is twice the buffer size of the previous level.
//
constexpr usize kPageBufferPoolLevels = 32;

}  // namespace llfs

#endif  // LLFS_CONFIG_HPP
