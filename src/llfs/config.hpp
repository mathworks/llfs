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

//+++++++++++-+-+--+----- --- -- -  -  -   -
#ifdef __APPLE__
#define LLFS_PLATFORM_IS_APPLE 1
#else
#undef LLFS_PLATFORM_IS_APPLE
#endif

//+++++++++++-+-+--+----- --- -- -  -  -   -
#ifdef __linux__
#define LLFS_PLATFORM_IS_LINUX 1
#else
#undef LLFS_PLATFORM_IS_LINUX
#endif

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

#if !LLFS_PLATFORM_IS_LINUX
#define LLFS_DISABLE_IO_URING 1
#endif

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

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
constexpr usize kLogPageSize = 512;
constexpr usize kLogPageSizeLog2 = 9;

BATT_STATIC_ASSERT_EQ(usize{1} << kLogPageSizeLog2, kLogPageSize);

constexpr usize kLogAtomicWriteSize = 512;
constexpr usize kLogAtomicWriteSizeLog2 = 9;

BATT_STATIC_ASSERT_EQ(usize{1} << kLogAtomicWriteSizeLog2, kLogAtomicWriteSize);

// The maximum number of page buffers of a given size to cache (in order to avoid/reduce heap
// allocation).
//
constexpr usize kPageBufferPoolSize = 256;

// The number of size-based sub-pools to maintain in the page buffer pool/cache; each level's buffer
// size is twice the buffer size of the previous level.
//
constexpr usize kPageBufferPoolLevels = 32;

// Used in the code to react to debug/release builds.
//
#ifndef NDEBUG
constexpr bool kDebugBuild = true;
#else
constexpr bool kDebugBuild = false;
#endif

// Logging configuration; uncomment one of the lines below to select the logging implementation.
//
//+++++++++++-+-+--+----- --- -- -  -  -   -
//#define LLFS_DISABLE_LOGGING
//#define LLFS_USE_BOOST_LOG
#define LLFS_USE_GLOG
//#define LLFS_USE_SELF_LOGGING
//+++++++++++-+-+--+----- --- -- -  -  -   -

// Experimental feature: first-class ranges as keys.
//
#define LLFS_ENABLE_RANGE_KEYS 0

// Experimental feature: fast log device initialization (only write the first log page header).
//
constexpr bool kFastIoRingLogDeviceInit = true;

// Experimental feature: fast page device initialization (don't write any page headers; always fail
// page recovery when generation == 0).
//
constexpr bool kFastIoRingPageDeviceInit = true;

}  // namespace llfs

#endif  // LLFS_CONFIG_HPP
