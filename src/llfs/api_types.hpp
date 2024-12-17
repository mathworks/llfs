//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_API_TYPES_HPP
#define LLFS_API_TYPES_HPP

#include <llfs/int_types.hpp>

#include <batteries/strong_typedef.hpp>

#include <sys/types.h>

namespace llfs {

BATT_STRONG_TYPEDEF(usize, ThreadPoolSize);

BATT_STRONG_TYPEDEF(usize, MaxQueueDepth);

/*! \brief If set to true and the specified item/page is not found, then additional diagnostics will
 * be emitted (via logging).  Setting to false will suppress these diagnostics (as the application
 * has indicated that 'not found' is an expected/normal case for these calls).
 */
BATT_STRONG_TYPEDEF(bool, OkIfNotFound);

/** \brief Ask DataPacker to use parallel_copy and its configured WorkerPool to copy data, if
 * possible.
 */
BATT_STRONG_TYPEDEF(bool, UseParallelCopy);

/** \brief The number of bytes by which to delay trimming a Volume root log.
 */
BATT_STRONG_TYPEDEF(u64, TrimDelayByteCount);

/** \brief Wrapper for off_t used as an offset.
 */
BATT_STRONG_TYPEDEF(off_t, FileOffset);

/** \brief Wrapper for off_t used as a dirent offset.
 */
BATT_STRONG_TYPEDEF(off_t, DirentOffset);

/** \brief Wrapper for off_t used as a length.
 */
BATT_STRONG_TYPEDEF(off_t, FileLength);

/** \brief Wrapper for int used as an fd.
 */
BATT_STRONG_TYPEDEF(int, FileDescriptorInt);

/** \brief A buffer size requirement.
 */
BATT_STRONG_TYPEDEF(usize, BufferSizeNeeded);

/** \brief Whether we are using the *_plus variant of an API.
 */
BATT_STRONG_TYPEDEF(bool, PlusApi);

/** \brief Integer interpreted as a FUSE Impl file handle.
 */
BATT_STRONG_TYPEDEF(u64, FuseFileHandle);

/** \brief True if only data/contents should be flushed, not metadata.
 */
BATT_STRONG_TYPEDEF(bool, IsDataSync);

/** \brief Int flags bitmask passed into open/opendir.
 */
BATT_STRONG_TYPEDEF(int, FileOpenFlags);

/** \brief The number of buffers.
 */
BATT_STRONG_TYPEDEF(usize, BufferCount);

/** \brief The size of each buffer.
 */
BATT_STRONG_TYPEDEF(usize, BufferSize);

/** \brief True if a page contains outgoing references to other pages.
 */
BATT_STRONG_TYPEDEF(bool, HasOutgoingRefs);

/** \brief An index into a collection of items.
 */
BATT_STRONG_TYPEDEF(usize, ItemOffset);

/** \brief A pseudo-random number generator seed.
 */
BATT_STRONG_TYPEDEF(u32, RandomSeed);

}  // namespace llfs

#endif  // LLFS_API_TYPES_HPP
