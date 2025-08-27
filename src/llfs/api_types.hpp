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

/** \brief If set to true and the specified item/page is not found, then additional diagnostics will
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
BATT_STRONG_TYPEDEF(i64, FileOffset);

/** \brief Wrapper for off_t used as a dirent offset.
 */
BATT_STRONG_TYPEDEF(off_t, DirentOffset);

/** \brief Wrapper for off_t used as a length.
 */
BATT_STRONG_TYPEDEF(i64, FileLength);

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

/** \brief A count of items.
 */
BATT_STRONG_TYPEDEF(usize, ItemCount);

/** \brief A count of hash functions.
 */
BATT_STRONG_TYPEDEF(usize, HashCount);

/** \brief A pseudo-random number generator seed.
 */
BATT_STRONG_TYPEDEF(u32, RandomSeed);

/** \brief A number of bytes.
 */
BATT_STRONG_TYPEDEF(usize, ByteCount);

/** \brief A integer number of bits.
 */
BATT_STRONG_TYPEDEF(usize, BitCount);

/** \brief A real number of bits.
 */
BATT_STRONG_TYPEDEF(double, RealBitCount);

/** \brief A number of 64-bit words.
 */
BATT_STRONG_TYPEDEF(usize, Word64Count);

/** \brief A false-positive error rate.
 */
BATT_STRONG_TYPEDEF(double, FalsePositiveRate);

/** \brief A number of slots.
 */
BATT_STRONG_TYPEDEF(usize, SlotCount);

/** \brief A limit on cache size, in bytes.
 */
BATT_STRONG_TYPEDEF(usize, MaxCacheSizeBytes);

/** \brief The relative priority for LRU page eviction; higher == prefer *not* to evict.
 */
BATT_STRONG_TYPEDEF(i64, LruPriority);

/** \brief Whether to ignore key (page_id) matching while pinning a slot in the cache.
 */
BATT_STRONG_TYPEDEF(bool, IgnoreKey);

/** \brief Whether to ignore page generation matching while pinning a slot in the cache.
 */
BATT_STRONG_TYPEDEF(bool, IgnoreGeneration);

/** \brief Whether to compute and store paranoid data integrity checks when building a packed Bloom
 * filter.
 */
BATT_STRONG_TYPEDEF(bool, ComputeChecksum);

/** \brief The number of bits per item to use when building a filter.
 */
BATT_STRONG_TYPEDEF(usize, BitsPerKey);

/** \brief Whether a new page in a page cache job is recovered (via load).
 */
BATT_STRONG_TYPEDEF(bool, IsRecoveredPage);

}  // namespace llfs

#endif  // LLFS_API_TYPES_HPP
