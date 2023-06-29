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

}  // namespace llfs

#endif  // LLFS_API_TYPES_HPP
