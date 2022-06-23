//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CACHE_STATUS_CODE_HPP
#define LLFS_CACHE_STATUS_CODE_HPP

namespace llfs {

enum struct StatusCode {
  kOk = 0,
  kPageIdInvalid = 1,
  kCacheSlotsFull = 2,
  kPrepareFailedLogClosed = 3,
  kPrepareFailedTrimRequired = 4,
  kCommitFailedLogClosed = 5,
  kFileLogEraseNotConfirmed = 6,
  kInvalidPageAllocatorProposal = 7,
  kRecyclerStopped = 8,
  kTooManyPendingBatchesDuringRecovery = 9,
  kInvalidBatchCommitDuringRecovery = 10,
  kBatchContainsUnknownPageDuringRecovery = 11,
  kRecoverFailedPageReallocated = 12,
  kNeedMoreDataToTrim = 13,
  kBreakSlotReaderLoop = 14,
  kSlotReaderOutOfData = 15,
  kDuplicatePrepareJob = 16,
  kBadPackedVariant = 17,
  kSlotGrantTooSmall = 18,
  kFailedToPackSlotVarHead = 19,
  kFailedToPackSlotVarTail = 20,
  kPageGenerationNotFound = 21,
};

bool initialize_status_codes();

}  // namespace llfs

#endif  // LLFS_CACHE_STATUS_CODE_HPP
