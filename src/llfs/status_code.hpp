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

#include <batteries/status.hpp>

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
  kFileLogDeviceConfigWriteFailed = 22,
  kFileLogDeviceConfigReadFailed = 23,
  kStorageObjectTypeError = 24,
  kTransferAllBadAlignment = 25,
  kPageCacheConfigFileBadMagic = 26,
  kFakeLogDeviceExpectedFailure = 27,
  kNoReaderForPageViewType = 28,
  kIoRingLogFlushOpExpectedFailure = 29,
  kFilesystemPageWriteFailed = 30,
  kFilesystemPageOpenFailed = 31,
  kFilesystemPageReadFailed = 32,
  kFilesystemRemoveFailed = 33,
  kPinFailedPageEvicted = 34,
  kPageViewUserDataAlreadyInitialized = 35,
  kRecoverFailedGenerationZero = 36,
  kUnpackCastWrongIntegerSize = 37,
  kUnpackCastNullptr = 38,
  kUnpackCastVariantStructOutOfBounds = 39,
  kUnpackCastIntegerOutOfBounds = 40,
  kUnpackCastPackedBytesStructUnder = 41,
  kUnpackCastPackedBytesStructOver = 42,
  kUnpackCastPackedBytesDataUnder = 43,
  kUnpackCastPackedBytesDataOver = 44,
  kUnpackCastStructUnder = 45,
  kUnpackCastStructOver = 46,
  kPageHeaderBadMagic = 47,
  kPageHeaderBadPageId = 48,
  kPageHeaderBadPageSize = 49,
  kPageHeaderBadUnusedSize = 50,
  kPageHeaderBadUnusedBegin = 51,
  kPageHeaderBadUnusedEnd = 52,
  kPageHeaderBadGeneration = 53,
  kLogBlockBadMagic = 54,
  kLogBlockCommitSizeOverflow = 55,
  kStorageFileBadConfigBlockMagic = 56,
  kStorageFileBadConfigBlockCrc = 57,
  kOutOfAttachments = 58,
  kPageAllocatorNotAttached = 59,
  kPageReaderConflict = 60,
  kPageHeaderBadLayoutId = 61,
  kPutViewUnknownLayoutId = 62,
  kPageCacheSlotNotInitialized = 63,
  kIoRingShutDown = 64,
  kLogControlBlockBadMagic = 65,
  kLogDeviceV1Deprecated = 66,
  kStorageObjectNotLastInFile = 67,
};

bool initialize_status_codes();

::batt::Status make_status(StatusCode code);

}  // namespace llfs

#endif  // LLFS_CACHE_STATUS_CODE_HPP
