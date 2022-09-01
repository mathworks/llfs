//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/status_code.hpp>
//

#include <batteries/status.hpp>

namespace llfs {

#define CODE_WITH_MSG_(code, msg)                                                                  \
  {                                                                                                \
    code, msg " (" #code ")"                                                                       \
  }

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool initialize_status_codes()
{
  static bool const initialized = batt::Status::register_codes<StatusCode>({
      CODE_WITH_MSG_(StatusCode::kOk, "Ok"),                                                  // 0
      CODE_WITH_MSG_(StatusCode::kPageIdInvalid, "The passed PageId is invalid"),             // 1
      CODE_WITH_MSG_(StatusCode::kCacheSlotsFull, "Could not allocate slot; cache is full"),  // 2
      CODE_WITH_MSG_(StatusCode::kPrepareFailedLogClosed,
                     "Could not prepare slot in RingBufferDevice: the log has been closed."),  // 3
      CODE_WITH_MSG_(StatusCode::kPrepareFailedTrimRequired,
                     "Could not prepare slot in RingBufferDevice: not enough space available; trim "
                     "the log."),  // 4
      CODE_WITH_MSG_(StatusCode::kCommitFailedLogClosed,
                     "Could not commit slot in RingBufferDevice: the log has been closed."),  // 5
      CODE_WITH_MSG_(StatusCode::kFileLogEraseNotConfirmed,
                     "FileLogDevice initialize was not confirmed"),  // 6
      CODE_WITH_MSG_(StatusCode::kInvalidPageAllocatorProposal,
                     "Invalid PageAllocator proposal"),                              // 7
      CODE_WITH_MSG_(StatusCode::kRecyclerStopped, "The PageRecycler was stopped"),  // 8
      CODE_WITH_MSG_(StatusCode::kTooManyPendingBatchesDuringRecovery, ""),          // 9
      CODE_WITH_MSG_(StatusCode::kInvalidBatchCommitDuringRecovery, ""),             // 10
      CODE_WITH_MSG_(StatusCode::kBatchContainsUnknownPageDuringRecovery, ""),       // 11
      CODE_WITH_MSG_(StatusCode::kRecoverFailedPageReallocated,
                     "Failed to recover the given PageId"),                            // 12
      CODE_WITH_MSG_(StatusCode::kNeedMoreDataToTrim, "Need more data to trim"),       // 13
      CODE_WITH_MSG_(StatusCode::kBreakSlotReaderLoop, "SlotReader was interrupted"),  // 14
      CODE_WITH_MSG_(StatusCode::kSlotReaderOutOfData, "SlotReader is out of data"),   // 15
      CODE_WITH_MSG_(StatusCode::kDuplicatePrepareJob,
                     "Found two PackedPrepareJob events with the same slot offset"),  // 16
      CODE_WITH_MSG_(StatusCode::kBadPackedVariant, "bad PackedVariant"),             // 17
      CODE_WITH_MSG_(StatusCode::kSlotGrantTooSmall, "Slot Grant too small"),         // 18
      CODE_WITH_MSG_(StatusCode::kFailedToPackSlotVarHead,
                     "Failed to pack slot variant head"),  // 19
      CODE_WITH_MSG_(StatusCode::kFailedToPackSlotVarTail,
                     "Failed to pack slot variant tail"),  // 20
      CODE_WITH_MSG_(StatusCode::kPageGenerationNotFound,
                     "PageDevice read failed with the given PageId because the current generation "
                     "number doesn't match the requested generation"),  // 21
      CODE_WITH_MSG_(StatusCode::kFileLogDeviceConfigWriteFailed,
                     "Could not write FileLogDevice config file"),  // 22
      CODE_WITH_MSG_(StatusCode::kFileLogDeviceConfigReadFailed,
                     "Could not read FileLogDevice config file"),  // 23
      CODE_WITH_MSG_(StatusCode::kStorageObjectTypeError,
                     "Storage object is not of expected type"),  // 24
      CODE_WITH_MSG_(StatusCode::kTransferAllBadAlignment,
                     "Transferred byte count has bad alignment"),  // 25
      CODE_WITH_MSG_(
          StatusCode::kPageCacheConfigFileBadMagic,
          "Bad magic number (PackedPageCacheConfigFile) - Possible data corruption"),  // 26
      CODE_WITH_MSG_(StatusCode::kFakeLogDeviceExpectedFailure,
                     "FakeLogDevice failed (as expected; TESTING ONLY)"),  // 27
      CODE_WITH_MSG_(StatusCode::kNoReaderForPageViewType,
                     "No reader registered for the specified page view type"),  // 28
      CODE_WITH_MSG_(StatusCode::kIoRingLogFlushOpExpectedFailure,
                     "IoRingFlushOpTest: write_data failed (as expected; TESTING ONLY)"),  // 29
      CODE_WITH_MSG_(StatusCode::kFilesystemPageWriteFailed,
                     "I/O error writing page to filesystem"),  // 30
      CODE_WITH_MSG_(StatusCode::kFilesystemPageOpenFailed,
                     "I/O error opening page in filesystem"),  // 31
      CODE_WITH_MSG_(StatusCode::kFilesystemPageReadFailed,
                     "I/O error reading page in filesystem"),               // 32
      CODE_WITH_MSG_(StatusCode::kFilesystemRemoveFailed, "remove error"),  // 33
  });
  return initialized;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
::batt::Status make_status(StatusCode code)
{
  initialize_status_codes();

  return ::batt::Status{code};
}

}  // namespace llfs
