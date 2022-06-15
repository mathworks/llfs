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

bool initialize_status_codes()
{
  static bool const initialized = batt::Status::register_codes<StatusCode>({
      CODE_WITH_MSG_(StatusCode::kOk, "Ok"),
      CODE_WITH_MSG_(StatusCode::kPageIdInvalid, "The passed PageId is invalid"),
      CODE_WITH_MSG_(StatusCode::kCacheSlotsFull, "Could not allocate slot; cache is full"),
      CODE_WITH_MSG_(StatusCode::kPrepareFailedLogClosed,
                     "Could not prepare slot in RingBufferDevice: the log has been closed."),
      CODE_WITH_MSG_(
          StatusCode::kPrepareFailedTrimRequired,
          "Could not prepare slot in RingBufferDevice: not enough space available; trim the log."),
      CODE_WITH_MSG_(StatusCode::kCommitFailedLogClosed,
                     "Could not commit slot in RingBufferDevice: the log has been closed."),
      CODE_WITH_MSG_(StatusCode::kFileLogEraseNotConfirmed,
                     "FileLogDevice initialize was not confirmed"),
      CODE_WITH_MSG_(StatusCode::kRecyclerStopped, "The PageRecycler was stopped"),
      CODE_WITH_MSG_(StatusCode::kTooManyPendingBatchesDuringRecovery, ""),
      CODE_WITH_MSG_(StatusCode::kInvalidBatchCommitDuringRecovery, ""),
      CODE_WITH_MSG_(StatusCode::kBatchContainsUnknownPageDuringRecovery, ""),
      CODE_WITH_MSG_(StatusCode::kRecoverFailedPageReallocated,
                     "Failed to recover the given PageId"),
      CODE_WITH_MSG_(StatusCode::kNeedMoreDataToTrim, "Need more data to trim"),
      CODE_WITH_MSG_(StatusCode::kBreakSlotReaderLoop, "SlotReader was interrupted"),
      CODE_WITH_MSG_(StatusCode::kSlotReaderOutOfData, "SlotReader is out of data"),
      CODE_WITH_MSG_(StatusCode::kDuplicatePrepareJob,
                     "Found two PackedPrepareJob events with the same slot offset"),
      CODE_WITH_MSG_(StatusCode::kBadPackedVariant, "bad PackedVariant"),
      CODE_WITH_MSG_(StatusCode::kSlotGrantTooSmall, "Slot Grant too small"),
      CODE_WITH_MSG_(StatusCode::kFailedToPackSlotVarHead, "Failed to pack slot variant head"),
      CODE_WITH_MSG_(StatusCode::kFailedToPackSlotVarTail, "Failed to pack slot variant tail"),
  });
  return initialized;
}

}  // namespace llfs
