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
      CODE_WITH_MSG_(StatusCode::kPinFailedPageEvicted,
                     "PageIdSlot::try_pin failed; the requested page was evicted"),  // 34
      CODE_WITH_MSG_(StatusCode::kPageViewUserDataAlreadyInitialized,
                     "PageView::init_user_data failed; there is already conflicting user data for "
                     "this page"),  // 35,
      CODE_WITH_MSG_(
          StatusCode::kRecoverFailedGenerationZero,
          "Failed to recover page; generation is 0 (can not confirm page header status)"),  // 36,
      CODE_WITH_MSG_(StatusCode::kUnpackCastWrongIntegerSize,
                     "Failed to unpack byte-ordered integer: wrong size"),  // 37,
      CODE_WITH_MSG_(StatusCode::kUnpackCastNullptr,
                     "Packed data is `nullptr`"),  // 38,
      CODE_WITH_MSG_(StatusCode::kUnpackCastVariantStructOutOfBounds,
                     "PackedVariant struct is (partially) outside the given buffer"),  // 39,
      CODE_WITH_MSG_(StatusCode::kUnpackCastIntegerOutOfBounds,
                     "Packed integer is (partially) outside the given buffer"),  // 40,
      CODE_WITH_MSG_(StatusCode::kUnpackCastPackedBytesStructUnder,
                     "Failed to unpack bytes"),  // 41,
      CODE_WITH_MSG_(StatusCode::kUnpackCastPackedBytesStructOver,
                     "Failed to unpack bytes"),                                               // 42,
      CODE_WITH_MSG_(StatusCode::kUnpackCastPackedBytesDataUnder, "Failed to unpack bytes"),  // 43,
      CODE_WITH_MSG_(StatusCode::kUnpackCastPackedBytesDataOver, "Failed to unpack bytes"),   // 44,
      CODE_WITH_MSG_(StatusCode::kUnpackCastStructUnder, "Failed to unpack struct"),          // 45,
      CODE_WITH_MSG_(StatusCode::kUnpackCastStructOver, "Failed to unpack struct"),           // 46,
      CODE_WITH_MSG_(StatusCode::kPageHeaderBadMagic, "Sanity check failed"),                 // 47,
      CODE_WITH_MSG_(StatusCode::kPageHeaderBadPageId, "Sanity check failed"),                // 48,
      CODE_WITH_MSG_(StatusCode::kPageHeaderBadPageSize, "Sanity check failed"),              // 49,
      CODE_WITH_MSG_(StatusCode::kPageHeaderBadUnusedSize, "Sanity check failed"),            // 50,
      CODE_WITH_MSG_(StatusCode::kPageHeaderBadUnusedBegin, "Sanity check failed"),           // 51,
      CODE_WITH_MSG_(StatusCode::kPageHeaderBadUnusedEnd, "Sanity check failed"),             // 52,
      CODE_WITH_MSG_(StatusCode::kPageHeaderBadGeneration, "Sanity check failed"),            // 53,
      CODE_WITH_MSG_(StatusCode::kLogBlockBadMagic,
                     "Log block header contains bad magic number"),  // 54,
      CODE_WITH_MSG_(StatusCode::kLogBlockCommitSizeOverflow,
                     "Log block header commit size is too large"),  // 55,
      CODE_WITH_MSG_(StatusCode::kStorageFileBadConfigBlockMagic,
                     "Failed to read storage file"),  // 56,
      CODE_WITH_MSG_(StatusCode::kStorageFileBadConfigBlockCrc,
                     "Failed to read storage file"),  // 57,
      CODE_WITH_MSG_(StatusCode::kOutOfAttachments,
                     "Failed to attach to PageAllocator: no more available attachments"),  // 58,
      CODE_WITH_MSG_(StatusCode::kPageAllocatorNotAttached,
                     "Client not attached to the PageAllocator"),  // 59,
      CODE_WITH_MSG_(StatusCode::kPageReaderConflict,
                     "Conflicting PageReaders registered for the same layout id"),  // 60,
      CODE_WITH_MSG_(
          StatusCode::kPageHeaderBadLayoutId,
          "PackedPageHeader::layout_id does not match PageView::get_page_layout_id()"),  // 61,
      CODE_WITH_MSG_(StatusCode::kPutViewUnknownLayoutId,
                     "PageCache::put_view failed; page layout id is not registered"),  // 62,
      CODE_WITH_MSG_(StatusCode::kPageCacheSlotNotInitialized,
                     "The page cache slot for this PageId is not initialized"),  // 63,
      CODE_WITH_MSG_(
          StatusCode::kIoRingShutDown,
          "The operation could not be completed because the IoRing was shut down"),  // 64,
      CODE_WITH_MSG_(StatusCode::kLogControlBlockBadMagic,
                     "Log device control block magic number is not correct (is this really a log "
                     "device?)"),  // 65,
      CODE_WITH_MSG_(StatusCode::kLogDeviceV1Deprecated,
                     "IoRingLogDevice (aka LogDevice storage object) has been deprecated; use "
                     "IoRingLogDevice2/LogDevice2 instead"),  // 66,
      CODE_WITH_MSG_(StatusCode::kStorageObjectNotLastInFile,
                     "Failed to create llfs file. There are multiple storage objects "
                     "marked as 'last_in_file'"),  // 67,
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
