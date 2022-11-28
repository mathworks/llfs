# VolumeTrimmer

The VolumeTrimmer task is responsible for monitoring a Volume's SlotLockManager and responding to advances in the trim position.  The SlotLockManager maintains a thread-safe priority heap of slot offsets; the lowest of these is the "safe" trim point of the log.  When a lock is advanced (i.e., its offset is increased) or released, this may cause the trim point to change.  This will cause the VolumeTrimmer to wake up and attempt to trim the log.

There are two main complications to log trimming: Page root references and Volume metadata.  A "root reference" (or just "root" for short) is a PageId in the Volume's main log that controls a ref count on that page.  Roots appear inside PrepareJob slots, which are the first phase of the two-phase commit protocol used to commit a multi-page write transaction.  Volume metadata includes the UUIDs of the Volume itself, its PageRecycler, and its VolumeTrimmer.  The other metadata are PageDevice attachment and detachment events.  It is necessary to attach to a PageDevice via its PageAllocator before a component can update ref counts for the pages in that device because the attachment provides the mechanism that prevents double-updates by giving each update a unique logical timestamp and enforcing a strict one-update-at-a-time discipline per attachment.

## Trimming Root References

The requirement when trimming a root reference is that the VolumeTrimmer must decrement the page ref count before doing the actual trim, which will permanently delete the trimmed roots.  Complicating matters, however, is that VolumeTrimmer must only decrement ref counts once the page update job has been _committed_.  It is not sufficient to simply find a PrepareJob slot; the corresponding CommitJob slot must be trimmed in order to validly decrement the ref count.  In addition, the VolumeTrimmer must send page ref count updates in a deterministic fashion; a given update must always use the same client slot offset even after crash recovery, so that updates are exactly-once.

If a PrepareJob and its matching CommitJob appear in the same trimmed log region, this is straightforward.  If the prepare and commit are split across different trimmed regions, however, VolumeTrimmer must maintain some state across trim operations.  In order to properly recover this state after a crash, a TrimEvent slot is written by the VolumeTrimmer before doing the ref count updates and log trim.  The TrimEvent contains the SlotRange of the trimmed region, as well as any unmatched PrepareJob and CommitJob records in the trimmed region.  It's not immediately obvious why CommitJobs are also necessary, but consider what happens on recovery: as the VolumeTrimmer is reconstructing its state while scanning the log, it must know which roots (PrepareJobs) are resolved in a subsequent trim.  Thus both are needed.

## Volume Metadata

Any VolumeIds or AttachmentEvent slots read while scanning a trimmed region are refreshed to the end of the log prior to trimming.  This guarantees they are never lost, at the cost of a slight increase in write amplification.

## Log Space Management

Trimming a log is the only way to reclaim space for future appends, but as described above, trimming requires appending information to the log itself.  Consequently, it is possible for a log to be too full to successfully trim, resulting in deadlock.

To prevent this, we must reserve space ahead of time in order to guarantee there is always enough log space available to perform a trim.  The general rule is that every append must reserve enough space both for itself and for any future appends that will be necessary when it is later trimmed.  Doing the reserve for both atomically ensures that deadlock can't happen due to hold-and-wait (one of the four necessary conditions for deadlock).

In the context of the VolumeTrimmer, this means we must allocate extra space whenever appending the following slot types:

- PrepareJob
- CommitJob
- DeviceAttach
- DeviceDetach
- VolumeIds
- TrimEvent

Right before the Grant for any of the above slot types is spent (i.e., during append), we split off the extra portion of the Grant and hand its quota off to the VolumeTrimmer, which collects these allocations in a single "trimmer grant."  When a trim event occurs, this grant is spent down to do the necessary appends.  However, trimming the log also frees up log space, which can be used to replentish the trimmer grant itself.  This is what provides the extra allocation needed to trim the trimmer's own appended slots later on.  Even though these allocations do not happen at the same time, like e.g. PrepareJob/CommitJob, we still avoid hold-and-wait since the "allocation" step is the same as trimming the log, which the VolumeTrimmer controls.

## Sequence of Events: A Trim Event

Putting all of this together, we present the protocol used by VolumeTrimmer to trim the log:

1. Wait for the trimmable lower bound (as given by SlotLockManager) to advance
2. Scan the trimmable region, collecting:
   1. PrepareJob slots
   2. CommitJob slots
   3. Device Attachment Events
   4. (past) Trim Events
   5. Volume UUIDs
3. Refresh Volume metadata by appending copies of all (2.3) and (2.5) slots found in step (2)
4. Collect up the list of droppable roots (PageIds) by matching the Prepare/Commit slots found in step (2), along with any past (unresolved) PrepareJobs
5. Write a TrimEvent to the log containing: 
   1. The slot range (offsets) being trimmed
   2. Any unresolved PrepareJobs from (2.1) and (2.4)
   3. Any CommitJobs from (2.2) whose prepare slot is _not_ in the trimmed region
6. Wait for (5) to be flushed to durable storage
7. Commit a PageCacheJob that decrements ref counts of all the droppable roots collected in step (4); wait for all PageAllocator logs to be completely flushed
8. Trim the log, retaining the grant; save sizeof(AttachEvents) + sizeof(VolumeIds) + 2 * sizeof(TrimEvents) - (sizeof(PrepareJobs from (2.1)) + sizeof(CommitJobs from (2.2))) of this grant and release the rest.

## State Recovery 

There are two goals for VolumeTrimmer recovery:

1. Recover the state of the Trimmer task itself
2. If there was an in-progress trim, complete that trim

The state we are trying to recover is mostly the list of pending PrepareJob slots that have been trimmed in the past.  These are captured in TrimEvent slots; during the recovery scan, we can reconstruct this information by processing these slots in-order.  Note, as stated above, the extracted CommitJob information in TrimEvents is crucial to this task, as they allow us to know when the absence of a CommitJob slot means that a PrepareJob is still pending, and when it means that the job completed in a trimmed portion of the log.  

While scanning, we reconstruct the data collected in step (2) of the trimming protocol.  Achieving goal (2) depends on being able to correctly and unambiguously infer where the VolumeTrimmer is in its loop as given above.  We divide the possible cases into two broad categories by comparing the true trim pos to the trimmed upper bound on the most recent TrimEvent found during the scan:

- If true trim pos == latest TrimEvent.upper_bound, then we must be in steps (1-5)
- If true trim pos < latest TrimEvent.upper_bound, then we must be after (6) and before (8); in other words, we are in step (7)

If the true trim pos is > latest TrimEvent.upper_bound, then the protocol must not have been followed, as (5) & (6) must come before (8); if this is the case, we declare the Volume to be invalid and fail.

If we are in step (7), then we must take the scanned information from step (2), which we obtained during the recovery scan, and use it to do step (4), finally repeating step (7) and (8).  If step (7) was already durable in the PageAllocator logs (partially or completely), then it is fine because using the slot of the most recent TrimEvent protects us against page ref count double-updates.  Once we have confirmation that (7) is complete, we continue at (8), etc.

If we are in steps (1-5), then there is no chance that we will erroneously double-update page ref counts, so we don't really care what (if any) the previously observed trimmable slot offset was from step (1) before the crash.   Because we scan the entire log on recovery, we know whether any Volume metadata in the next trimmable region is refreshed later on in the log, so we can avoid repeating work for step (3).  All that's left is to do step (5), which we know for sure hasn't happened because we didn't find a TrimEvent for a pending trim.

To make recovery easier for step (3), we will track the most recent slot for all the Volume metadata as part of the trimmer task state.