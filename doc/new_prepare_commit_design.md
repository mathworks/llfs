# Proposal: Replicate Root Page Refs and User Data to Simply Job Txns and Trimming

## Problem Statement

Currently, a multi-page transaction ("job" from hereafter) is considered
to be durable/committed only once all the following have occurred:

1. PackedPrepareJob slot written/flushed
2. All new Page data written
3. All PageAllocator ref count updates written/flushed
4. PackedCommitJob slot written

A PackedCommitJob is currently only a slot offset pointer to the
prepare slot it finalizes.  Thus in order to present a committed job's
user data to the application, we need to reference information stored
in the prior PackedPrepareJob slot.  This same dependency on both
slots is also present when the VolumeTrimmer is trim the Volume root
log, since the main function of the VolumeTrimmer is to update ref
counts in response to trimmed root refs.

This current design presents several significant problems:

1. Additional complexity when reading slots (currently implemented by
   the VolumeSlotDemuxer class), since we must store a map from slot
   offset to prepare job record until we see the corresponding commit,
   in both reading and trimming workflows
2. This poses a dilemma to the trimmer: should we allow the trimming
   of a prepare slot, but not its later commit slot?  If we do, we
   have a commit slot that is essentially useless from the standpoint
   of the application.  If we don't, this could introduce latency
   spikes into trimming due to the fact that "interleaved" jobs
   (prepare-1, prepare-2, commit-1, prepare-3, commit-2, prepare-4,
   etc.) could indefinitely arrest trimming, up to the maximum
   capacity of the log.  This is possible even today, with serialized
   jobs, since we allow pipelining of the job txn protocol steps
   enumerated above.

As of 2023-08-23 (when this is being written), the VolumeTrimmer
implements further problematic behavior, namely it will happily trim
the prepare slot for an _ongoing_ transaction!  When it does so, it
attempts to roll-forward the information from the prepare slot that it
cares about (the root ref list), which has resulted in the
implementation of a complicated and buggy scheme of Grant reservation
and management.  Given the complexity of this system, it is very
difficult to achieve high confidence in its correctness.

Worse (and more to the point), the very scenario it presumes is
nonsensical: what use is it to trim a prepare _before the job has even
committed?_ Recall from above that the application is unable to
derive anything useful from just the commit slot, as it is just a
pointer to the slot offset of the prepare (where all useful
information is stored).

## Solution

The new design trades a small amount of write amplification for
drastically simpler system design by extending the PackedCommitJob
record to include the (opaque) user data and root page refs from the
prepare record.  Typically the user data is small (&lt;100 bytes), and
the increased I/O is largely offset by alleviating the need for
obscure hacks like reserving extra space in the PackedPrepareJob
record to balance the trimmer's current roll-forward scheme (this is
so that the Grant needed to roll prepare information forward can
always be reclaimed from the trimmed region itself).  Since the commit
is no longer depend on the earlier prepare slot, the new design
eliminates the need to keep track of prepare slots as the root log is
read, and the trimmer no longer faces the dilemma described above: it
can simply treat commit slots as stand-alone records and take action
accordingly when they are trimmed.  This means that log scanning gets
faster, and we can get rid of the `depends_on_slot` field in the
`SlotParse` structure, simplifying application code.
