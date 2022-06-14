#pragma once
#ifndef LLFS_VOLUME_EVENTS_FWD_HPP
#define LLFS_VOLUME_EVENTS_FWD_HPP

#include <llfs/packed_variant.hpp>

namespace llfs {

struct PackedVolumeAttachEvent;
struct PackedVolumeDetachEvent;
struct PackedVolumeIds;
struct PackedVolumeRecovered;
struct PackedVolumeFormatUpgrade;
struct PackedPrepareJob;
struct PackedCommitJob;
struct PackedRollbackJob;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
using VolumeEventVariant = PackedVariant<PackedVolumeIds,            // 0
                                         PackedVolumeAttachEvent,    // 1
                                         PackedVolumeDetachEvent,    // 2
                                         PackedVolumeRecovered,      // 3
                                         PackedPrepareJob,           // 4
                                         PackedCommitJob,            // 5
                                         PackedRollbackJob,          // 6
                                         PackedVolumeFormatUpgrade,  // 7
                                         PackedRawData               // 8
                                         // 9..255 : reserved for future use.
                                         >;

}  // namespace llfs

#endif  // LLFS_VOLUME_EVENTS_FWD_HPP
