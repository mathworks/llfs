//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_METADATA_HPP
#define LLFS_VOLUME_METADATA_HPP

#include <llfs/config.hpp>
//
#include <llfs/optional.hpp>
#include <llfs/slot.hpp>
#include <llfs/volume_events.hpp>

#include <unordered_map>

namespace llfs {

/** \brief Volume metadata that is written to the root log and refreshed on trim.
 */
struct VolumeMetadata {
  //----- --- -- -  -  -   -

  /** \brief Stores the slot offset of the most recent attach event appended to record an attachment
   * associated with this volume.
   */
  struct AttachInfo {
    Optional<slot_offset_type> last_refresh;
    PackedVolumeAttachEvent event;
  };

  /** \brief The uuids for this Volume.
   */
  Optional<PackedVolumeIds> ids;

  /** \brief The slot offset of the most recent refresh of `ids`.
   */
  Optional<slot_offset_type> ids_last_refresh;

  /** \brief The current set of attachments, with most recent slot (if present).
   */
  std::unordered_map<VolumeAttachmentId, AttachInfo, VolumeAttachmentId::Hash> attachments;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_METADATA_HPP
