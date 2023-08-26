//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_METADATA_REFRESHER_HPP
#define LLFS_VOLUME_METADATA_REFRESHER_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_writer.hpp>
#include <llfs/volume_events.hpp>
#include <llfs/volume_metadata.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/mutex.hpp>

#include <unordered_map>
#include <vector>

namespace llfs {

/** \brief Tracks when the Volume metadata was last refreshed, and writes update slots to the log.
 */
class VolumeMetadataRefresher
{
 public:
  static const usize kVolumeIdsGrantSize = VolumeMetadata::kVolumeIdsGrantSize;
  static const usize kAttachmentGrantSize = VolumeMetadata::kAttachmentGrantSize;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit VolumeMetadataRefresher(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                   VolumeMetadata&& recovered) noexcept;

  VolumeMetadataRefresher(const VolumeMetadataRefresher&) = delete;
  VolumeMetadataRefresher& operator=(const VolumeMetadataRefresher&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the most recent slot at which the volume ids were refreshed, or None if they
   * are not currently in the log.
   */
  Optional<slot_offset_type> ids_last_refresh_slot() const noexcept;

  /** \brief Returns the most recent slot at which the given attachment was refreshed, or None if it
   * is not currently in the log.
   */
  Optional<slot_offset_type> attachment_last_refresh_slot(
      const VolumeAttachmentId& attach_id) const noexcept;

  /** \brief Returns the size that the refresh grant *should*, ideally, be.
   */
  u64 grant_target() const noexcept;

  /** \brief Returns the actual refresh grant size.
   */
  u64 grant_size() const noexcept;

  /** \brief Returns the number of grant bytes needed to reach `grant_target()`; this is always
   * `this->grant_target() - this->grant_size()`.
   */
  u64 grant_required() const noexcept;

  /** \brief Spends some of `pool`, if necessary, to bring the refresh grant up to where it should
   * be.  Differs from this->update_grant(pool) in that this function does not fail if it can only
   * grab _some_ grant from `pool`.
   */
  Status update_grant_partial(batt::Grant& pool) noexcept;

  /** \brief Spends some of `pool`, if necessary, to bring the refresh grant up to where it should
   * be.
   */
  Status update_grant(batt::Grant& pool) noexcept;

  /** \brief Adds a new attachment; does not append anything to the log (see flush).
   */
  Status add_attachment(VolumeAttachmentId id, slot_offset_type user_slot_offset,
                        batt::Grant& grant) noexcept;

  /** \brief Removes an attachment; does not append anything to the log (see flush).
   */
  Status remove_attachment(VolumeAttachmentId id) noexcept;

  /** \brief Returns the number of attachments being tracked by this object.
   */
  usize attachment_count() const noexcept;

  /** \brief Marks anything that was last refreshed before the specified log offset as needing to be
   * refreshed.
   */
  Status invalidate(slot_offset_type slot_offset) noexcept;

  /** \brief Returns true iff metadata needs to be refreshed to the log.
   */
  bool needs_flush() const noexcept;

  /** \brief The size of grant needed to call flush() successfully.
   */
  u64 flush_grant_size() const noexcept;

  /** \brief Writes all pending updates to the log, returning the slot range of the update(s).
   */
  StatusOr<SlotRange> flush() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  class State
  {
   public:
    explicit State(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                   VolumeMetadata&& recovered) noexcept;

    Optional<slot_offset_type> ids_last_refresh_slot() const noexcept;

    Optional<slot_offset_type> attachment_last_refresh_slot(
        const VolumeAttachmentId& attach_id) const noexcept;

    u64 grant_target() const noexcept;

    u64 grant_size() const noexcept;

    u64 grant_required() const noexcept;

    Status update_grant_partial(batt::Grant& pool) noexcept;

    Status update_grant(batt::Grant& pool) noexcept;

    Status add_attachment(VolumeAttachmentId id, slot_offset_type user_slot_offset,
                          batt::Grant& grant) noexcept;

    Status remove_attachment(VolumeAttachmentId id) noexcept;

    usize attachment_count() const noexcept;

    Status invalidate(slot_offset_type slot_offset) noexcept;

    bool needs_flush() const noexcept;

    u64 flush_grant_size() const noexcept;

    StatusOr<SlotRange> flush() noexcept;

   private:
    TypedSlotWriter<VolumeEventVariant>& slot_writer_;

    VolumeMetadata metadata_;

    bool ids_need_refresh_ = false;

    std::vector<VolumeAttachmentId> attachments_needing_refresh_;

    batt::Grant grant_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::Mutex<State> state_;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_METADATA_REFRESHER_HPP
