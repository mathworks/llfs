//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_metadata_refresher.hpp>
//

namespace llfs {

/*explicit*/ VolumeMetadataRefresher::VolumeMetadataRefresher(
    TypedSlotWriter<VolumeEventVariant>& slot_writer, VolumeMetadata&& recovered) noexcept
    : state_{slot_writer, std::move(recovered)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 VolumeMetadataRefresher::grant_target() const noexcept
{
  return this->state_.lock()->grant_target();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 VolumeMetadataRefresher::grant_size() const noexcept
{
  return this->state_.lock()->grant_size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 VolumeMetadataRefresher::grant_required() const noexcept
{
  return this->state_.lock()->grant_required();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::update_grant(batt::Grant& pool) noexcept
{
  return this->state_.lock()->update_grant(pool);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::add_attachment(VolumeAttachmentId id,
                                               slot_offset_type user_slot_offset,
                                               batt::Grant& grant) noexcept
{
  return this->state_.lock()->add_attachment(id, user_slot_offset, grant);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::remove_attachment(VolumeAttachmentId id) noexcept
{
  return this->state_.lock()->remove_attachment(id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize VolumeMetadataRefresher::attachment_count() const noexcept
{
  return this->state_.lock()->attachment_count();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::invalidate(slot_offset_type slot_offset) noexcept
{
  return this->state_.lock()->invalidate(slot_offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool VolumeMetadataRefresher::needs_flush() const noexcept
{
  return this->state_.lock()->needs_flush();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> VolumeMetadataRefresher::flush() noexcept
{
  return this->state_.lock()->flush();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class VolumeMetadataRefresher::State

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeMetadataRefresher::State::State(TypedSlotWriter<VolumeEventVariant>& slot_writer,
                                                   VolumeMetadata&& metadata) noexcept
    : slot_writer_{slot_writer}
    , metadata_{std::move(metadata)}
    , grant_{BATT_OK_RESULT_OR_PANIC(
          this->slot_writer_.reserve(this->grant_target(), batt::WaitForResource::kFalse))}
{
  BATT_CHECK(this->metadata_.ids);

  if (!this->metadata_.ids_last_refresh) {
    this->ids_need_refresh_ = true;
  }

  for (const auto& [attach_id, attach_info] : this->metadata_.attachments) {
    if (!attach_info.last_refresh) {
      this->attachments_needing_refresh_.emplace_back(attach_id);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 VolumeMetadataRefresher::State::grant_target() const noexcept
{
  return packed_sizeof_slot_with_payload_size(sizeof(PackedVolumeIds)) +
         kAttachmentGrantSize * this->attachment_count();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 VolumeMetadataRefresher::State::grant_size() const noexcept
{
  return this->grant_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 VolumeMetadataRefresher::State::grant_required() const noexcept
{
  const u64 target = this->grant_target();

  return target - std::min(target, this->grant_size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::State::update_grant(batt::Grant& pool) noexcept
{
  StatusOr<batt::Grant> spent = pool.spend(this->grant_required(), batt::WaitForResource::kFalse);
  BATT_REQUIRE_OK(spent);

  this->grant_.subsume(std::move(*spent));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::State::add_attachment(VolumeAttachmentId attach_id,
                                                      slot_offset_type user_slot_offset,
                                                      batt::Grant& grant) noexcept
{
  if (this->metadata_.attachments.count(attach_id) != 0) {
    return OkStatus();
  }

  BATT_ASSIGN_OK_RESULT(batt::Grant attachment_grant,
                        grant.spend(kAttachmentGrantSize, batt::WaitForResource::kFalse));

  this->metadata_.attachments.emplace(attach_id, VolumeMetadata::AttachInfo{
                                                     .last_refresh = None,
                                                     .event = PackedVolumeAttachEvent{{
                                                         .id = attach_id,
                                                         .user_slot_offset = user_slot_offset,
                                                     }},
                                                 });

  this->attachments_needing_refresh_.emplace_back(attach_id);

  BATT_CHECK_OK(this->update_grant(attachment_grant));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::State::remove_attachment(VolumeAttachmentId attach_id) noexcept
{
  if (this->metadata_.attachments.count(attach_id) == 0) {
    return OkStatus();
  }

  this->metadata_.attachments.erase(attach_id);
  this->attachments_needing_refresh_.emplace_back(attach_id);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize VolumeMetadataRefresher::State::attachment_count() const noexcept
{
  return this->metadata_.attachments.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeMetadataRefresher::State::invalidate(slot_offset_type slot_offset) noexcept
{
  if (this->metadata_.ids_last_refresh &&
      slot_less_than(*this->metadata_.ids_last_refresh, slot_offset)) {
    this->ids_need_refresh_ = true;
  }

  for (const auto& [attach_id, attach_info] : this->metadata_.attachments) {
    if (!attach_info.last_refresh) {
      continue;
    }
    const slot_offset_type last_refresh_slot = *attach_info.last_refresh;
    if (slot_less_than(last_refresh_slot, slot_offset)) {
      this->attachments_needing_refresh_.emplace_back(attach_id);
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool VolumeMetadataRefresher::State::needs_flush() const noexcept
{
  return this->ids_need_refresh_ || !this->attachments_needing_refresh_.empty();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> VolumeMetadataRefresher::State::flush() noexcept
{
  Optional<slot_offset_type> lower_bound;
  Optional<slot_offset_type> upper_bound;

  const auto update_written_slot_range = [&](const SlotRange& slot_range) {
    // Update the written slot range.
    //
    if (!lower_bound) {
      lower_bound.emplace(slot_range.lower_bound);
    }
    clamp_min_slot(&upper_bound, slot_range.upper_bound);
  };

  //----- --- -- -  -  -   -
  //
  if (this->ids_need_refresh_) {
    LLFS_VLOG(1) << "Refreshing volume ids: " << this->metadata_.ids;

    StatusOr<SlotRange> ids_refresh_slot =
        this->slot_writer_.append(this->grant_, *this->metadata_.ids);

    BATT_REQUIRE_OK(ids_refresh_slot);

    // Mark ids as up-to-date.
    //
    this->ids_need_refresh_ = false;
    this->metadata_.ids_last_refresh = ids_refresh_slot->lower_bound;

    update_written_slot_range(*ids_refresh_slot);
  }

  //----- --- -- -  -  -   -
  // Refresh any attachments that are out-of-date.
  //
  usize attach_refresh_count = 0;

  // In any case, erase those attachments which we succeeded in updating.
  //
  auto on_scope_exit = batt::finally([&] {
    this->attachments_needing_refresh_.erase(
        this->attachments_needing_refresh_.begin(),
        std::next(this->attachments_needing_refresh_.begin(), attach_refresh_count));
  });

  for (const auto& attach_id : this->attachments_needing_refresh_) {
    LLFS_VLOG(1) << "Refreshing attachment " << attach_id;

    auto iter = this->metadata_.attachments.find(attach_id);
    const bool is_attached = iter != this->metadata_.attachments.end();

    StatusOr<SlotRange> slot_range = [&] {
      if (is_attached) {
        StatusOr<SlotRange> new_slot_range =
            this->slot_writer_.append(this->grant_, iter->second.event);

        if (new_slot_range.ok()) {
          iter->second.last_refresh = new_slot_range->lower_bound;
        }
        return new_slot_range;
      }
      return this->slot_writer_.append(  //
          this->grant_, PackedVolumeDetachEvent{{
                            .id = attach_id,
                            .user_slot_offset = this->slot_writer_.slot_offset(),
                        }});
    }();

    BATT_REQUIRE_OK(slot_range);

    ++attach_refresh_count;
    update_written_slot_range(*slot_range);
  }

  if (!lower_bound) {
    lower_bound = this->slot_writer_.slot_offset();
  }
  if (!upper_bound) {
    upper_bound = lower_bound;
  }

  return {SlotRange{*lower_bound, *upper_bound}};
}

}  //namespace llfs
