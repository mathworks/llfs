//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_SLOT_DEMUXER_IPP
#define LLFS_VOLUME_SLOT_DEMUXER_IPP

#include <llfs/volume_event_visitor.hpp>

#include <batteries/utility.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
template <typename FnArg>
/*explicit*/ VolumeSlotDemuxer<R, Fn>::VolumeSlotDemuxer(FnArg&& slot_visitor_fn,
                                                         VolumeEventVisitor<R>& base) noexcept
    : visitor_fn_{BATT_FORWARD(slot_visitor_fn)}
    , base_{base}
    , visited_upper_bound_{None}
{
  initialize_status_codes();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
Optional<slot_offset_type> VolumeSlotDemuxer<R, Fn>::get_safe_trim_pos() const
{
  return this->visited_upper_bound_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_raw_data(const SlotParse& slot,
                                                  const Ref<const PackedRawData>& raw) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_raw_data(" << BATT_INSPECT(slot) << ")";

  Status status = this->visitor_fn_(slot, raw_data_from_slot(slot, raw.pointer()));
  BATT_REQUIRE_OK(status);
  return this->base_.on_raw_data(slot, raw);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_prepare_job(
    const SlotParse& slot, const Ref<const PackedPrepareJob>& prepare) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_prepare_job(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_prepare_job(slot, prepare);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_commit_job(
    const SlotParse& slot, const Ref<const PackedCommitJob>& commit) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_commit_job(" << BATT_INSPECT(slot) << ")";

  const SlotParse& commit_slot = slot;
  const usize commit_slot_size = slot.size_in_bytes();
  std::string_view user_data = commit.get().user_data();

  // The user_slot must reference the job prepare slot so that data isn't trimmed too soon by user
  // code.
  //
  const auto user_slot = SlotParse{
      .offset = commit_slot.offset,
      .body = commit_slot.body,
      .total_grant_spent = commit.get().prepare_slot_size + commit_slot_size,
  };

  Status status = this->visitor_fn_(user_slot, user_data);
  BATT_REQUIRE_OK(status);

  return this->base_.on_commit_job(slot, commit);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_rollback_job(
    const SlotParse& slot, const PackedRollbackJob& rollback) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_rollback_job(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_rollback_job(slot, rollback);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_volume_attach(
    const SlotParse& slot, const PackedVolumeAttachEvent& attach) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_volume_attach(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_volume_attach(slot, attach);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_volume_detach(
    const SlotParse& slot, const PackedVolumeDetachEvent& detach) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_volume_detach(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_volume_detach(slot, detach);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_volume_ids(const SlotParse& slot,
                                                    const PackedVolumeIds& ids) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_volume_ids(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_volume_ids(slot, ids);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_volume_recovered(
    const SlotParse& slot, const PackedVolumeRecovered& recovered) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_volume_recovered(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_volume_recovered(slot, recovered);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_volume_format_upgrade(
    const SlotParse& slot, const PackedVolumeFormatUpgrade& upgrade) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_volume_format_upgrade(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_volume_format_upgrade(slot, upgrade);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
StatusOr<R> VolumeSlotDemuxer<R, Fn>::on_volume_trim(const SlotParse& slot,
                                                     const VolumeTrimEvent& trim) /*override*/
{
  auto on_scope_exit = batt::finally([&] {
    this->mark_slot_visited(slot);
  });

  LLFS_VLOG(1) << "on_volume_trim(" << BATT_INSPECT(slot) << ")";

  return this->base_.on_volume_trim(slot, trim);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename R, typename Fn>
void VolumeSlotDemuxer<R, Fn>::mark_slot_visited(const SlotParse& slot)
{
  BATT_CHECK_IMPLIES(this->visited_upper_bound_,
                     slot_less_than(*this->visited_upper_bound_, slot.offset.upper_bound))
      << "Slot offsets must be strictly increasing! " << BATT_INSPECT(this->visited_upper_bound_)
      << BATT_INSPECT(slot);

  this->visited_upper_bound_ = slot.offset.upper_bound;
}

}  // namespace llfs

#endif  // LLFS_VOLUME_SLOT_DEMUXER_IPP
