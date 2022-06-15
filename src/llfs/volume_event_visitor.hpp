//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_EVENT_VISITOR_HPP
#define LLFS_VOLUME_EVENT_VISITOR_HPP

#include <llfs/packable_ref.hpp>
#include <llfs/ref.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/volume_events.hpp>

#include <batteries/utility.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Handlers for Volume events, each of which returns type R.
//
template <typename R>
class VolumeEventVisitor
{
 public:
  static VolumeEventVisitor& null_impl();

  VolumeEventVisitor(const VolumeEventVisitor&) = delete;
  VolumeEventVisitor& operator=(const VolumeEventVisitor&) = delete;

  virtual ~VolumeEventVisitor() = default;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#define LLFS_VOLUME_EVENT_HANDLER_DECL(EVENT_TYPE, METHOD_NAME)                                    \
  virtual R METHOD_NAME(const SlotParse&, const EVENT_TYPE&) = 0;                                  \
                                                                                                   \
  decltype(auto) operator()(const SlotParse& slot, const EVENT_TYPE& event)                        \
  {                                                                                                \
    return this->METHOD_NAME(slot, event);                                                         \
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  LLFS_VOLUME_EVENT_HANDLER_DECL(Ref<const PackedRawData>, on_raw_data)
  LLFS_VOLUME_EVENT_HANDLER_DECL(Ref<const PackedPrepareJob>, on_prepare_job)
  LLFS_VOLUME_EVENT_HANDLER_DECL(PackedCommitJob, on_commit_job)
  LLFS_VOLUME_EVENT_HANDLER_DECL(PackedRollbackJob, on_rollback_job)
  LLFS_VOLUME_EVENT_HANDLER_DECL(PackedVolumeAttachEvent, on_volume_attach)
  LLFS_VOLUME_EVENT_HANDLER_DECL(PackedVolumeDetachEvent, on_volume_detach)
  LLFS_VOLUME_EVENT_HANDLER_DECL(PackedVolumeIds, on_volume_ids)
  LLFS_VOLUME_EVENT_HANDLER_DECL(PackedVolumeRecovered, on_volume_recovered)
  LLFS_VOLUME_EVENT_HANDLER_DECL(PackedVolumeFormatUpgrade, on_volume_format_upgrade)

#undef LLFS_VOLUME_EVENT_HANDLER_DECL

 protected:
  VolumeEventVisitor() = default;
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

template <typename R>
/*static*/ VolumeEventVisitor<R>& VolumeEventVisitor<R>::null_impl()
{
  class NullImpl : public VolumeEventVisitor<R>
  {
   public:
    R on_raw_data(const SlotParse&, const Ref<const PackedRawData>&) override
    {
      return batt::make_default<R>();
    }

    R on_prepare_job(const SlotParse&, const Ref<const PackedPrepareJob>&) override
    {
      return batt::make_default<R>();
    }

    R on_commit_job(const SlotParse&, const PackedCommitJob&) override
    {
      return batt::make_default<R>();
    }

    R on_rollback_job(const SlotParse&, const PackedRollbackJob&) override
    {
      return batt::make_default<R>();
    }

    R on_volume_attach(const SlotParse&, const PackedVolumeAttachEvent&) override
    {
      return batt::make_default<R>();
    }

    R on_volume_detach(const SlotParse&, const PackedVolumeDetachEvent&) override
    {
      return batt::make_default<R>();
    }

    R on_volume_ids(const SlotParse&, const PackedVolumeIds&) override
    {
      return batt::make_default<R>();
    }

    R on_volume_recovered(const SlotParse&, const PackedVolumeRecovered&) override
    {
      return batt::make_default<R>();
    }

    R on_volume_format_upgrade(const SlotParse&, const PackedVolumeFormatUpgrade&) override
    {
      return batt::make_default<R>();
    }
  };

  // It's OK that this is non-const, since it has no state.
  //
  static NullImpl impl_;

  return impl_;
}

}  // namespace llfs

#endif  // LLFS_VOLUME_EVENT_VISITOR_HPP
