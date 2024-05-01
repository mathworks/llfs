//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_MULTI_APPEND_HPP
#define LLFS_VOLUME_MULTI_APPEND_HPP

#ifndef LLFS_VOLUME_HPP
#error This file must be included from <llfs/volume.hpp>!
#endif

namespace llfs {

class VolumeMultiAppend
{
 public:
  explicit VolumeMultiAppend(Volume& volume) noexcept : op_{*volume.slot_writer_}
  {
  }

  VolumeMultiAppend(const VolumeMultiAppend&) = delete;
  VolumeMultiAppend& operator=(const VolumeMultiAppend&) = delete;

  ~VolumeMultiAppend() noexcept
  {
    BATT_CHECK(this->completed_);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  StatusOr<SlotRange> append(const T& payload, const batt::Grant& grant) noexcept
  {
    BATT_CHECK(!this->completed_);

    llfs::PackObjectAsRawData<const T&> packed_obj_as_raw{payload};

    return this->op_.append(grant, packed_obj_as_raw);
  }

  StatusOr<SlotRange> append(const std::string_view& payload, const batt::Grant& grant)
  {
    return this->append(pack_as_raw(payload), grant);
  }

  StatusOr<SlotRange> commit(batt::Grant& grant) noexcept
  {
    BATT_CHECK(!this->completed_);
    this->completed_ = true;
    return this->op_.finalize(grant);
  }

  void cancel() noexcept
  {
    BATT_CHECK(!this->completed_);
    this->completed_ = true;
  }

 private:
  TypedSlotWriter<VolumeEventVariant>::MultiAppend op_;
  bool completed_ = false;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_MULTI_APPEND_HPP
