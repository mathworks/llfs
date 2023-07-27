//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_IPP
#define LLFS_VOLUME_IPP

#include <llfs/buffered_log_data_reader.hpp>
#include <llfs/volume_slot_demuxer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline StatusOr<TypedVolumeReader<T>> Volume::typed_reader(const SlotRangeSpec& slot_range,
                                                           LogReadMode mode, batt::StaticType<T>)
{
  StatusOr<VolumeReader> reader = this->reader(slot_range, mode);
  BATT_REQUIRE_OK(reader);

  return TypedVolumeReader<T>{std::move(*reader)};
}

template <typename T>
u64 Volume::calculate_grant_size(const T& payload) const
{
  return packed_sizeof_slot(payload);
}

template <typename T>
StatusOr<SlotRange> Volume::append(const T& payload, batt::Grant& grant)
{
  llfs::PackObjectAsRawData<const T&> packed_obj_as_raw{payload};

  return this->slot_writer_.append(grant, packed_obj_as_raw);
}

}  // namespace llfs

#endif  // LLFS_VOLUME_IPP
