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

namespace llfs {

template <typename T>
inline StatusOr<TypedVolumeReader<T>> Volume::typed_reader(const SlotRangeSpec& slot_range,
                                                           LogReadMode mode, batt::StaticType<T>)
{
  StatusOr<VolumeReader> reader = this->reader(slot_range, mode);
  BATT_REQUIRE_OK(reader);

  return TypedVolumeReader<T>{std::move(*reader)};
}

}  // namespace llfs

#endif  // LLFS_VOLUME_IPP
