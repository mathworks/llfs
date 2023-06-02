//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_PENDING_JOBS_MAP_HPP
#define LLFS_VOLUME_PENDING_JOBS_MAP_HPP

#include <llfs/ref.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_parse.hpp>
#include <llfs/volume_event_visitor.hpp>

#include <map>

namespace llfs {

using VolumePendingJobsMap =
    std::map<slot_offset_type, SlotParseWithPayload<Ref<const PackedPrepareJob>>, SlotLess>;

}  //namespace llfs

#endif  // LLFS_VOLUME_PENDING_JOBS_MAP_HPP
