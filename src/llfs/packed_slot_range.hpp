//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_SLOT_RANGE_HPP
#define LLFS_PACKED_SLOT_RANGE_HPP

#include <llfs/data_packer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/packed_interval.hpp>
#include <llfs/packed_page_user_slot.hpp>
#include <llfs/slot.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/static_assert.hpp>

#include <type_traits>

namespace llfs {

using PackedSlotRange = PackedInterval<PackedSlotOffset>;

BATT_STATIC_ASSERT_EQ(sizeof(PackedSlotRange), 16);

}  // namespace llfs

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

static_assert(std::is_same_v<llfs::PackedTypeFor<llfs::SlotRange>, llfs::PackedSlotRange>, "");

#endif  // LLFS_PACKED_SLOT_RANGE_HPP
