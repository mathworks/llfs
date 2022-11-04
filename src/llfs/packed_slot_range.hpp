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
#include <llfs/packed_page_user_slot.hpp>
#include <llfs/slot.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/static_assert.hpp>

#include <type_traits>

namespace llfs {

struct PackedSlotRange {
  PackedSlotOffset lower_bound;
  PackedSlotOffset upper_bound;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedSlotRange), 16);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize packed_sizeof(const PackedSlotRange&)
{
  return sizeof(PackedSlotRange);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize packed_sizeof(const SlotRange&)
{
  return sizeof(PackedSlotRange);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedSlotRange* pack_object_to(const SlotRange& object, PackedSlotRange* packed_object,
                                DataPacker* packer);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedSlotRange& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline batt::Status validate_packed_value(const llfs::PackedSlotRange& packed,
                                          const void* buffer_data, std::size_t buffer_size)
{
  return validate_packed_struct(packed, buffer_data, buffer_size);
}

}  // namespace llfs

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace batt {

LLFS_DEFINE_PACKED_TYPE_FOR(::llfs::SlotRange, ::llfs::PackedSlotRange);

}  // namespace batt

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

static_assert(std::is_same_v<llfs::PackedTypeFor<llfs::SlotRange>, llfs::PackedSlotRange>, "");

#endif  // LLFS_PACKED_SLOT_RANGE_HPP
