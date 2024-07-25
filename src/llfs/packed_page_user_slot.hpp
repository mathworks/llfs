//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_PAGE_USER_SLOT_HPP
#define LLFS_PACKED_PAGE_USER_SLOT_HPP

#include <llfs/int_types.hpp>
#include <llfs/packed_slot_offset.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>

#include <ostream>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Represents a user-specified logical timestamp for a page.  The `user` here isn't necessarily an
// end-user or human, it could be another part of the system (e.g., a Tablet).
//
struct PackedPageUserSlot {
  boost::uuids::uuid user_id;
  PackedSlotOffset slot_offset;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageUserSlot), 24);

std::ostream& operator<<(std::ostream& out, const PackedPageUserSlot& t);

}  // namespace llfs

#endif  // LLFS_PACKED_PAGE_USER_SLOT_HPP
