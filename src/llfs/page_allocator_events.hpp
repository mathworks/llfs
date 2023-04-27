//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_EVENTS_HPP
#define LLFS_PAGE_ALLOCATOR_EVENTS_HPP

#include <llfs/config.hpp>
//
#include <llfs/array_packer.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/packed_page_ref_count.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_writer.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace llfs {

struct PackedPageAllocatorAttach;
struct PackedPageAllocatorDetach;
struct PackedPageAllocatorTxn;
struct PackedPageRefCountRefresh;

using PackedPageAllocatorEvent = PackedVariant<  //
    PackedPageAllocatorAttach,                   //
    PackedPageAllocatorDetach,                   //
    PackedPageRefCountRefresh,                   //
    PackedPageAllocatorTxn                       //
    >;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageAllocatorAttach {
  PackedPageUserSlot user_slot;
  little_u32 user_index;
};

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorAttach& t);

LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageAllocatorAttach, PackedPageAllocatorAttach);

inline usize packed_sizeof(const PackedPageAllocatorAttach&)
{
  return packed_sizeof(batt::StaticType<PackedPageAllocatorAttach>{});
}

inline usize packed_sizeof_checkpoint(const PackedPageAllocatorAttach& obj)
{
  return packed_sizeof_slot(obj);
}

template <typename Dst>
bool pack_object_to(const PackedPageAllocatorAttach& from, PackedPageAllocatorAttach* to, Dst*)
{
  *to = from;
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageAllocatorDetach {
  PackedPageUserSlot user_slot;
};

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorDetach& t);

LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageAllocatorDetach, PackedPageAllocatorDetach);

inline usize packed_sizeof(const PackedPageAllocatorDetach&)
{
  return packed_sizeof(batt::StaticType<PackedPageAllocatorDetach>{});
}

inline usize packed_sizeof_checkpoint(const PackedPageAllocatorDetach& obj)
{
  return packed_sizeof_slot(obj);
}

template <typename Dst>
bool pack_object_to(const PackedPageAllocatorDetach& from, PackedPageAllocatorDetach* to, Dst*)
{
  *to = from;
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageRefCountRefresh : PackedPageRefCount {
  little_u32 user_index;
};

std::ostream& operator<<(std::ostream& out, const PackedPageRefCountRefresh& t);

LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageRefCountRefresh, PackedPageRefCountRefresh);

inline usize packed_sizeof(const PackedPageRefCountRefresh&)
{
  return packed_sizeof(batt::StaticType<PackedPageRefCountRefresh>{});
}

inline usize packed_sizeof_checkpoint(const PackedPageRefCountRefresh& obj)
{
  return packed_sizeof_slot(obj);
}

template <typename Dst>
bool pack_object_to(const PackedPageRefCountRefresh& from, PackedPageRefCountRefresh* to, Dst*)
{
  *to = from;
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageAllocatorTxn {
  PackedPageUserSlot user_slot;
  little_u32 user_index;
  PackedArray<PackedPageRefCount> ref_counts;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageAllocatorTxn), sizeof(PackedPageUserSlot) +
                                                          sizeof(little_u32) +
                                                          sizeof(PackedArray<PackedPageRefCount>));

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorTxn& t);

LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageAllocatorTxn, PackedPageAllocatorTxn);

template <typename Dst>
bool pack_object_to(const PackedPageAllocatorTxn& from, PackedPageAllocatorTxn* to, Dst* dst)
{
  to->user_slot = from.user_slot;
  to->user_index = from.user_index;
  to->ref_counts.initialize(0u);

  BasicArrayPacker<PackedPageRefCount, Dst> array_packer{&to->ref_counts, dst};
  for (const PackedPageRefCount& item : from.ref_counts) {
    if (!array_packer.pack_item(item)) {
      return false;
    }
  }
  return true;
}

inline usize packed_sizeof_page_allocator_txn(usize n_ref_counts)
{
  return sizeof(PackedPageUserSlot) + sizeof(little_u32) +
         packed_array_size<PackedPageRefCount>(n_ref_counts);
}

inline usize packed_sizeof(const PackedPageAllocatorTxn& txn)
{
  return packed_sizeof_page_allocator_txn(txn.ref_counts.item_count);
}

inline usize packed_sizeof_checkpoint(const PackedPageAllocatorTxn& txn)
{
  static const PackedPageRefCountRefresh packed_ref_count{
      {
          .page_id = {0},
          .ref_count = 0,
      },
      .user_index = 0,
  };
  static const PackedPageAllocatorAttach packed_attachment{
      .user_slot =
          {
              .user_id = {},
              .slot_offset = 0,
          },
      .user_index = 0,
  };
  return packed_sizeof_slot(packed_attachment) +
         packed_sizeof_slot(packed_ref_count) * txn.ref_counts.size();
}

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_EVENTS_HPP
