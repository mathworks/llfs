#pragma once
#ifndef LLFS_PAGE_DEVICE_ALLOCATOR_EVENTS_HPP
#define LLFS_PAGE_DEVICE_ALLOCATOR_EVENTS_HPP

#include <llfs/array_packer.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/page_layout.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_writer.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace llfs {

struct PackedPageAllocatorAttach;
struct PackedPageAllocatorDetach;
struct PackedPageAllocatorTxn;

using PackedPageAllocatorEvent = PackedVariant<  //
    PackedPageAllocatorAttach,                   //
    PackedPageAllocatorDetach,                   //
    PackedPageRefCount,                          //
    PackedPageAllocatorTxn                       //
    >;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageAllocatorAttach {
  PackedPageUserSlot user_slot;
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
struct PackedPageAllocatorTxn {
  PackedPageUserSlot user_slot;
  PackedArray<PackedPageRefCount> ref_counts;
};

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorTxn& t);

LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageAllocatorTxn, PackedPageAllocatorTxn);

template <typename Dst>
bool pack_object_to(const PackedPageAllocatorTxn& from, PackedPageAllocatorTxn* to, Dst* dst)
{
  to->user_slot = from.user_slot;
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
  return sizeof(PackedPageUserSlot) + packed_array_size<PackedPageRefCount>(n_ref_counts);
}

inline usize packed_sizeof(const PackedPageAllocatorTxn& txn)
{
  return packed_sizeof_page_allocator_txn(txn.ref_counts.item_count);
}

inline usize packed_sizeof_checkpoint(const PackedPageAllocatorTxn& txn)
{
  static const PackedPageRefCount packed_ref_count{
      .page_id = 0,
      .ref_count = 0,
  };
  static const PackedPageAllocatorAttach packed_attachment{.user_slot = {
                                                               .user_id = {},
                                                               .slot_offset = 0,
                                                           }};
  return packed_sizeof_slot(packed_attachment) +
         packed_sizeof_slot(packed_ref_count) * txn.ref_counts.size();
}

}  // namespace llfs

#endif  // LLFS_PAGE_DEVICE_ALLOCATOR_EVENTS_HPP
