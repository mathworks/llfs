//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_LAYOUT_HPP
#define LLFS_PAGE_LAYOUT_HPP

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Must be first
#include <llfs/logging.hpp>
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/buffer.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_page_header.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/packed_page_user_slot.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_ref_count.hpp>
#include <llfs/page_view.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/type_traits.hpp>

#include <cstddef>
#include <string_view>

namespace llfs {

// Offset is circular; thus the log can never be bigger than 2^63-1, but we
// will never need more bits.
//
using slot_offset_type = u64;

// Compute the crc64 for the given page.
//
u64 compute_page_crc64(const PageBuffer& page);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

// Finalize a page by computing its crc64 and user slot information.
//
Status finalize_page_header(PageBuffer* page,
                            const Interval<u64>& unused_region = Interval<u64>{0u, 0u});

template <typename Dst>
[[nodiscard]] PackedPageId* pack_object_to(const PageId& id, PackedPageId* packed_id, Dst*)
{
  packed_id->id_val = id.int_value();

  BATT_CHECK_EQ(packed_id->id_val, id.int_value());

  return packed_id;
}

template <typename Src>
inline StatusOr<PageId> unpack_object(const PackedPageId& packed_id, Src*)
{
  return PageId{packed_id.id_val};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PackedPageRefCount {
  little_page_id_int page_id;
  little_i32 ref_count;

  PageRefCount as_page_ref_count() const
  {
    return PageRefCount{
        .page_id = page_id,
        .ref_count = ref_count,
    };
  }
};

LLFS_DEFINE_PACKED_TYPE_FOR(PageRefCount, PackedPageRefCount);
LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageRefCount, PackedPageRefCount);

std::ostream& operator<<(std::ostream& out, const PackedPageRefCount& t);

inline usize packed_sizeof(const PackedPageRefCount&)
{
  return packed_sizeof(batt::StaticType<PackedPageRefCount>{});
}

template <typename Dst>
[[nodiscard]] bool pack_object_to(const PageRefCount& prc, PackedPageRefCount* packed, Dst*)
{
  packed->page_id = prc.page_id;
  packed->ref_count = prc.ref_count;
  return true;
}

template <typename Dst>
[[nodiscard]] bool pack_object_to(const PackedPageRefCount& from, PackedPageRefCount* to, Dst*)
{
  *to = from;
  return true;
}

}  // namespace llfs

#endif  // LLFS_PAGE_LAYOUT_HPP
