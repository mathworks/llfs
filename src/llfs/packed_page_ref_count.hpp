//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_PAGE_REF_COUNT_HPP
#define LLFS_PACKED_PAGE_REF_COUNT_HPP

#include <llfs/config.hpp>
//
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_ref_count.hpp>

namespace llfs {

struct PackedPageRefCount {
  little_page_id_int page_id;
  little_i32 ref_count;

  PageRefCount as_page_ref_count() const
  {
    return PageRefCount{
        .page_id = PageId{page_id},
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
  packed->page_id = prc.page_id.int_value();
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

#endif  // LLFS_PACKED_PAGE_REF_COUNT_HPP
