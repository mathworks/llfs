//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_PAGE_ID_HPP
#define LLFS_PACKED_PAGE_ID_HPP

#include <llfs/buffer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/page_id.hpp>
#include <llfs/seq.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/static_assert.hpp>

#include <ostream>

namespace llfs {

using little_page_id_int = little_u64;
using big_page_id_int = big_u64;

struct PackedPageId {
  static PackedPageId from(const PageId& id)
  {
    return PackedPageId{
        .id_val = id.int_value(),
    };
  }

  little_page_id_int id_val;

  PageId as_page_id() const
  {
    return PageId{id_val.value()};
  }

  auto debug_dump(const void* base) const
  {
    return [base, this](std::ostream& out) {
      out << "[" << byte_distance(base, this) << ".." << byte_distance(base, this + 1)
          << "] PackedPageId{.id_val=" << std::hex << this->id_val << std::dec << ",}";
    };
  }
};

inline std::ostream& operator<<(std::ostream& out, const PackedPageId& t)
{
  return out << t.as_page_id();
}

inline PageId get_page_id(const PackedPageId& packed_page_id)
{
  return packed_page_id.as_page_id();
}

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageId), 8);

LLFS_DEFINE_PACKED_TYPE_FOR(PageId, PackedPageId);

BoxedSeq<PageId> trace_refs(const PackedArray<PackedPageId>& packed);

BoxedSeq<PageId> trace_refs(const BoxedSeq<PageId>& page_ids);

template <typename Dst>
[[nodiscard]] PackedPageId* pack_object_to(const PackedPageId& from_id, PackedPageId* to_id, Dst*)
{
  *to_id = from_id;
  return to_id;
}

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

inline batt::Status validate_packed_value(const PackedPageId& packed, const void* buffer_data,
                                          usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));

  return batt::OkStatus();
}

}  // namespace llfs

#endif  // LLFS_PACKED_PAGE_ID_HPP
