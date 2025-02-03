//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_RECYCLER_EVENTS_HPP
#define LLFS_PAGE_RECYCLER_EVENTS_HPP

#include <llfs/config.hpp>
//
#include <llfs/data_layout.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/packed_uuid.hpp>
#include <llfs/page_layout.hpp>
#include <llfs/slot.hpp>

#include <batteries/static_assert.hpp>

namespace llfs {

struct PageToRecycle {
  // Which page to recycle.
  //
  PageId page_id;

  // The highest slot at which this page was refreshed.
  //
  Optional<slot_offset_type> refresh_slot;

  // The slot where this page is recycled.
  //
  Optional<slot_offset_type> batch_slot;

  // The page reference depth at which this page was discovered to be dead.
  //
  i32 depth;

  // The offset given by volume trimmer.
  //
  slot_offset_type offset_as_unique_identifier;

  // This tracks the page index within recycle_pages request.
  //
  u16 page_index;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static PageToRecycle make_invalid()
  {
    return PageToRecycle{
        .page_id = PageId{kInvalidPageId},
        .refresh_slot = None,
        .batch_slot = None,
        .depth = 0,
        .offset_as_unique_identifier = 0,
        .page_index = 0,
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool is_valid() const
  {
    return this->page_id.is_valid();
  }

  explicit operator bool() const
  {
    return this->is_valid();
  }
};

inline slot_offset_type get_slot_offset(const PageToRecycle& to_recycle)
{
  BATT_CHECK(to_recycle.refresh_slot);
  return *to_recycle.refresh_slot;
}

std::ostream& operator<<(std::ostream& out, const PageToRecycle& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

class PageRecyclerOptions;
struct PackedPageToRecycle;
struct PackedRecycleBatchCommit;
struct PackedPageRecyclerInfo;

using PageRecycleEvent = PackedVariant<  //
    PackedPageToRecycle,                 //
    PackedRecycleBatchCommit,            //
    PackedPageRecyclerInfo               //
    >;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedPageRecyclerInfo {
  PackedUUID uuid;
  little_u32 info_refresh_rate;
  little_u32 batch_size;
  little_u32 refresh_factor;
  little_u32 max_page_ref_depth;
  little_u32 max_refs_per_page;

  static PackedPageRecyclerInfo from(const boost::uuids::uuid& uuid,
                                     const PageRecyclerOptions& options);
};

BATT_STATIC_ASSERT_EQ(36, sizeof(PackedPageRecyclerInfo));

inline usize packed_sizeof(const PackedPageRecyclerInfo&)
{
  return sizeof(PackedPageRecyclerInfo);
}

LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageRecyclerInfo, PackedPageRecyclerInfo);

template <typename Dst>
inline bool pack_object_to(const PackedPageRecyclerInfo& from, PackedPageRecyclerInfo* to, Dst*)
{
  *to = from;
  return true;
}

inline StatusOr<PackedPageRecyclerInfo> unpack_object(const PackedPageRecyclerInfo& info,
                                                      DataReader*)
{
  return info;
}

std::ostream& operator<<(std::ostream& out, const PackedPageRecyclerInfo& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedPageToRecycle {
  enum Flags : u8 {
    kHasBatchSlot = 0x01,
  };

  little_page_id_int page_id;
  little_u64 batch_slot;
  u64 offset_as_unique_identifier;
  little_i32 depth;
  u16 page_index;
  u8 flags;
  u8 reserved_[1];
};

BATT_STATIC_ASSERT_EQ(32, sizeof(PackedPageToRecycle));

inline std::size_t packed_sizeof(const PackedPageToRecycle&)
{
  return sizeof(PackedPageToRecycle);
}

inline std::size_t packed_sizeof(const PageToRecycle&)
{
  return sizeof(PackedPageToRecycle);
}

LLFS_DEFINE_PACKED_TYPE_FOR(PageToRecycle, PackedPageToRecycle);

template <typename Dst>
inline bool pack_object_to(const PageToRecycle& from, PackedPageToRecycle* to, Dst*)
{
  to->page_id = from.page_id.int_value();
  to->depth = from.depth;
  to->flags = 0;
  std::memset(&to->reserved_, 0, sizeof(PackedPageToRecycle::reserved_));
  if (from.batch_slot) {
    to->flags |= PackedPageToRecycle::kHasBatchSlot;
    to->batch_slot = *from.batch_slot;
  } else {
    to->batch_slot = 0;
  }
  to->offset_as_unique_identifier = from.offset_as_unique_identifier;
  to->page_index = from.page_index;
  return true;
}

inline StatusOr<PageToRecycle> unpack_object(const PackedPageToRecycle& packed, DataReader*)
{
  return PageToRecycle{
      .page_id = PageId{packed.page_id.value()},
      .refresh_slot = None,
      .batch_slot = [&]() -> Optional<slot_offset_type> {
        if (packed.flags & PackedPageToRecycle::kHasBatchSlot) {
          return packed.batch_slot;
        }
        return None;
      }(),
      .depth = packed.depth,
      .offset_as_unique_identifier = packed.offset_as_unique_identifier,
      .page_index = packed.page_index,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedRecycleBatchCommit {
  little_u64 batch_slot;
};

BATT_STATIC_ASSERT_EQ(8, sizeof(PackedRecycleBatchCommit));

inline std::size_t packed_sizeof(const PackedRecycleBatchCommit&)
{
  return sizeof(PackedRecycleBatchCommit);
}

LLFS_DEFINE_PACKED_TYPE_FOR(PackedRecycleBatchCommit, PackedRecycleBatchCommit);

template <typename Dst>
inline bool pack_object_to(const PackedRecycleBatchCommit& from, PackedRecycleBatchCommit* to, Dst*)
{
  *to = from;
  return true;
}

inline StatusOr<PackedRecycleBatchCommit> unpack_object(const PackedRecycleBatchCommit& removed,
                                                        DataReader*)
{
  return removed;
}

std::ostream& operator<<(std::ostream& out, const PackedRecycleBatchCommit& t);

inline slot_offset_type get_slot_offset(const PackedRecycleBatchCommit& commit)
{
  return commit.batch_slot.value();
}

}  // namespace llfs

#endif  // LLFS_PAGE_RECYCLER_EVENTS_HPP
