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

#include <llfs/data_layout.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/page_layout.hpp>
#include <llfs/slot.hpp>

#include <batteries/static_assert.hpp>

namespace llfs {

struct PageToRecycle {
  // Which page to recycle.
  //
  PageId page_id;
  slot_offset_type slot_offset;
  i32 depth;

  bool is_valid() const
  {
    return this->page_id.is_valid();
  }

  explicit operator bool() const
  {
    return this->is_valid();
  }

  static PageToRecycle make_invalid()
  {
    return PageToRecycle{
        .page_id = PageId{kInvalidPageId},
        .slot_offset = 0,
        .depth = 0,
    };
  }
};

inline slot_offset_type get_slot_offset(const PageToRecycle& to_recycle)
{
  return to_recycle.slot_offset;
}

std::ostream& operator<<(std::ostream& out, const PageToRecycle& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PageRecyclerOptions;
struct PackedRecyclePageInserted;
struct PackedRecyclePagePrepare;
struct PackedRecycleBatchCommit;
struct PackedPageRecyclerInfo;

using PageRecycleEvent = PackedVariant<  //
    PackedRecyclePageInserted,           //
    PackedRecyclePagePrepare,            //
    PackedRecycleBatchCommit,            //
    PackedPageRecyclerInfo               //
    >;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedPageRecyclerInfo {
  boost::uuids::uuid uuid;
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

struct PackedRecyclePageInserted {
  little_page_id_int page_id;
  little_u64 slot_offset;
  little_i32 depth;
  u8 reserved_[4];
};

BATT_STATIC_ASSERT_EQ(24, sizeof(PackedRecyclePageInserted));

inline std::size_t packed_sizeof(const PackedRecyclePageInserted&)
{
  return sizeof(PackedRecyclePageInserted);
}

inline std::size_t packed_sizeof(const PageToRecycle&)
{
  return sizeof(PackedRecyclePageInserted);
}

LLFS_DEFINE_PACKED_TYPE_FOR(PageToRecycle, PackedRecyclePageInserted);

template <typename Dst>
inline bool pack_object_to(const PageToRecycle& from, PackedRecyclePageInserted* to, Dst*)
{
  to->page_id = from.page_id.int_value();
  to->slot_offset = from.slot_offset;
  to->depth = from.depth;
  return true;
}

inline StatusOr<PageToRecycle> unpack_object(const PackedRecyclePageInserted& inserted, DataReader*)
{
  return PageToRecycle{
      .page_id = PageId{inserted.page_id.value()},
      .slot_offset = inserted.slot_offset,
      .depth = inserted.depth,
  };
}

inline slot_offset_type get_slot_offset(const PackedRecyclePageInserted& inserted)
{
  return inserted.slot_offset.value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedRecyclePagePrepare {
  little_page_id_int page_id;
  little_u64 batch_slot;
};

BATT_STATIC_ASSERT_EQ(16, sizeof(PackedRecyclePagePrepare));

inline std::size_t packed_sizeof(const PackedRecyclePagePrepare&)
{
  return sizeof(PackedRecyclePagePrepare);
}

LLFS_DEFINE_PACKED_TYPE_FOR(PackedRecyclePagePrepare, PackedRecyclePagePrepare);

template <typename Dst>
inline bool pack_object_to(const PackedRecyclePagePrepare& from, PackedRecyclePagePrepare* to, Dst*)
{
  *to = from;
  return true;
}

inline StatusOr<PackedRecyclePagePrepare> unpack_object(const PackedRecyclePagePrepare& removed,
                                                        DataReader*)
{
  return removed;
}

std::ostream& operator<<(std::ostream& out, const PackedRecyclePagePrepare& t);

inline slot_offset_type get_slot_offset(const PackedRecyclePagePrepare& prepare)
{
  return prepare.batch_slot.value();
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
