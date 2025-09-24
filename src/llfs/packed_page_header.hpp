//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_PAGE_HEADER_HPP
#define LLFS_PACKED_PAGE_HEADER_HPP

#include <llfs/int_types.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/packed_page_user_slot.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_size.hpp>

#include <batteries/assert.hpp>

#include <ostream>

namespace llfs {

struct PackedPageHeader;

std::ostream& operator<<(std::ostream& out, const PackedPageHeader& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageHeader {
  static constexpr u64 kMagic = 0x35f2e78c6a06fc2bull;
  static constexpr u32 kCrc32NotSet = 0xdeadcc32ul;

  u32 unused_size() const noexcept
  {
    BATT_CHECK_LE(this->unused_begin, this->unused_end) << *this;
    return this->unused_end - this->unused_begin;
  }

  usize used_size() const noexcept
  {
    return this->size - this->unused_size();
  }

  Status sanity_check(PageSize page_size, PageId page_id,
                      const PageIdFactory& id_factory) const noexcept;

  big_u64 magic;
  PackedPageId page_id;
  PageLayoutId layout_id;
  little_u32 crc32;
  little_u32 unused_begin;
  little_u32 unused_end;
  PackedPageUserSlot user_slot_DEPRECATED;
  little_u32 size;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageHeader), 64);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//

inline const PackedPageHeader& get_page_header(const PageBuffer& page)
{
  return *reinterpret_cast<const PackedPageHeader*>(&page);
}

inline PackedPageHeader* mutable_page_header(PageBuffer* page)
{
  return reinterpret_cast<PackedPageHeader*>(page);
}

/** \brief If required_layout is non-None, returns OkStatus() iff the layout_id field in the header
 * of the passed `page` matches the required_layout; if required_layout is None, always returns
 * OkStatus().
 *
 * When the status does not match, returns llfs::StatusCode::kPageHeaderBadLayoutId.
 */
Status require_page_layout(const PageBuffer& page,
                           const Optional<PageLayoutId>& required_layout) noexcept;

}  // namespace llfs

#endif  // LLFS_PACKED_PAGE_HEADER_HPP
