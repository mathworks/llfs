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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

}  // namespace llfs

#endif  // LLFS_PAGE_LAYOUT_HPP
