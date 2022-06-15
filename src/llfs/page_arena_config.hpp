//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ARENA_CONFIG_HPP
#define LLFS_PAGE_ARENA_CONFIG_HPP

#include <llfs/int_types.hpp>
#include <llfs/ioring_log_flush_op.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_allocator_config.hpp>
#include <llfs/page_device_config.hpp>
#include <llfs/status.hpp>

#include <boost/uuid/uuid.hpp>

namespace llfs {

struct PageArenaConfigOptions {
  PageCount page_count;
  u16 page_size_bits;
  Optional<u16> log_block_size_bits;
  Optional<u64> device_id;
  Optional<boost::uuids::uuid> device_uuid;
  u64 max_attachments;

  u64 page_size() const noexcept
  {
    return u64{1} << this->page_size_bits;
  }

  void page_size(u64 n)
  {
    this->page_size_bits = batt::log2_ceil(n);
    BATT_CHECK_EQ(this->page_size(), n);
  }
};

struct PackedPageArenaConfig : PackedConfigSlotHeader {
  static constexpr usize kSize = 64;

  // byte 0 +++++++++++-+-+--+----- --- -- -  -  -   -

  // The PageDevice config.
  //
  boost::uuids::uuid page_device_uuid;

  // The PageAllocator config.
  //
  boost::uuids::uuid page_allocator_uuid;

  // Must be zero for now.
  //
  little_u8 reserved_[12];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageArenaConfig), PackedPageArenaConfig::kSize);
BATT_STATIC_ASSERT_EQ(PackedPageArenaConfig::kSize, PackedConfigSlot::kSize);

std::ostream& operator<<(std::ostream& out, const PackedPageArenaConfig& t);

}  // namespace llfs

#endif  // LLFS_PAGE_ARENA_CONFIG_HPP
