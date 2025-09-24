//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_LOG_PAGE_BUFFER_HPP
#define LLFS_PACKED_LOG_PAGE_BUFFER_HPP

#include <llfs/config.hpp>
//
#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_log_page_header.hpp>

#include <batteries/static_assert.hpp>

#include <type_traits>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Aligned memory buffer that holds a single log page.
//
struct PackedLogPageBuffer {
  using AlignedUnit = std::aligned_storage_t<kDirectIOBlockSize, kDirectIOBlockAlign>;

  union {
    AlignedUnit aligned_storage;
    PackedLogPageHeader header;
  };

  void clear()
  {
    std::memset(this, 0, sizeof(PackedLogPageBuffer));
  }

  ConstBuffer as_const_buffer() const
  {
    return ConstBuffer{this, sizeof(PackedLogPageBuffer)};
  }

  MutableBuffer as_mutable_buffer()
  {
    return MutableBuffer{this, sizeof(PackedLogPageBuffer)};
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLogPageBuffer), kDirectIOBlockSize);

}  // namespace llfs

#endif  // LLFS_PACKED_LOG_PAGE_BUFFER_HPP
