//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_LOG_CONTROL_BLOCK2_HPP
#define LLFS_PACKED_LOG_CONTROL_BLOCK2_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/packed_slot_offset.hpp>

#include <array>

#include <batteries/static_assert.hpp>

namespace llfs {

struct PackedLogControlBlock2 {
  /** \brief The control block structure should take up exactly kDirectIOBlockSize, since that is
   * the smallest atomic block size for the kinds of devices we care about.
   */
  static constexpr usize kSize = kDirectIOBlockSize;

  /** \brief Used to sanity check instances of this packed structure read from media.
   */
  static constexpr u64 kMagic = 0x128095f84cfba8b0ull;

  /** \brief Must always be set to PackedLogControlBlock2::kMagic.
   */
  big_u64 magic;

  /** \brief The total capacity in bytes of the log.
   */
  little_i64 data_size;

  /** \brief The current trim (logical) offset in bytes from the beginning of the log.
   */
  PackedSlotOffset trim_pos;

  /** \brief The current flush (logical) offset in bytes from the beginning of the log.
   */
  PackedSlotOffset flush_pos;

  /** \brief The number of times this control block has been updated.
   */
  little_u64 generation;

  /** \brief The size in bytes of the control block in storage.  The data portion of the log always
   * starts at the offset of this structure plus this->control_block_size.
   */
  little_u32 control_block_size;

  /** \brief The size of this structure.  This may be smaller than this->control_block_size; the
   * difference is padding so that the data region will be correctly aligned (see
   * this->data_alignment_log2).
   */
  little_u32 control_header_size;

  /** \brief The size of the storage media page, log2.
   */
  little_i16 device_page_size_log2;

  /** \brief The number of bits to which reads/writes within the log data region must be aligned.
   */
  little_i16 data_alignment_log2;

  /** \brief Padding; reserved for future use.
   */
  std::array<u8, (kSize - (52 /*== the offset of this field*/))> reserved_;
};

// Verify that the struct is the desired size.
//
BATT_STATIC_ASSERT_EQ(sizeof(PackedLogControlBlock2), PackedLogControlBlock2::kSize);

}  //namespace llfs

#endif  // LLFS_PACKED_LOG_CONTROL_BLOCK2_HPP
