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

  /** \brief The index of the next element of this->commit_points to be overwritten.
   */
  little_u32 next_commit_i;

  /** \brief A rolling history of known commit points; this is updated each time the control block
   * is updated.  It is used to speed up log recovery, especially in the case where the log device
   * was cleanly shut down.
   */
  std::array<little_u64, (512 - 56) / sizeof(little_u64)> commit_points;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLogControlBlock2), 512);

}  //namespace llfs

#endif  // LLFS_PACKED_LOG_CONTROL_BLOCK2_HPP
