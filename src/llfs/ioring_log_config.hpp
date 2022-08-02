//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_CONFIG_HPP
#define LLFS_IORING_LOG_CONFIG_HPP

#include <llfs/config.hpp>
#include <llfs/constants.hpp>
#include <llfs/file_offset_ptr.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_log_page_header.hpp>

namespace llfs {

struct PackedLogDeviceConfig;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Physical configuration of an IoRing-based LogDevice.
//
struct IoRingLogConfig {
  static constexpr usize kDefaultBlockSize = 16 * kKiB;
  static constexpr usize kDefaultPagesPerBlockLog2 = 5;

  static_assert((1ull << (kDefaultPagesPerBlockLog2 + kLogAtomicWriteSizeLog2)) ==
                    kDefaultBlockSize,
                "");

  // The in-memory (logical) size in bytes of the log.
  //
  u64 logical_size;

  // The offset in bytes of the log from the beginning of the file.
  //
  i64 physical_offset;

  // The physical size in bytes of the log.
  //
  u64 physical_size;

  // Specifies the size of a "flush block," the size of a single write operation while flushing,
  // in number of 4kb pages, log2.  For example:
  //
  //  | flush_block_pages_log2   | flush write buffer size   |
  //  |--------------------------|---------------------------|
  //  | 0                        | 512                       |
  //  | 1                        | 1kb                       |
  //  | 2                        | 2kb                       |
  //  | 3                        | 4kb                       |
  //  | 4                        | 8kb                       |
  //  | 5                        | 16kb                      |
  //  | 6                        | 32kb                      |
  //  | 7                        | 64kb                      |
  //
  usize pages_per_block_log2 = kDefaultPagesPerBlockLog2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static IoRingLogConfig from_packed(
      const FileOffsetPtr<const PackedLogDeviceConfig&>& packed_config);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize pages_per_block() const
  {
    return usize{1} << this->pages_per_block_log2;
  }

  usize block_size() const
  {
    return kLogPageSize * this->pages_per_block();
  }

  usize block_capacity() const
  {
    return this->block_size() - sizeof(PackedLogPageHeader);
  }

  usize block_count() const
  {
    BATT_CHECK_EQ(this->physical_size % this->block_size(), 0u)
        << "The physical size of the log must be a multiple of the block size!"
        << BATT_INSPECT(this->physical_size) << BATT_INSPECT(this->block_size())
        << BATT_INSPECT(this->pages_per_block_log2);

    return this->physical_size / this->block_size();
  }
};

}  // namespace llfs

#endif  // LLFS_IORING_LOG_CONFIG_HPP
