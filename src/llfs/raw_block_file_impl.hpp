//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

// Contains various implementations of abstract class RawBlockFile.

#pragma once
#ifndef LLFS_RAW_BLOCK_FILE_IMPL_HPP
#define LLFS_RAW_BLOCK_FILE_IMPL_HPP

#include <llfs/ioring_file.hpp>
#include <llfs/optional.hpp>
#include <llfs/raw_block_file.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace llfs {

class IoRingRawBlockFile : public RawBlockFile
{
 public:
  static StatusOr<std::unique_ptr<IoRingRawBlockFile>> open(const IoRing& io_ring,
                                                            const char* file_name, int flags,
                                                            Optional<mode_t> mode);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingRawBlockFile(IoRing::File&& file) noexcept;

  StatusOr<i64> write_some(i64 offset, const ConstBuffer& data) override;

  StatusOr<i64> read_some(i64 offset, const MutableBuffer& buffer) override;

  StatusOr<i64> get_size() override;

  Status truncate(i64 /*new_offset_upper_bound*/) override;

  Status truncate_at_least(i64 /*minimum_size*/) override;

  Status close();

  IoRing::File* get_io_ring_file() override
  {
    return &file_;
  }

 private:
  IoRing::File file_;
};

}  // namespace llfs

#endif  // LLFS_RAW_BLOCK_FILE_IMPL_HPP
