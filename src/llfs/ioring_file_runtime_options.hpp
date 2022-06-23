//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_FILE_RUNTIME_OPTIONS_HPP
#define LLFS_IORING_FILE_RUNTIME_OPTIONS_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring_file.hpp>

namespace llfs {

struct IoRingFileRuntimeOptions {
  static IoRingFileRuntimeOptions with_default_values(IoRing& io);

  IoRing& io;
  bool use_raw_io;
  bool allow_read;
  bool allow_write;
};

StatusOr<IoRing::File> open_ioring_file(const std::string& file_name,
                                        const IoRingFileRuntimeOptions& file_options);

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_FILE_RUNTIME_OPTIONS_HPP
