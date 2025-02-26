//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring_file_runtime_options.hpp>
//

#include <llfs/filesystem.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ IoRingFileRuntimeOptions IoRingFileRuntimeOptions::with_default_values(
    const IoRing& io_ring)
{
  return IoRingFileRuntimeOptions{
      .io_ring = io_ring,
      .use_raw_io = true,
      .allow_read = true,
      .allow_write = true,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<IoRing::File> open_ioring_file(const std::string& file_name,
                                        const IoRingFileRuntimeOptions& file_options)
{
  int flags = 0;
  {
    if (file_options.use_raw_io) {
      flags |= O_DIRECT | O_SYNC;
    }
    if (file_options.allow_read) {
      if (file_options.allow_write) {
        flags |= O_RDWR;
      } else {
        flags |= O_RDONLY;
      }
    } else {
      if (file_options.allow_write) {
        flags |= O_WRONLY;
      } else {
        return {batt::StatusCode::kInvalidArgument};
      }
    }
  }

  int fd = batt::syscall_retry([&] {
    return system_open2(file_name.c_str(), flags);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return IoRing::File{file_options.io_ring, fd};
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
