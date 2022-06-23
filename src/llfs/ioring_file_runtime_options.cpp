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

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ IoRingFileRuntimeOptions IoRingFileRuntimeOptions::with_default_values(IoRing& io)
{
  return IoRingFileRuntimeOptions{
      .io = io,
      .use_raw_io = true,
      .allow_read = true,
      .allow_write = true,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<IoRing::File> open_ioring_file(const std::string& /*file_name*/,
                                        const IoRingFileRuntimeOptions& /*file_options*/)
{
  return {batt::StatusCode::kUnimplemented};
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
