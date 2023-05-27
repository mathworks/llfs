//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_arena_file.hpp>
//

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/crc.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/ioring_log_device.hpp>
#include <llfs/ioring_page_file_device.hpp>
#include <llfs/ring_buffer.hpp>
#include <llfs/status.hpp>
#include <llfs/system_config.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/math.hpp>

#include <boost/uuid/random_generator.hpp>

#include <cstdlib>

#include <unistd.h>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto driver_config_from_arena_config(const FileOffsetPtr<PackedPageArenaConfig>& /*config*/)
{
  return;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> open_page_device_file_impl(
    batt::TaskScheduler& /*scheduler*/, IoRing& /*ioring*/, const FileSegmentRef& /*file_ref*/,
    const PageArenaFileRuntimeOptions& /*options*/, PackedPageArenaConfig* /*config*/,
    bool /*write_config*/, ConfirmThisWillEraseAllMyData /*confirm*/)
{
  //TODO [tastolfi 2023-05-22] implement me!
  return {batt::StatusCode::kUnimplemented};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> page_arena_from_file(batt::TaskScheduler& scheduler, IoRing& ioring,
                                         const FileSegmentRef& file_ref,
                                         const PageArenaFileRuntimeOptions& options,
                                         PackedPageArenaConfig* config_out)
{
  return open_page_device_file_impl(scheduler, ioring, file_ref, options, config_out,
                                    /*write_config=*/
                                    false, ConfirmThisWillEraseAllMyData::kNo);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> initialize_page_arena_file(batt::TaskScheduler& scheduler, IoRing& ioring,
                                               const FileSegmentRef& file_ref,
                                               const PageArenaFileRuntimeOptions& options,
                                               const PackedPageArenaConfig& /*config*/,
                                               ConfirmThisWillEraseAllMyData confirm)
{
  if (confirm != ConfirmThisWillEraseAllMyData::kYes) {
    return Status{batt::StatusCode::kInvalidArgument};  // TODO [tastolfi 2021-10-20] "operation not
                                                        // confirmed!"
  }

  //  auto config_copy = config;
  return open_page_device_file_impl(scheduler, ioring, file_ref, options, /*&config_copy*/ nullptr,
                                    /*write_config=*/true, confirm);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::vector<std::unique_ptr<PageArenaConfigInFile>>> read_arena_configs_from_file(
    const std::string& /*filename*/)
{
  //TODO [tastolfi 2023-05-22] implement me!
  return {batt::StatusCode::kUnimplemented};
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
