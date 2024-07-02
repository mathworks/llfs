//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_runtime_options.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ VolumeRuntimeOptions VolumeRuntimeOptions::with_default_values()
{
  return VolumeRuntimeOptions{
      .slot_visitor_fn = [](const SlotParse& /*slot*/, std::string_view /*user_data*/) -> Status {
        return OkStatus();
      },
      .root_log_options = LogDeviceRuntimeOptions::with_default_values(),
      .recycler_log_options = LogDeviceRuntimeOptions::with_default_values(),
      .trim_control = nullptr,
  };
}

}  // namespace llfs
