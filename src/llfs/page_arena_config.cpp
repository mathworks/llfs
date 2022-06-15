//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_arena_config.hpp>
//

#include <llfs/crc.hpp>
#include <llfs/ioring_log_device.hpp>
#include <llfs/page_allocator.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

BATT_PRINT_OBJECT_IMPL(PackedPageArenaConfig,
                       (tag)                  //
                       (page_device_uuid)     //
                       (page_allocator_uuid)  //
)

}  // namespace llfs
