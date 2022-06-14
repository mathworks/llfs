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
