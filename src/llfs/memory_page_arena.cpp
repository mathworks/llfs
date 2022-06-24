//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/memory_page_arena.hpp>
//

#include <llfs/config.hpp>
#include <llfs/memory_log_device.hpp>
#include <llfs/memory_page_device.hpp>

namespace llfs {

PageArena make_memory_page_arena(batt::TaskScheduler& scheduler, isize n_pages, PageSize page_size,
                                 std::string&& name, page_device_id_int device_id)
{
  const auto log_size = PageAllocator::calculate_log_size(n_pages, kDefaultMaxPoolAttachments);

  return PageArena{
      std::make_unique<MemoryPageDevice>(device_id, n_pages, page_size),
      PageAllocator::recover_or_die(PageAllocatorRuntimeOptions{scheduler, name},
                                    PageIdFactory{n_pages, device_id},
                                    *std::make_unique<MemoryLogDeviceFactory>(log_size))};
}

}  // namespace llfs
