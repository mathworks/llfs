//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_device_entry.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageDeviceEntry::PageDeviceEntry(
    PageArena&& arena, boost::intrusive_ptr<PageCacheSlot::Pool>&& slot_pool) noexcept
    : arena{std::move(arena)}
    , can_alloc{true}
    , cache{this->arena.device().page_ids(), std::move(slot_pool)}
    , no_outgoing_refs_cache{this->arena.device().page_ids()}
    , page_size_log2{batt::log2_ceil(get_page_size(this->arena))}
    , is_sharded_view{false}
    , device_id_shifted{(this->arena.id() << kPageIdDeviceShift)}
    , sharded_views{}
{
  this->sharded_views.fill(nullptr);

  if (!this->arena.has_allocator()) {
    this->can_alloc = false;
  }

  //----- --- -- -  -  -   -
  // Sanity checks.
  //
  BATT_CHECK_EQ(PageSize{1} << this->page_size_log2, get_page_size(this->arena))
      << "Page sizes must be powers of 2!";

  BATT_CHECK_LT(page_size_log2, kMaxPageSizeLog2);
}

}  //namespace llfs
