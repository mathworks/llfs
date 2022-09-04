//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/memory_page_device.hpp>
//

#include <batteries/checked_cast.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemoryPageDevice::MemoryPageDevice(page_device_id_int device_id, PageCount capacity,
                                   PageSize page_size) noexcept
    : page_ids_{capacity, device_id}
    , page_size_{page_size}
{
  this->state_.lock()->page_recs.resize(capacity);
  this->state_.lock()->recently_dropped.fill(PageId{kInvalidPageId});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageIdFactory MemoryPageDevice::page_ids()
{
  return this->page_ids_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageSize MemoryPageDevice::page_size()
{
  return this->page_size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> MemoryPageDevice::prepare(PageId page_id)
{
  return PageBuffer::allocate(this->page_size_, page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryPageDevice::write(std::shared_ptr<const PageBuffer>&& buffer, WriteHandler&& handler)
{
  BATT_CHECK_NOT_NULLPTR(buffer);

  auto result = [&]() -> Status {
    const auto physical_page = this->page_ids_.get_physical_page(buffer->page_id());
    const auto generation = this->page_ids_.get_generation(buffer->page_id());

    auto locked = state_.lock();

    BATT_CHECK_LT(physical_page, batt::checked_cast<i64>(locked->page_recs.size()));
    BATT_CHECK(this->page_ids_.generation_less_than(locked->page_recs[physical_page].generation,
                                                    generation))
        << "\n  current generation = " << locked->page_recs[physical_page].generation
        << "\n  write generation =   " << generation;

    locked->page_recs[physical_page].page = std::move(buffer);
    locked->page_recs[physical_page].generation = generation;

    return OkStatus();
  }();

  handler(result);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryPageDevice::read(PageId page_id, ReadHandler&& handler)
{
  auto result = [&]() -> StatusOr<std::shared_ptr<const PageBuffer>> {
    const auto physical_page = this->page_ids_.get_physical_page(page_id);
    const auto requested_generation = this->page_ids_.get_generation(page_id);

    auto locked = this->state_.lock();

    BATT_CHECK_LT(physical_page, batt::checked_cast<i64>(locked->page_recs.size()));
    auto& rec = locked->page_recs[physical_page];
    const auto current_generation_on_device = rec.generation;
    bool not_found = false;
    const char* not_found_reason = "";
    if (requested_generation != current_generation_on_device) {
      not_found = true;
      not_found_reason = "generations do not match";
    }
    if (rec.page == nullptr) {
      not_found = true;
      not_found_reason = "page has been dropped";
    }
    if (not_found) {
      LLFS_LOG_INFO() << "failing read with `kNotFound` (" << not_found_reason << ");"
                      << BATT_INSPECT(page_id) << BATT_INSPECT(requested_generation)
                      << BATT_INSPECT(current_generation_on_device)
                      << BATT_INSPECT((const void*)rec.page.get()) <<
          [&](std::ostream& out) {
            out << std::endl;
            batt::this_task_debug_info(out);
            out << std::endl;
            out << std::endl << boost::stacktrace::stacktrace{} << std::endl << std::endl;

            for (PageId dropped_page_id : locked->recently_dropped) {
              const auto dropped_physical_page = this->page_ids_.get_physical_page(dropped_page_id);
              if (physical_page == dropped_physical_page && dropped_page_id.is_valid()) {
                out << " (physical page found in recently dropped list: " << dropped_page_id << ")"
                    << std::endl;
              }
            }
          };
      LLFS_VLOG(1) << boost::stacktrace::stacktrace{};
      return Status{batt::StatusCode::kNotFound};  // TODO [tastolfi 2021-10-20] Add custom message?
    }

    return rec.page;
  }();

  BATT_CHECK(handler);
  std::move(handler)(result);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryPageDevice::drop(PageId page_id, WriteHandler&& handler)
{
  auto result = [&]() -> Status {
    const auto physical_page = this->page_ids_.get_physical_page(page_id);
    const auto generation_to_drop = this->page_ids_.get_generation(page_id);

    auto locked = state_.lock();

    BATT_CHECK_LT(physical_page, batt::checked_cast<i64>(locked->page_recs.size()));
    const auto generation_on_device = locked->page_recs[physical_page].generation;
    if (generation_on_device == generation_to_drop) {
      if (locked->page_recs[physical_page].page == nullptr) {
        LLFS_LOG_INFO() << "page dropped before it is written: " << BATT_INSPECT(page_id)
                        << [&](std::ostream& out) {
                             out << std::endl << boost::stacktrace::stacktrace{} << std::endl;
                           };
      }
      locked->page_recs[physical_page].page = nullptr;
    } else {
      // This is expected behavior; PageRecycler must update the PageAllocator to release its
      // refcount before dropping it from the device.  At this point it is safe to reallocate, which
      // sets up a harmless race condition between PageDevice::drop() and PageAllocator::allocate().
      // Since the refcount is 0 in this case, nobody is actually using the page so no harm, no
      // foul.
      //
      LLFS_VLOG(1) << " -- skipping drop " << BATT_INSPECT(page_id)
                   << BATT_INSPECT(generation_on_device) << BATT_INSPECT(generation_to_drop);
    }

    const usize index = locked->recently_dropped_next % locked->recently_dropped.size();
    locked->recently_dropped[index] = page_id;
    ++locked->recently_dropped_next;

    return OkStatus();
  }();

  handler(result);
}

}  // namespace llfs
