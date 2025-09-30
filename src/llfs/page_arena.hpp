//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ARENA_HPP
#define LLFS_PAGE_ARENA_HPP

#include <llfs/page_allocator.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_device.hpp>

namespace llfs {

// A page storage device with a log-structured allocation index.
//
class PageArena
{
 public:
  explicit PageArena(std::unique_ptr<PageDevice> device,
                     std::unique_ptr<PageAllocator> allocator) noexcept
      : device_{std::move(device)}
      , allocator_{std::move(allocator)}
  {
  }

  PageArena(const PageArena&) = delete;
  PageArena& operator=(const PageArena&) = delete;

  PageArena(PageArena&&) = default;
  PageArena& operator=(PageArena&&) = default;

  page_device_id_int id() const
  {
    return this->device().page_ids().get_device_id();
  }

  PageDevice& device() const
  {
    return *this->device_;
  }

  PageAllocator& allocator() const
  {
    BATT_CHECK_NOT_NULLPTR(this->allocator_);
    return *this->allocator_;
  }

  bool has_allocator() const
  {
    return this->allocator_ != nullptr;
  }

  void halt()
  {
    // this->device_->close();  TODO [tastolfi 2021-04-07]
    if (this->allocator_) {
      this->allocator_->halt();
    }
  }

  void join()
  {
    // this->device_->join();  TODO [tastolfi 2021-04-07]
    if (this->allocator_) {
      this->allocator_->join();
    }
  }

 private:
  std::unique_ptr<PageDevice> device_;
  std::unique_ptr<PageAllocator> allocator_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline usize get_page_size(const PageArena& arena)
{
  return arena.device().page_size();
}

}  // namespace llfs

#endif  // LLFS_PAGE_ARENA_HPP
