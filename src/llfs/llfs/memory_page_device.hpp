#pragma once
#ifndef LLFS_MEMORY_PAGE_DEVICE_HPP
#define LLFS_MEMORY_PAGE_DEVICE_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_device.hpp>
#include <llfs/status.hpp>

#include <glog/logging.h>

#include <batteries/assert.hpp>
#include <batteries/async/mutex.hpp>

#include <atomic>
#include <memory>
#include <unordered_map>

namespace llfs {

class MemoryPageDevice : public PageDevice
{
 public:
  explicit MemoryPageDevice(page_device_id_int device_id, isize capacity, u32 page_size) noexcept;

  PageIdFactory page_ids() override;

  u32 page_size() override;

  StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) override;

  void write(std::shared_ptr<const PageBuffer>&& buffer, WriteHandler&& handler) override;

  void read(PageId page_id, ReadHandler&& handler) override;

  void drop(PageId page_id, WriteHandler&& handler) override;

 private:
  struct PageRec {
    std::shared_ptr<const PageBuffer> page;
    page_generation_int generation = 0;
  };

  struct State {
    std::vector<PageRec> page_recs;
    std::array<PageId, 4096> recently_dropped;
    usize recently_dropped_next = 0;
  };

  const PageIdFactory page_ids_;
  const u32 page_size_;
  batt::Mutex<State> state_;
};

}  // namespace llfs

#endif  // LLFS_MEMORY_PAGE_DEVICE_HPP
