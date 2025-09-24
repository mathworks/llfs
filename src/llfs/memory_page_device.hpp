//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEMORY_PAGE_DEVICE_HPP
#define LLFS_MEMORY_PAGE_DEVICE_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_size.hpp>
#include <llfs/status.hpp>

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/mutex.hpp>

#include <atomic>
#include <memory>
#include <unordered_map>

namespace llfs {

class MemoryPageDevice : public PageDevice
{
 public:
  explicit MemoryPageDevice(page_device_id_int device_id, PageCount capacity,
                            PageSize page_size) noexcept;

  std::unique_ptr<PageDevice> make_sharded_view(page_device_id_int device_id,
                                                PageSize shard_size) override;

  PageIdFactory page_ids() override;

  PageSize page_size() override;

  StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) override;

  void write(std::shared_ptr<const PageBuffer>&& buffer, WriteHandler&& handler) override;

  void read(PageId page_id, ReadHandler&& handler) override;

  void drop(PageId page_id, WriteHandler&& handler) override;

 private:
  class ShardedView : public PageDevice
  {
   public:
    explicit ShardedView(MemoryPageDevice& real_device, page_device_id_int device_id,
                         PageSize shard_size) noexcept;

    PageIdFactory page_ids() override;

    PageSize page_size() override;

    StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) override;

    void write(std::shared_ptr<const PageBuffer>&& buffer, WriteHandler&& handler) override;

    void read(PageId page_id, ReadHandler&& handler) override;

    void drop(PageId page_id, WriteHandler&& handler) override;

   private:
    MemoryPageDevice& real_device_;
    const PageIdFactory page_ids_;
    PageSize shard_size_;
  };

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
  const PageSize page_size_;
  batt::Mutex<State> state_;
};

}  // namespace llfs

#endif  // LLFS_MEMORY_PAGE_DEVICE_HPP
