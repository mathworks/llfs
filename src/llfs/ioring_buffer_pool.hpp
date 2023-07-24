//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_BUFFER_POOL_HPP
#define LLFS_IORING_BUFFER_POOL_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/constants.hpp>
#include <llfs/ioring.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/cpu_align.hpp>

namespace llfs {

class IoRingBufferPool
{
 public:
  using Self = IoRingBufferPool;

  static constexpr usize kShardCount = 256;
  static constexpr usize kMemorySize = 2 * kMiB;

  using MemoryUnit = std::aligned_storage_t<kMemorySize, 512>;

  class AllocatedBuffer : public batt::RefCounted<AllocatedBuffer>
  {
   public:
    explicit AllocatedBuffer(batt::MutableBuffer buffer, IoRingBufferPool& pool) noexcept
        : buffer_{buffer}
        , pool_{pool}
    {
    }

    ~AllocatedBuffer() noexcept;

    batt::MutableBuffer buffer_;
    int index_;
    IoRingBufferPool& pool_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingBufferPool(IoRing& io_ring, BufferCount count, BufferSize size) noexcept;

  ~IoRingBufferPool() noexcept;

  template <typename Handler = void(batt::StatusOr<batt::SharedPtr<const AllocatedBuffer>>)>
  void async_alloc(Handler&& handler)
  {
    (void)handler;
  }

  batt::StatusOr<batt::SharedPtr<const AllocatedBuffer>> await_alloc()
  {
    return batt::Task::await<batt::StatusOr<batt::SharedPtr<AllocatedBuffer>>>(
        [this](auto&& handler) {
          this->async_alloc(BATT_FORWARD(handler));
        });
  }

 private:
  void free_buffer(int index, const batt::MutableBuffer& buffer);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRing& io_ring_;

  BufferCount buffer_count_;

  BufferSize buffer_size_;

  std::vector<std::unique_ptr<MemoryUnit>> memory_;

  batt::CpuCacheLineIsolated<std::atomic<u64>> alloc_count_{0};

  batt::CpuCacheLineIsolated<std::atomic<u64>> free_count_{0};
};

}  //namespace llfs

#endif  // LLFS_IORING_BUFFER_POOL_HPP
