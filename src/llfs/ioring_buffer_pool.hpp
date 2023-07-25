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

#include <boost/intrusive/options.hpp>
#include <boost/intrusive/slist.hpp>

#include <memory>
#include <type_traits>
#include <vector>

namespace llfs {

class IoRingBufferPool
{
 public:
  using Self = IoRingBufferPool;

  static constexpr usize kMemoryUnitSize = 2 * kMiB;

  using MemoryUnit = std::aligned_storage_t<kMemoryUnitSize, 512>;

  //----- --- -- -  -  -   -
  //
  class Registered
  {
   public:
    MemoryUnit memory_;
    i32 io_ring_index_ = -1;
  };

  //----- --- -- -  -  -   -
  //
  class Allocated : public batt::RefCounted<Allocated>
  {
   public:
    explicit Allocated(IoRingBufferPool* pool) noexcept;

    Allocated(const Allocated&) = delete;
    Allocated& operator=(const Allocated&) = delete;

    ~Allocated() noexcept;

    void* operator new(size_t size) = delete;

    inline void* operator new(size_t, void* where) noexcept
    {
      return where;
    }

    void operator delete(void*);

    IoRingBufferPool* pool_;
    i32 index_;
    batt::MutableBuffer buffer_;
  };

  //----- --- -- -  -  -   -
  //
  class Deallocated
      : public boost::intrusive::slist_base_hook<boost::intrusive::cache_last<true>,
                                                 boost::intrusive::constant_time_size<true>>
  {
   public:
    Deallocated() = default;

    Deallocated(const Deallocated&) = delete;
    Deallocated& operator=(const Deallocated&) = delete;
  };

  class Buffer
  {
   public:
    Buffer() noexcept : allocated_{nullptr}
    {
    }

    explicit Buffer(batt::SharedPtr<const Allocated>&& allocated) noexcept
        : allocated_{std::move(allocated)}
    {
    }

    explicit operator bool() const noexcept
    {
      return this->allocated_ != nullptr;
    }

    i32 index() const noexcept
    {
      return this->allocated_->index_;
    }

    const batt::MutableBuffer& get() const noexcept
    {
      return this->allocated_->buffer_;
    }

    usize size() const noexcept
    {
      return this->get().size();
    }

    void* data() const noexcept
    {
      return this->get().data();
    }

    IoRingBufferPool& pool() const noexcept
    {
      return *this->allocated_->pool_;
    }

   private:
    batt::SharedPtr<const Allocated> allocated_;
  };

  using AllocResult = batt::StatusOr<Buffer>;

  using ObjectStorage = std::aligned_storage_t<std::max(sizeof(Allocated), sizeof(Deallocated))>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingBufferPool(const IoRing& io_ring, BufferCount count, BufferSize size) noexcept;

  ~IoRingBufferPool() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::Status initialize() noexcept;

  template <typename Handler = void(AllocResult)>
  void async_allocate(Handler&& handler)
  {
    this->async_allocate_impl(
        batt::HandlerImpl<Handler, AllocResult>::make_new(BATT_FORWARD(handler)));
  }

  void async_allocate_impl(batt::AbstractHandler<AllocResult>* handler);

  auto await_allocate() -> AllocResult;

  usize in_use() noexcept
  {
    std::unique_lock<std::mutex> lock{this->mutex_};
    return this->buffer_count_ - this->free_pool_.size();
  }

  usize available() noexcept
  {
    std::unique_lock<std::mutex> lock{this->mutex_};
    return this->free_pool_.size();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  static void swap_this(IoRingBufferPool*& ptr1);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::SharedPtr<Allocated> allocate(void* ptr);

  void deallocate(void* a) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const IoRing& io_ring_;

  const BufferCount buffer_count_;

  const BufferSize buffer_size_;

  const usize buffers_per_memory_unit_ = Self::kMemoryUnitSize / this->buffer_size_;

  std::vector<std::unique_ptr<Registered>> registered_;

  std::vector<ObjectStorage> objects_;

  std::mutex mutex_;

  boost::intrusive::slist<Deallocated,                         //
                          boost::intrusive::cache_last<true>,  //
                          boost::intrusive::constant_time_size<true>>
      free_pool_;

  batt::HandlerList<AllocResult> waiters_;
};

}  //namespace llfs

#endif  // LLFS_IORING_BUFFER_POOL_HPP
