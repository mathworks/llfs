//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_buffer_pool.hpp>
//

#include <llfs/seq.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::IoRingBufferPool(const IoRing& io_ring, BufferCount count,
                                                BufferSize size) noexcept
    : io_ring_{io_ring}
    , buffer_count_{count}
    , buffer_size_{size}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingBufferPool::~IoRingBufferPool() noexcept
{
  BATT_CHECK_EQ(this->free_pool_.size(), this->buffer_count_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status IoRingBufferPool::initialize() noexcept
{
  if (this->buffer_size_ > Self::kMemoryUnitSize) {
    return batt::StatusCode::kInvalidArgument;
  }

  this->objects_.resize(this->buffer_count_);

  for (usize i = 0; i < this->buffer_count_; ++i) {
    auto* deallocated = new (&this->objects_[i]) Deallocated{};
    this->free_pool_.push_back(*deallocated);
  }

  const usize n_units =
      (this->buffer_count_ + this->buffers_per_memory_unit_ - 1) / this->buffers_per_memory_unit_;

  LLFS_VLOG(1) << BATT_INSPECT(n_units);

  for (usize i = 0; i < this->buffer_count_; i += this->buffers_per_memory_unit_) {
    this->registered_.emplace_back(std::make_unique<Registered>());
  }

  StatusOr<usize> begin_index = this->io_ring_.register_buffers(
      as_seq(this->registered_)  //
          | seq::map([](auto& p_registered) {
              return batt::MutableBuffer{&(p_registered->memory_), kMemoryUnitSize};
            })  //
          | seq::boxed(),
      /*update=*/true);

  BATT_REQUIRE_OK(begin_index);

  for (auto& p_registered : this->registered_) {
    p_registered->io_ring_index_ = i32(*begin_index);
    *begin_index += 1;
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingBufferPool::allocate(void* ptr) -> batt::SharedPtr<Allocated>
{
  return batt::SharedPtr<Allocated>{new (ptr) Allocated{this}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingBufferPool::deallocate(void* ptr) noexcept
{
  batt::AbstractHandler<AllocResult>* waiter = nullptr;
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    if (this->waiters_.empty()) {
      this->free_pool_.push_back(*(new (ptr) Deallocated{}));
      return;
    }

    waiter = &(this->waiters_.front());
    this->waiters_.pop_front();
  }

  BATT_CHECK_NOT_NULLPTR(waiter);

  waiter->notify(Buffer{this->allocate(ptr)});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingBufferPool::async_allocate_impl(batt::AbstractHandler<AllocResult>* handler)
{
  Deallocated* deallocated = nullptr;
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    if (this->free_pool_.empty()) {
      this->waiters_.push_back(*handler);
      return;
    }

    deallocated = &(this->free_pool_.front());
    this->free_pool_.pop_front();
  }

  BATT_CHECK_NOT_NULLPTR(deallocated);

  (*deallocated).~Deallocated();

  handler->notify(this->allocate(deallocated));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingBufferPool::await_allocate() -> AllocResult
{
  return batt::Task::await<AllocResult>([this](auto&& handler) {
    this->async_allocate(BATT_FORWARD(handler));
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ void IoRingBufferPool::swap_this(IoRingBufferPool*& ptr1)
{
  thread_local IoRingBufferPool* ptr0_ = nullptr;
  std::swap(ptr0_, ptr1);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class IoRingBufferPool::Allocated

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::Allocated::Allocated(IoRingBufferPool* pool) noexcept : pool_{pool}
{
  const isize byte_offset_of_this = byte_distance(this->pool_->objects_.data(), this);
  const isize index_of_this = byte_offset_of_this / sizeof(ObjectStorage);
  const isize memory_unit_i = index_of_this / this->pool_->buffers_per_memory_unit_;
  const isize index_within_unit = index_of_this % this->pool_->buffers_per_memory_unit_;
  const isize offset_within_unit = index_within_unit * this->pool_->buffer_size_;

  Registered& registered = *this->pool_->registered_[memory_unit_i];
  u8* const data_start = (u8*)(&registered.memory_) + offset_within_unit;

  LLFS_VLOG(1) << BATT_INSPECT(sizeof(ObjectStorage)) << BATT_INSPECT(byte_offset_of_this)
               << BATT_INSPECT(index_of_this) << BATT_INSPECT(memory_unit_i)
               << BATT_INSPECT(index_within_unit) << BATT_INSPECT(offset_within_unit)
               << BATT_INSPECT(this->pool_->buffer_size_);

  this->index_ = registered.io_ring_index_;
  this->buffer_ = batt::MutableBuffer{data_start, this->pool_->buffer_size_};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingBufferPool::Allocated::~Allocated() noexcept
{
  IoRingBufferPool::swap_this(this->pool_);

  BATT_CHECK_EQ(this->pool_, nullptr);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingBufferPool::Allocated::operator delete(void* ptr)
{
  IoRingBufferPool* pool = nullptr;

  IoRingBufferPool::swap_this(pool);

  BATT_CHECK_NOT_NULLPTR(pool);

  pool->deallocate(ptr);
}

}  //namespace llfs
