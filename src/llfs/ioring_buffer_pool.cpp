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
/*static*/ batt::StatusOr<std::unique_ptr<IoRingBufferPool>> IoRingBufferPool::make_new(
    const IoRing& io_ring, BufferCount count, BufferSize size) noexcept
{
  std::unique_ptr<IoRingBufferPool> p_pool{new IoRingBufferPool{io_ring, count, size}};

  BATT_REQUIRE_OK(p_pool->initialize());

  return p_pool;
}

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
  BATT_CHECK_EQ(this->free_pool_.size(), this->buffer_count_)
      << "Buffer pool was destroyed with some buffers still allocated/active";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status IoRingBufferPool::initialize() noexcept
{
  // Check for invalid buffer size.
  //
  if (this->buffer_size_ > Self::kMemoryUnitSize) {
    return batt::StatusCode::kInvalidArgument;
  }

  // Initialize the free_pool_.
  //
  this->objects_.resize(this->buffer_count_);
  for (usize i = 0; i < this->buffer_count_; ++i) {
    // We could just call this->deallocate for each address, but that would be slower on account of
    // having to lock the mutex repeatedly and check for waiters.
    //
    auto* deallocated = new (&this->objects_[i]) Deallocated{};
    this->free_pool_.push_back(*deallocated);
  }

  // Calculate (and log) the number of memory units required for the current configuration.
  //
  const usize n_units =
      (this->buffer_count_ + this->buffers_per_memory_unit_ - 1) / this->buffers_per_memory_unit_;

  LLFS_VLOG(1) << BATT_INSPECT(n_units);

  // Allocate the buffer memory.
  //
  for (usize i = 0; i < this->buffer_count_; i += this->buffers_per_memory_unit_) {
    this->registered_.emplace_back(std::make_unique<Registered>());
  }

  // Register the buffers with the IoRing.
  //
  StatusOr<usize> begin_index = this->io_ring_.register_buffers(
      as_seq(this->registered_)  //
          | seq::map([](auto& p_registered) {
              return batt::MutableBuffer{&(p_registered->memory_), kMemoryUnitSize};
            })  //
          | seq::boxed(),
      /*update=*/true);

  BATT_REQUIRE_OK(begin_index);

  // Update each `Registered` with the `buf_index` value for fixed I/O.
  //
  for (auto& p_registered : this->registered_) {
    p_registered->io_ring_index_ = i32(*begin_index);
    *begin_index += 1;
  }

  // Success!
  //
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
  batt::AbstractHandler<batt::StatusOr<Buffer>>* waiter = nullptr;
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    if (this->waiters_.empty()) {
      // Use FILO (stack) order to try to reuse "hot" buffers.
      //
      this->free_pool_.push_front(*(new (ptr) Deallocated{}));
      return;
    }

    waiter = &(this->waiters_.front());
    this->waiters_.pop_front();
  }
  //
  // IMPORTANT: always invoke handlers _after_ unlocking our mutex!

  BATT_CHECK_NOT_NULLPTR(waiter);

  waiter->notify(Buffer{this->allocate(ptr)});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingBufferPool::async_allocate_impl(batt::AbstractHandler<batt::StatusOr<Buffer>>* handler)
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
  //
  // IMPORTANT: always invoke handlers _after_ unlocking our mutex!

  BATT_CHECK_NOT_NULLPTR(deallocated);

  (*deallocated).~Deallocated();

  handler->notify(this->allocate(deallocated));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingBufferPool::await_allocate() -> batt::StatusOr<Buffer>
{
  return batt::Task::await<batt::StatusOr<Buffer>>([this](auto&& handler) {
    this->async_allocate(BATT_FORWARD(handler));
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingBufferPool::try_allocate() -> batt::StatusOr<Buffer>
{
  Deallocated* deallocated = nullptr;
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    if (!this->free_pool_.empty()) {
      deallocated = &(this->free_pool_.front());
      this->free_pool_.pop_front();
    }
  }

  if (deallocated) {
    (*deallocated).~Deallocated();
    return this->allocate(deallocated);
  }

  return {batt::StatusCode::kResourceExhausted};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize IoRingBufferPool::in_use() noexcept
{
  std::unique_lock<std::mutex> lock{this->mutex_};
  return this->buffer_count_ - this->free_pool_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize IoRingBufferPool::available() noexcept
{
  std::unique_lock<std::mutex> lock{this->mutex_};
  return this->free_pool_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ void IoRingBufferPool::swap_this(IoRingBufferPool*& incoming)
{
  thread_local IoRingBufferPool* stashed = nullptr;
  std::swap(stashed, incoming);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class IoRingBufferPool::Allocated

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::Allocated::Allocated(IoRingBufferPool* pool) noexcept : pool_{pool}
{
  // Find the array index of `this` within `pool->objects_` based on address.
  //
  const isize byte_offset_of_this_in_objects = byte_distance(this->pool_->objects_.data(), this);
  const isize array_index_of_this = byte_offset_of_this_in_objects / sizeof(ObjectStorage);

  BATT_CHECK_LT(array_index_of_this, this->pool_->objects_.size()) << "Pool/object mismatch?";

  // Find the registered memory that backs this object.
  //
  const isize memory_unit_i = array_index_of_this / this->pool_->buffers_per_memory_unit_;
  Registered& registered = *this->pool_->registered_[memory_unit_i];

  // Each memory unit (Registered buffer) can hold many user-visible buffers; figure out which one
  // within the memory unit corresponds to `this`, and calculate the data offset based on that.
  //
  const isize index_within_unit = array_index_of_this % this->pool_->buffers_per_memory_unit_;
  const isize offset_within_unit = index_within_unit * this->pool_->buffer_size_;
  u8* const data_start = (u8*)(&registered.memory_) + offset_within_unit;

  // Dump our calculations for debugging.
  //
  LLFS_DVLOG(1) << BATT_INSPECT(sizeof(ObjectStorage))           //
                << BATT_INSPECT(byte_offset_of_this_in_objects)  //
                << BATT_INSPECT(array_index_of_this)             //
                << BATT_INSPECT(memory_unit_i)                   //
                << BATT_INSPECT(index_within_unit)               //
                << BATT_INSPECT(offset_within_unit)              //
                << BATT_INSPECT(this->pool_->buffer_size_);

  this->io_ring_index_ = registered.io_ring_index_;
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class IoRingBufferPool::Buffer

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingBufferPool::Buffer::Buffer() noexcept : allocated_{nullptr}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::Buffer::Buffer(batt::SharedPtr<const Allocated>&& allocated) noexcept
    : allocated_{std::move(allocated)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingBufferPool::Buffer::is_valid() const noexcept
{
  return this->allocated_ != nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::Buffer::operator bool() const noexcept
{
  return this->is_valid();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 IoRingBufferPool::Buffer::index() const noexcept
{
  return this->allocated_->io_ring_index_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const batt::MutableBuffer& IoRingBufferPool::Buffer::get() const noexcept
{
  return this->allocated_->buffer_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize IoRingBufferPool::Buffer::size() const noexcept
{
  return this->get().size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void* IoRingBufferPool::Buffer::data() const noexcept
{
  return this->get().data();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingBufferPool& IoRingBufferPool::Buffer::pool() const noexcept
{
  return *this->allocated_->pool_;
}

}  //namespace llfs
