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
  // Check for invalid buffer size.
  //
  if (size > Self::kMemoryUnitSize) {
    return {batt::StatusCode::kInvalidArgument};
  }

  std::unique_ptr<IoRingBufferPool> p_pool{new IoRingBufferPool{io_ring, count, size}};

  batt::Status status = p_pool->initialize();
  BATT_REQUIRE_OK(status);

  return p_pool;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::IoRingBufferPool(const IoRing& io_ring, BufferCount count,
                                                BufferSize size) noexcept
    : io_ring_{io_ring}
    , buffer_count_{count}
    , buffer_size_{size}
    , storage_{std::vector<std::unique_ptr<Registered>>{}}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::IoRingBufferPool(BufferVec&& borrowed_buffers) noexcept
    : io_ring_{[&]() -> const IoRing& {
      BATT_CHECK(!borrowed_buffers.empty());
      return borrowed_buffers.front().pool().get_io_ring();
    }()}
    , buffer_count_{[&] {
      return BufferCount{borrowed_buffers.size()};
    }()}
    , buffer_size_{[&] {
      return BufferSize{borrowed_buffers.front().size()};
    }()}
    , storage_{std::move(borrowed_buffers)}
{
  // Because we are just borrowing buffers that have already been registered, there are no syscalls
  // that can fail (just sanity checks), so we can panic if `initialize()` fails.
  //
  BATT_CHECK_OK(this->initialize());
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
void IoRingBufferPool::initialize_objects() noexcept
{
  BATT_CHECK_LE(this->buffer_size_, Self::kMemoryUnitSize);

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
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status IoRingBufferPool::initialize_registered(
    std::vector<std::unique_ptr<Registered>>& registered) noexcept
{
  // Allocate the buffer memory.
  //
  for (usize i = 0; i < this->buffer_count_; i += this->buffers_per_memory_unit_) {
    registered.emplace_back(std::make_unique<Registered>());
  }

  // Register the buffers with the IoRing.
  //
  StatusOr<usize> begin_index = this->io_ring_.register_buffers(
      as_seq(registered)  //
          | seq::map([](auto& p_registered) {
              return batt::MutableBuffer{&(p_registered->memory_), kMemoryUnitSize};
            })  //
          | seq::boxed(),
      /*update=*/true);

  BATT_REQUIRE_OK(begin_index);

  // Update each `Registered` with the `buf_index` value for fixed I/O.
  //
  for (auto& p_registered : registered) {
    p_registered->io_ring_index_ = i32(*begin_index);
    *begin_index += 1;
  }

  // Success!
  //
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status IoRingBufferPool::initialize() noexcept
{
  this->initialize_objects();

  return batt::case_of(
      this->storage_,

      //----- --- -- -  -  -   -
      [this](std::vector<std::unique_ptr<Registered>>& registered) -> batt::Status {
        return this->initialize_registered(registered);
      },

      //----- --- -- -  -  -   -
      [this](BufferVec& borrowed) -> batt::Status {
        BATT_CHECK(!borrowed.empty());
        BATT_CHECK_EQ(borrowed.size(), this->buffer_count_);
        BATT_CHECK_EQ(borrowed.front().size(), this->buffer_size_);

        return batt::OkStatus();
      });
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
  IoRingBufferPool::AbstractHandler* waiter = nullptr;
  IoRingBufferPool::BufferFreePoolList buffer_list;
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    // Use FILO (stack) order to try to reuse "hot" buffers.
    //
    if (this->waiters_.empty() ||
        this->waiters_.front().required_count > this->free_pool_.size() + 1) {
      this->free_pool_.push_front(*(new (ptr) Deallocated{}));
      return;
    }

    std::swap(this->free_pool_, buffer_list);
    waiter = &(this->waiters_.front());
    this->waiters_.pop_front();
  }
  //
  // IMPORTANT: always invoke handlers _after_ unlocking our mutex!

  BATT_CHECK_NOT_NULLPTR(waiter);
  BATT_CHECK_EQ(usize{waiter->required_count}, buffer_list.size() + usize{1});

  // Transfer the swapped `buffer_list` plus the newly deallocated `ptr` to a BufferVec to pass to
  // the waiter.
  //
  BufferVec buffers;

  // Completely drain `buffer_list`.
  //
  while (!buffer_list.empty()) {
    this->transfer_one(/*from=*/buffer_list, /*to=*/buffers);
  }

  // Finally add the last buffer, using the just-freed `ptr` storage.
  //
  buffers.emplace_back(Buffer{this->allocate(ptr)});

  BATT_CHECK_EQ(buffers.size(), waiter->required_count);
  waiter->notify(batt::StatusOr<BufferVec>{std::move(buffers)});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingBufferPool::transfer_one(BufferFreePoolList& from, BufferVec& to)
{
  Deallocated* deallocated = std::addressof(from.front());
  from.pop_front();

  (*deallocated).~Deallocated();

  to.emplace_back(Buffer{this->allocate(deallocated)});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingBufferPool::async_allocate_impl(IoRingBufferPool::AbstractHandler* handler)
{
  BufferVec buffers;
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    if (this->free_pool_.size() < handler->required_count) {
      this->waiters_.push_back(*handler);
      return;
    }

    for (usize i = 0; i < handler->required_count; ++i) {
      this->transfer_one(/*from=*/this->free_pool_, /*to=*/buffers);
    }
  }
  //
  // IMPORTANT: always invoke handlers _after_ unlocking our mutex!

  BATT_CHECK_EQ(buffers.size(), handler->required_count);
  handler->notify(batt::StatusOr<BufferVec>{std::move(buffers)});
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
auto IoRingBufferPool::await_allocate(BufferCount count) -> batt::StatusOr<BufferVec>
{
  return batt::Task::await<batt::StatusOr<BufferVec>>([this, count](auto&& handler) {
    this->async_allocate(count, BATT_FORWARD(handler));
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
  BATT_CHECK_GE(this->buffer_count_, this->free_pool_.size());
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

  batt::case_of(
      pool->storage_,

      //----- --- -- -  -  -   -
      [&](std::vector<std::unique_ptr<Registered>>& registered_storage)  //
      {
        // Find the registered memory that backs this object.
        //
        const isize memory_unit_i = array_index_of_this / this->pool_->buffers_per_memory_unit_;
        Registered& registered = *registered_storage[memory_unit_i];

        // Each memory unit (Registered buffer) can hold many user-visible buffers; figure out which
        // one within the memory unit corresponds to `this`, and calculate the data offset based on
        // that.
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
      },

      //----- --- -- -  -  -   -
      [&](BufferVec& borrowed)  //
      {
        Buffer& src_buffer = borrowed[array_index_of_this];
        this->io_ring_index_ = src_buffer.index();
        this->buffer_ = src_buffer.get();
      });
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
  LLFS_VLOG_IF(1, this->allocated_)
      << BATT_INSPECT(this->allocated_->use_count()) << "->" << (this->allocated_->use_count() + 1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingBufferPool::Buffer::~Buffer() noexcept
{
  LLFS_VLOG_IF(1, this->allocated_)
      << BATT_INSPECT(this->allocated_->use_count()) << "->" << (this->allocated_->use_count() - 1);
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
