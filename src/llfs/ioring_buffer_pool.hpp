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
#include <llfs/status.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/cpu_align.hpp>

#include <boost/intrusive/options.hpp>
#include <boost/intrusive/slist.hpp>

#include <memory>
#include <type_traits>
#include <vector>

namespace llfs {

/** \brief An asychronous pool of buffers registered with an IoRing for fast I/O.
 *
 * Must be created via static IoRingBufferPool::make_new function.
 */
class IoRingBufferPool
{
 public:
  /** \brief Alias for this type.
   */
  using Self = IoRingBufferPool;

  /** \brief The underlying unit of allocation and buffer registration; this is the minimum amount
   * of memory that will be allocated by the buffer pool, even if (BufferSize * BufferCount) is
   * smaller.
   */
  static constexpr usize kMemoryUnitSize = 2 * kMiB;

  /** \brief A chunk of memory for the pool.
   */
  using MemoryUnit = std::aligned_storage_t<kMemoryUnitSize, kDirectIOBlockAlign>;

  //----- --- -- -  -  -   -
  /** \brief A buffer registered with the IoRing.
   */
  class Registered
  {
   public:
    /** \brief The buffer storage area.  One or more user-visible buffers will be placed in
     * this memory region.
     */
    MemoryUnit memory_;

    /** \brief The `buf_index` field to use when doing "fixed" I/O using this buffer.
     */
    i32 io_ring_index_ = -1;
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

  //----- --- -- -  -  -   -
  /** \brief Tracks a user-visible buffer in the 'allocated' state.
   */
  class Allocated : public batt::RefCounted<Allocated>
  {
   public:
    /** \brief Change the state of `this` tracking object to 'allocated'.
     */
    explicit Allocated(IoRingBufferPool* pool) noexcept;

    /** \brief Allocated is not copyable.
     */
    Allocated(const Allocated&) = delete;

    /** \brief Allocated is not copyable.
     */
    Allocated& operator=(const Allocated&) = delete;

    /** \brief Stashes the `pool_` in thread-local storage so it can later be accessed by operator
     * delete.
     */
    ~Allocated() noexcept;

    /** \brief Allocated always uses placement-new.
     */
    void* operator new(size_t size) = delete;

    /** \brief Required because we deleted the regular operator new.
     */
    void* operator new(size_t, void* ptr) noexcept
    {
      return ptr;
    }

    /** \brief Uses the stashed pool pointer to construct a Deallocated object at this address,
     * adding it to the free list.
     */
    void operator delete(void*);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief The pool to which this object belongs.
     */
    IoRingBufferPool* pool_;

    /** \brief The `buf_index` for the underlying memory chunk that backs this buffer.
     */
    i32 io_ring_index_;

    /** \brief The user-visible allocated buffer; this will be the configured size (passed in
     * at pool construction time), *not* kMemoryUnitSize.
     */
    batt::MutableBuffer buffer_;
  };

  //----- --- -- -  -  -   -
  /** \brief A user-visible allocated buffer.
   *
   * This class is copyable; all copies share the same underlying buffer memory.  When the last copy
   * is destroyed, the buffer is returned to the pool.
   */
  class Buffer
  {
   public:
    friend class Allocated;

    //----- --- -- -  -  -   -

    /** \brief Constructs an invalid (empty) Buffer.
     */
    Buffer() noexcept;

    /** \brief Constructs a new Buffer, taking ownership of the passed pointer.
     */
    explicit Buffer(batt::SharedPtr<const Allocated>&& allocated) noexcept;

    /** This class is copyable; all copies share the same underlying buffer memory.  When the last
     * copy is destroyed, the buffer is returned to the pool.
     */
    Buffer(const Buffer&) = default;

    /** \brief Destroys this Buffer object, decrementing the ref count on the underlying memory by
     * one.
     */
    ~Buffer() noexcept;

    //----- --- -- -  -  -   -

    /** This class is copyable; all copies share the same underlying buffer memory.  When the last
     * copy is destroyed, the buffer is returned to the pool.
     */
    Buffer& operator=(const Buffer&) = default;

    /** This class is copyable; all copies share the same underlying buffer memory.  When the last
     * copy is destroyed, the buffer is returned to the pool.
     */
    Buffer(Buffer&&) = default;

    /** This class is copyable; all copies share the same underlying buffer memory.  When the last
     * copy is destroyed, the buffer is returned to the pool.
     */
    Buffer& operator=(Buffer&&) = default;

    /** \brief Returns true iff this Buffer is valid (non-nullptr).
     */
    explicit operator bool() const noexcept;

    /** \brief Returns true iff this Buffer is valid (non-nullptr).
     */
    bool is_valid() const noexcept;

    /** \brief The `buf_index` to use when performing 'fixed' I/O with this buffer.
     */
    i32 index() const noexcept;

    /** \brief Returns this as a batt::MutableBuffer (boost::asio::mutable_buffer).
     */
    const batt::MutableBuffer& get() const noexcept;

    /** \brief Returns the size (bytes) of the buffer.
     */
    usize size() const noexcept;

    /** \brief Returns the starting address of the buffer.
     */
    void* data() const noexcept;

    /** \brief Returns a reference to the pool to which this buffer belongs.
     */
    IoRingBufferPool& pool() const noexcept;

   private:
    /** \brief The tracking object for this buffer.
     */
    batt::SharedPtr<const Allocated> allocated_;
  };

  /** \brief A chunk of memory used to track a single user-visible buffer.
   *
   * An array of these is allocated when the pool is created, one element for each user-visible
   * buffer (BufferCount).  When the corresponding buffer is not in use, an instance of
   * `Deallocated` is constructed in this memory and linked into IoRingBufferPool::free_pool_.
   *
   * When the buffer is allocated, it is removed from free_pool_, the Deallocated object is
   * destroyed, and then the memory is reused to construct an instance of `Allocated`, which is then
   * tracked via boost::intrusive_ptr inside IoRingBufferPool::Buffer.
   */
  using ObjectStorage = std::aligned_storage_t<std::max(sizeof(Allocated), sizeof(Deallocated))>;

  //----- --- -- -  -  -   -

  struct HandlerBase : batt::DefaultHandlerBase {
    BufferCount required_count;
  };

  using BufferVec = batt::SmallVec<Buffer, 2>;

  using AbstractHandler = batt::BasicAbstractHandler<HandlerBase, StatusOr<BufferVec>&&>;

  template <typename Fn>
  using HandlerImpl = batt::BasicHandlerImpl<Fn, HandlerBase, StatusOr<BufferVec>&&>;

  using HandlerList = batt::BasicHandlerList<HandlerBase, StatusOr<BufferVec>&&>;

  using BufferFreePoolList = boost::intrusive::slist<Deallocated,                         //
                                                     boost::intrusive::cache_last<true>,  //
                                                     boost::intrusive::constant_time_size<true>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs, initializes, and returns a new IoRingBufferPool.
   *
   * Automatically registers the buffers in the pool with the passed IoRing context.
   */
  static StatusOr<std::unique_ptr<IoRingBufferPool>> make_new(const IoRing& io_ring,
                                                              BufferCount count,
                                                              BufferSize size = BufferSize{
                                                                  Self::kMemoryUnitSize}) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a private sub-pool of buffers borrowed from another pool.
   */
  explicit IoRingBufferPool(BufferVec&& borrowed_buffers) noexcept;

  ~IoRingBufferPool() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const IoRing& get_io_ring() const noexcept
  {
    return this->io_ring_;
  }

  BufferSize buffer_size() const noexcept
  {
    return this->buffer_size_;
  }

  BufferCount buffer_count() const noexcept
  {
    return this->buffer_count_;
  }

  /** \brief Asynchronously allocate a new buffer.  Waits until a buffer becomes available and then
   * invokes the passed handler.
   *
   * The signature of the handler is: `void (StatusOr<llfs::IoRingBufferPool::Buffer>)`.
   */
  template <typename Handler = void(StatusOr<Buffer>&&)>
  void async_allocate(Handler&& handler)
  {
    this->async_allocate(
        BufferCount{1},
        batt::bind_handler(  //
            BATT_FORWARD(handler), [](Handler&& handler, StatusOr<BufferVec>&& buffers) {
              if (!buffers.ok()) {
                BATT_FORWARD(handler)(StatusOr<Buffer>{buffers.status()});
                return;
              }
              BATT_CHECK_EQ(buffers->size(), 1u);
              BATT_FORWARD(handler)(StatusOr<Buffer>{std::move(buffers->front())});
            }));
  }

  /** \brief Asynchronously allocate the specified number of buffers.  Waits until enough buffers
   * become available and then invokes the passed handler.
   *
   * The signature of the handler is: `void (StatusOr<llfs::IoRingBufferPool::Buffer>)`.
   */
  template <typename Fn = void(StatusOr<BufferVec>&&)>
  void async_allocate(BufferCount count, Fn&& fn)
  {
    AbstractHandler* handler = HandlerImpl<Fn>::make_new(BATT_FORWARD(fn));
    handler->required_count = count;
    this->async_allocate_impl(handler);
  }

  /** \brief Asynchronous allocate with pre-allocated handler.  See async_allocate.
   */
  void async_allocate_impl(AbstractHandler* handler);

  /** \brief Blocks the current Task until a buffer becomes available, then allocates and returns
   * it.
   */
  auto await_allocate() -> StatusOr<Buffer>;

  /** \brief Blocks the current Task until a buffer becomes available, then allocates and returns
   * it.
   */
  auto await_allocate(BufferCount count) -> StatusOr<BufferVec>;

  /** \brief Attempts to allocate a buffer without blocking; if successful, returns the buffer; else
   * returns batt::StatusCode::kResourceExhausted.
   */
  auto try_allocate() -> StatusOr<Buffer>;

  /** \brief Returns the number of buffers in the pool which are currently in use (allocated).
   */
  usize in_use() noexcept;

  /** \brief Returns the number of buffers in the pool which are currently available for allocate
   * (i.e., deallocated).
   */
  usize available() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Stash the passed pool pointer in thread-local storage; this is a hack used when freeing
   * a buffer to avoid relying on undefined behavior re: overlapped object lifetimes using the same
   * memory.
   */
  static void swap_this(IoRingBufferPool*& ptr);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingBufferPool(const IoRing& io_ring, BufferCount count,
                            BufferSize size = BufferSize{Self::kMemoryUnitSize}) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Called once inside make_new; registers the buffers with the IoRing.
   */
  batt::Status initialize() noexcept;

  void initialize_objects() noexcept;

  batt::Status initialize_registered(std::vector<std::unique_ptr<Registered>>& registered) noexcept;

  /** \brief Constructs and returns an Allocated object at the given memory address.
   *
   * `ptr` MUST be in the `this->objects_` array, or behavior is undefined!
   */
  batt::SharedPtr<Allocated> allocate(void* ptr);

  /** \brief Constructs and returns a Deallocated object at the given memory address, pushing it
   * onto the free list.
   *
   * `ptr` MUST be in the `this->objects_` array, or behavior is undefined!
   */
  void deallocate(void* a) noexcept;

  void transfer_one(BufferFreePoolList& from, BufferVec& to);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The IoRing context for this pool.
   */
  const IoRing& io_ring_;

  /** \brief The number of buffers in the pool.
   */
  const BufferCount buffer_count_;

  /** \brief The size of each buffer.
   */
  const BufferSize buffer_size_;

  /** \brief The number of buffers that fit within a "memory unit" (the unit of allocation and
   * IoRing registration).
   */
  const usize buffers_per_memory_unit_ = Self::kMemoryUnitSize / this->buffer_size_;

  /** \brief The registered/borrowed memory units.
   */
  std::variant<std::vector<std::unique_ptr<Registered>>, BufferVec> storage_;

  /** \brief Memory for the per-buffer tracking objects; each `ObjectStorage` can be constructed as
   * an instance of either the `Deallocated` or `Allocated` class.
   */
  std::vector<ObjectStorage> objects_;

  /** \brief Protectes all the following data members.
   */
  std::mutex mutex_;

  /** \brief A linked list of buffers available for allocation.
   */
  BufferFreePoolList free_pool_;

  /** \brief A linked list of waiters blocked until a buffer becomes available.
   */
  HandlerList waiters_;
};

inline bool operator==(const IoRingBufferPool::Buffer& l, const IoRingBufferPool::Buffer& r)
{
  return l.data() == r.data();
}

inline bool operator!=(const IoRingBufferPool::Buffer& l, const IoRingBufferPool::Buffer& r)
{
  return !(l == r);
}
}  //namespace llfs

#endif  // LLFS_IORING_BUFFER_POOL_HPP
