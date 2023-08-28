//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_RING_BUFFER_HPP
#define LLFS_RING_BUFFER_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/status.hpp>
#include <llfs/system_config.hpp>

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>
#include <batteries/case_of.hpp>
#include <batteries/interval.hpp>
#include <batteries/small_vec.hpp>

#include <boost/intrusive/options.hpp>
#include <boost/intrusive/slist.hpp>

#include <stdio.h>
#include <unistd.h>

#include <cstddef>
#include <mutex>
#include <string>
#include <unordered_map>
#include <variant>

namespace llfs {

// Ring buffer implementation that uses memory mapping to mirror the physical buffer pages twice
// consecutively, so that wrapping at the end of the buffer is dealt with transparently my the MMU.
//
class RingBuffer
{
 public:
  struct TempFile {
    u64 byte_size;
  };

  struct NamedFile {
    std::string file_name;
    u64 byte_size;
    i64 byte_offset = 0;
    bool create = true;
    bool truncate = true;
  };

  struct FileDescriptor {
    int fd = -1;
    FILE* fp = nullptr;
    u64 byte_size = 0;
    i64 byte_offset = 0;
    bool truncate = true;
    bool close = false;

    // If true, RingBuffer::Impl objects created from this struct will be saved to a global pool for
    // reuse, instead of being destroyed (un-mapping the memory regions and closing the fd).
    //
    bool cache_on_deallocate = false;
  };

  using Params = std::variant<TempFile, NamedFile, FileDescriptor>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Accesses the global atomic bool flag that controls whether buffer pooling is enabled.
   */
  static std::atomic<bool>& pool_enabled();

  /** \brief Clears the cached buffer pool.
   */
  static void reset_pool();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Create a new RingBuffer using the prescribed method.
  //
  // `params` can be one of the following types:
  //
  // TempFile{.byte_size}
  //   Create a new temp file with a unique name and the given size; the temp file is
  //   automatically deleted when this RingBuffer is destroyed (closing the file descriptor).
  //
  // NamedFile{.file_name, .byte_size, .byte_offset, .create, .truncate}
  //   Create or open the backing file at the given file name (path) and map the RingBuffer to the
  //   given byte offset/size within that file.  If `create` is true, then the create flag is set
  //   while opening the file.  If `truncate` is true, then the file is truncated at `size +
  //   offset`.
  //
  // FileDescriptor{.fd, .byte_size, .byte_offset, .truncate, .close}
  //   Map the given region of the file for which `fd` is an open descriptor.
  //
  explicit RingBuffer(const Params& params) noexcept;

  RingBuffer(const RingBuffer&) = delete;
  RingBuffer& operator=(const RingBuffer&) = delete;

  // Destroy the ring buffer.  Will close the file descriptor and unmap all mapped regions that were
  // backed by it.
  //
  ~RingBuffer() noexcept;

  // The total size in bytes of this buffer.
  //
  usize size() const;

  // The file descriptor of the open file backing the buffer.
  //
  int file_descriptor() const;

  // Return the contents of the buffer (mutable, read/write), shifted by the given offset.  The size
  // of the returned buffer will be equal to `this->size()`.
  //
  MutableBuffer get_mut(usize offset);

  // Return the contents of the buffer (const, read-only), shifted by the given offset.  The size of
  // the returned buffer will be equal to `this->size()`.
  //
  ConstBuffer get(usize offset) const;

  // Synchronize mapped region to backing file, including (inode) metadata.
  //
  Status sync();

  // Synchronize mapped region to backing file, **DATA ONLY** (no metadata).
  //
  Status datasync();

  /*! \brief Returns the list of physical buffer offsets for a given logical offset interval,
   * splitting the interval if necessary to account for wrap-around.
   */
  batt::SmallVec<batt::Interval<isize>, 2> physical_offsets_from_logical(
      const batt::Interval<isize>& logical_offsets);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct ImplNode;

  /** \brief A linked list of ImplNodes; used to save/reuse resources in the pool.
   */
  using ImplNodeList = boost::intrusive::slist<ImplNode,                            //
                                               boost::intrusive::cache_last<true>,  //
                                               boost::intrusive::constant_time_size<true>>;
  //----- --- -- -  -  -   -
  /** \brief Holds the resources for a single memory-mapped ring buffer.
   *
   * Instances of this class may be pooled for reuse.
   */
  struct Impl {
    /** \brief The _current_ size of the buffer.  This is 1/2 the size of the total mapped regions
     * beginning at `this->memory_`.
     */
    usize size_ = 0;

    /** \brief The _original_ allocated size of the buffer.  This is 1/2 the size of the total
     * available regions beginning at `this->memory_`.
     */
    usize capacity_ = 0;

    /** \brief The file descriptor of the file backing the mapped regions.
     */
    int fd_ = -1;

    /** \brief The FILE* for the file, if fopen (or similar) was used. (OPTIONAL)
     */
    FILE* fp_ = nullptr;

    /** \brief The starting offset within the file to place the mapped region.
     */
    i64 offset_within_file_ = 0;

    /** \brief Whether `this->fd_` should be closed when this Impl is destroyed.
     */
    bool close_fd_ = false;

    /** \brief The beginning of the first mapped region of size `this->size_`.  This region is
     * followed by a mirror of the same memory.
     */
    char* memory_ = nullptr;

    /** \brief Whether to save `this` to the global pool when the RingBuffer using it is done.
     */
    bool cache_on_deallocate_ = false;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    //----- --- -- -  -  -   -
    /** \brief Impl is a move-only type.
     */
    Impl(const Impl&) = delete;

    /** \brief Impl is a move-only type.
     */
    Impl& operator=(const Impl&) = delete;
    //----- --- -- -  -  -   -

    /** \brief Constructs an invalid Impl instance.
     */
    Impl() = default;

    /** \brief Creates a double-mapped ring buffer from the file described in `params`.
     */
    explicit Impl(const FileDescriptor& params) noexcept;

    /** \brief Moves other to a new Impl instance.  This will invalidate `other`.
     */
    Impl(Impl&& other) noexcept;

    /** \brief Destroys the Impl; if still valid, this also unmaps the memory regions and possibly
     * closes the file descriptor as well.
     */
    ~Impl() noexcept;

    /** \brief Moves the memory region and file descriptor owned by `other` into this; this will
     * release any resources previously held by `this` (as though the dtor had been called) and
     * invalidate `other`.
     *
     * \return *this
     */
    Impl& operator=(Impl&& other) noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    void resize(usize new_size) noexcept;

    void update_mapped_regions() noexcept;
  };

  //----- --- -- -  -  -   -
  /** \brief A linked-list node wrapper around an Impl.
   *
   * Instances of this type are in-place constructed in the mapped memory region of an already
   * allocated ring buffer Impl so they can be pushed onto the right size-specific linked list in
   * the pool.
   */
  struct ImplNode
      : boost::intrusive::slist_base_hook<boost::intrusive::cache_last<true>,
                                          boost::intrusive::constant_time_size<true>> {
    //----- --- -- -  -  -   -

    // IMPORTANT: intentionally *not* defining ctor that takes Impl&&, as a safeguard against
    // std::move/access bugs (move the impl, try to access impl.memory_, nullptr fault...)
    //
    ImplNode() = default;

    /** \brief The cached resources.
     */
    Impl impl;
  };

  //----- --- -- -  -  -   -
  /** \brief A thread-safe pool of cached RingBuffer::Impl objects, indexed by size.
   */
  class ImplPool
  {
   public:
    /** \brief Constructs an empty pool.
     */
    ImplPool() = default;

    //----- --- -- -  -  -   -
    /** \brief ImplPool is non-copyable.
     */
    ImplPool(const ImplPool&) = delete;

    /** \brief ImplPool is non-copyable.
     */
    ImplPool& operator=(const ImplPool&) = delete;
    //----- --- -- -  -  -   -

    /** \brief Returns a freshly initialized Impl, if possible reusing resources from the pool.
     *
     * Only `params` with type TempFile will attempt to reuse a buffer from the pool.
     */
    auto allocate(const Params& params) noexcept -> Impl;

    /** \brief Inserts `impl` into the pool for future reuse.
     */
    auto deallocate(Impl&& impl) noexcept -> void;

    /** \brief Clears all cached buffers from the pool.
     */
    void reset() noexcept;

   private:
    std::mutex mutex_;
    std::unordered_map<usize, ImplNodeList> pool_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a reference to the global, thread-safe buffer pool.
   */
  static auto impl_pool() noexcept -> ImplPool&;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The buffer/file resources in use by this RingBuffer.
   */
  Impl impl_;
};

}  // namespace llfs

#endif  // LLFS_RING_BUFFER_HPP
