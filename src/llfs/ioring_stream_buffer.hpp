//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_STREAM_BUFFER_HPP
#define LLFS_IORING_STREAM_BUFFER_HPP

#include <llfs/config.hpp>
//
#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_buffer_pool.hpp>
#include <llfs/ioring_buffer_view.hpp>
#include <llfs/optional.hpp>
#include <llfs/status.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/seq.hpp>
#include <batteries/small_vec.hpp>

#include <atomic>
#include <variant>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A zero-copy stream of IoRing pooled buffers.
 */
class IoRingStreamBuffer
{
 public:
  /** \brief The maximum number of buffers of committed data (i.e., instances of
   * IoRingBufferPool::Buffer allocated from the underlying pool passed in at construction time)
   * that a stream can contain.
   */
  static constexpr usize kMaxBuffersCapacity = 2;

  using BufferView = IoRingConstBufferView;
  using PreparedView = IoRingMutableBufferView;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  /** \brief A (short) sequence of IoRingStreamBuffer::BufferView slices.
   */
  struct Fragment {
   public:
    /** \brief Returns the contents of this queue as a Seq of const BufferView&.
     */
    auto as_seq() const noexcept
    {
      return batt::as_seq(this->views_);
    }

    /** \brief Returns true iff this sequence is empty.
     */
    bool empty() const noexcept;

    /** \brief Returns the number of BufferView slices in this queue.
     */
    usize view_count() const noexcept;

    /** \brief Returns the total bytes count across all views in this.
     */
    usize byte_size() const noexcept;

    /** \brief Pushes the specified BufferView onto the end of this sequence.
     */
    void push(BufferView&& view);

    /** \brief Removes up to the specified number of bytes from the beginning of this sequence,
     * returning the resulting BufferView slices as a Fragment.
     */
    Fragment pop(usize max_byte_count);

    /** \brief Consolidates the contents of this Fragment as a single contiguous chunk of
     * data.
     */
    template <typename T>
    ConstBuffer gather(std::variant<IoRingBufferPool::Buffer, T>& storage) const noexcept
    {
      if (this->views_.empty()) {
        return ConstBuffer{};
      }

      if (this->views_.size() == 1) {
        storage.template emplace<IoRingBufferPool::Buffer>(this->views_.front().buffer);
        return this->views_.front().slice;
      }

      T& typed_storage = storage.template emplace<T>();

      return this->gather_impl(resize_buffer_storage(typed_storage, this->byte_size()));
    }

    //----- --- -- -  -  -   -
   private:
    ConstBuffer gather_impl(MutableBuffer dst) const noexcept;

    //----- --- -- -  -  -   -

    batt::SmallVec<BufferView, kMaxBuffersCapacity> views_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingStreamBuffer(IoRingBufferPool& buffer_pool);

  /** \brief IoRingStreamBuffer is not copyable.
   */
  IoRingStreamBuffer(const IoRingStreamBuffer&) = delete;

  /** \brief IoRingStreamBuffer is not copyable.
   */
  IoRingStreamBuffer& operator=(const IoRingStreamBuffer&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the maximum size (in bytes) that the stream buffer can hold.
   */
  usize max_size() const noexcept
  {
    return this->queue_capacity_ * this->buffer_size();
  }

  /** \brief Returns the current size (in bytes) of the buffered data available in this stream.
   */
  usize size() const noexcept;

  /** \brief Closes the stream for writing.  If there is data in the stream, it is still consumable;
   * the stream is now in "draining" mode.
   */
  void close();

  /** \brief Allocate from the pool a buffer that will later be inserted into the stream via
   * IoRingStreamBuffer::commit.
   *
   * NOT SAFE to invoke concurrently on the same object.
   */
  StatusOr<PreparedView> prepare();

  /** \brief Inserts part of all of a previously allocated IoRingBufferPool::Buffer into the stream.
   * BufferView objects committed to the stream using this function must be adjacent,
   * in-order slices of Buffers allocated by `this->prepare()`.
   */
  void commit(BufferView&& view);

  /** \brief Consume a specific range of the stream.  Will block until this range is available.
   */
  StatusOr<Fragment> consume(i64 start, i64 end);

  /** \brief Returns (and removes) the next non-empty sequence of available committed data from the
   * stream.
   */
  StatusOr<Fragment> consume_some();

  /** \brief The size of the buffers in the pool from which this stream allocates.
   */
  usize buffer_size() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Checks to see if the stream has been closed for write _and_ all data has been drained;
   * if both of these conditions are met, then all waiting consumers are unblocked (with kClosed
   * status code).
   */
  void check_for_end_of_stream(batt::ScopedLock<Fragment>& locked);

  /** \brief Performs one-time initialization of this->private_buffer_pool_.
   */
  Status initialize() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const usize queue_capacity_ = kMaxBuffersCapacity;

  /** \brief The pool from which registered memory buffers are allocated.
   */
  IoRingBufferPool& shared_buffer_pool_;

  /** \brief A subpool of `this->shared_buffer_pool_` that guarantees that the maximum number of
   * buffers will be available for allocation via this->prepare.
   */
  Optional<IoRingBufferPool> private_buffer_pool_;

  /** \brief The committed data in the stream.
   */
  batt::Mutex<Fragment> queue_;

  /** \brief The offset in bytes of the first byte in `this->queue_`, relative to the start of the
   * stream.
   */
  batt::Watch<i64> consume_pos_;

  /** \brief One past the offset in bytes of last byte in `this->queue_`, relative to the start of
   * the stream.  This represents the stream offset of the _next_ slice of data that will be passed
   * to this->commit.
   */
  batt::Watch<i64> commit_pos_;

  /** \brief Set to true when `this->close()` is called.  Used by `this->check_for_end_of_stream()`
   * to detect when we are transitioning from "normal" (open) to "draining"
   * (closed-for-write/commit) to "drained" (fully-closed/end-of-stream) states.
   */
  std::atomic<bool> end_of_stream_{false};
};

}  //namespace llfs

#endif  // LLFS_IORING_STREAM_BUFFER_HPP
