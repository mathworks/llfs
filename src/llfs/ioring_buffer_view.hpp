//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_BUFFER_VIEW_HPP
#define LLFS_IORING_BUFFER_VIEW_HPP

#include <llfs/config.hpp>
//
#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_buffer_pool.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A read-only view (slice) of a pooled buffer.
 */
template <typename SliceT>
struct BasicIoRingBufferView {
  using Self = BasicIoRingBufferView;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRingBufferPool::Buffer buffer;
  SliceT slice;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BasicIoRingBufferView() = default;

  explicit BasicIoRingBufferView(IoRingBufferPool::Buffer&& buffer, const SliceT& slice) noexcept
      : buffer{std::move(buffer)}
      , slice{slice}
  {
  }

  explicit BasicIoRingBufferView(const IoRingBufferPool::Buffer& buffer,
                                 const SliceT& slice) noexcept
      : buffer{buffer}
      , slice{slice}
  {
  }

  template <typename SliceU, typename = std::enable_if_t<!std::is_same_v<SliceU, SliceT>>>
  /*implicit*/ BasicIoRingBufferView(const BasicIoRingBufferView<SliceU>& other) noexcept
      : buffer{other.buffer}
      , slice{other.slice}
  {
  }

  template <typename SliceU, typename = std::enable_if_t<!std::is_same_v<SliceU, SliceT>>,
            typename = void>
  /*implicit*/ BasicIoRingBufferView(BasicIoRingBufferView<SliceU>&& other) noexcept
      : buffer{std::move(other.buffer)}
      , slice{other.slice}
  {
  }

  BasicIoRingBufferView(const Self&) = default;
  BasicIoRingBufferView& operator=(const Self&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a pointer to the start of the viewed data (`this->slice.data()`).
   */
  auto* data() const noexcept
  {
    return this->slice.data();
  }

  /** \brief Returns the size of the viewed data (`this->slice.size()`).
   */
  usize size() const noexcept
  {
    return this->slice.size();
  }

  /** \brief Returns true iff this->size() is 0.
   */
  bool empty() const noexcept
  {
    return this->size() == 0;
  }

  /** \brief Returns a new buffer view containing the prefix of `this` ending at
   * `byte_offset`; `this` is adjusted to start at `byte_offset` (immediately after the
   * returned prefix).
   *
   * If byte_offset is after the end of `this->slice`, then it is automatically truncated to
   * `this->slice.size()`, a copy of `this` is returned, and `this` becomes empty.
   */
  Self split(usize byte_offset) noexcept;

  /** \brief Returns true iff other is a view of the same Buffer as this, and other.slice
   * comes immediately after this->slice.
   */
  bool can_merge_with(const Self& other) const noexcept;

  /** \brief If this->can_merge_with(other) would return true, then this view's slice is
   * extended to include other.slice and true is returned.  Otherwise returns false.
   */
  bool merge_with(const Self& other) noexcept;
};

using IoRingConstBufferView = BasicIoRingBufferView<ConstBuffer>;
using IoRingMutableBufferView = BasicIoRingBufferView<MutableBuffer>;

}  //namespace llfs

#endif  // LLFS_IORING_BUFFER_VIEW_HPP
