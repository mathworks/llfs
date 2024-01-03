//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_buffer_view.hpp>
//

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class IoRingStreamBuffer::BufferView

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingBufferView IoRingBufferView::split(usize byte_offset) noexcept
{
  byte_offset = std::min(byte_offset, this->slice.size());

  IoRingBufferView prefix{
      .buffer = this->buffer,
      .slice = ConstBuffer{this->slice.data(), byte_offset},
  };

  this->slice += byte_offset;

  return prefix;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingBufferView::can_merge_with(const IoRingBufferView& other) const noexcept
{
  return this->buffer == other.buffer  //
         && get_buffer_end(this->slice) == other.slice.data();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingBufferView::merge_with(const IoRingBufferView& other) noexcept
{
  if (!this->can_merge_with(other)) {
    return false;
  }
  this->slice = ConstBuffer{
      this->slice.data(),
      this->slice.size() + other.slice.size(),
  };
  return true;
}

}  //namespace llfs
