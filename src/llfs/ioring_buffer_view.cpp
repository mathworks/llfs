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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SliceT>
auto BasicIoRingBufferView<SliceT>::split(usize byte_offset) noexcept -> Self
{
  byte_offset = std::min(byte_offset, this->slice.size());

  Self prefix{
      this->buffer,
      SliceT{this->slice.data(), byte_offset},
  };

  this->slice += byte_offset;

  return prefix;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SliceT>
bool BasicIoRingBufferView<SliceT>::can_merge_with(const Self& other) const noexcept
{
  return this->buffer == other.buffer  //
         && get_buffer_end(this->slice) == other.slice.data();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SliceT>
bool BasicIoRingBufferView<SliceT>::merge_with(const Self& other) noexcept
{
  if (!this->can_merge_with(other)) {
    return false;
  }
  this->slice = SliceT{
      this->slice.data(),
      this->slice.size() + other.slice.size(),
  };
  return true;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Explicitly specialize for MutableBuffer and ConstBuffer only.

template class BasicIoRingBufferView<ConstBuffer>;
template class BasicIoRingBufferView<MutableBuffer>;

}  //namespace llfs
