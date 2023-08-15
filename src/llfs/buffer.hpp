//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BUFFER_HPP
#define LLFS_BUFFER_HPP

#include <llfs/int_types.hpp>

#include <batteries/buffer.hpp>

#include <vector>

namespace llfs {

using batt::buffer_from_struct;
using batt::ConstBuffer;
using batt::make_buffer;
using batt::mutable_buffer_from_struct;
using batt::MutableBuffer;
using batt::resize_buffer;

// Returns the distance, in bytes, from `begin` to `end`.  If `end` is less than `begin`, the result
// is negative.
//
inline isize byte_distance(const void* begin, const void* end)
{
  return static_cast<const u8*>(end) - static_cast<const u8*>(begin);
}

/** \brief Returns an empty sequence of ConstBuffer objects.
 */
inline const std::vector<ConstBuffer>& no_buffers()
{
  static const std::vector<ConstBuffer> no_buffers_;

  return no_buffers_;
}

}  // namespace llfs

#endif  // LLFS_BUFFER_HPP
