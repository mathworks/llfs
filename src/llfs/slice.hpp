//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLICE_HPP
#define LLFS_SLICE_HPP

#include <batteries/slice.hpp>

namespace llfs {

using ::batt::as_const_slice;
using ::batt::as_range;
using ::batt::as_seq;
using ::batt::as_slice;
using ::batt::Slice;

}  // namespace llfs

#endif  // LLFS_SLICE_HPP
