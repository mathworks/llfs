//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_POINTERS_HPP
#define LLFS_POINTERS_HPP

#include <batteries/pointers.hpp>

namespace llfs {

using ::batt::NoopDeleter;
using ::batt::UniqueNonOwningPtr;

}  // namespace llfs

#endif  // LLFS_POINTERS_HPP
