//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_REF_HPP
#define LLFS_REF_HPP

#include <batteries/ref.hpp>

namespace llfs {

using ::batt::as_cref;
using ::batt::as_ref;
using ::batt::into_cref;
using ::batt::into_ref;
using ::batt::Ref;

}  // namespace llfs

#endif  // LLFS_REF_HPP
