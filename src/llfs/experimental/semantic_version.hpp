//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SEMANTIC_VERSION_HPP
#define LLFS_SEMANTIC_VERSION_HPP

#include <llfs/int_types.hpp>
#include <llfs/simple_packed_type.hpp>

#include <batteries/static_assert.hpp>

namespace llfs {

struct PackedSemanticVersion {
  big_u32 major;
  big_u16 minor;
  big_u16 patch;
};

LLFS_SIMPLE_PACKED_TYPE(PackedSemanticVersion);

BATT_STATIC_ASSERT_EQ(sizeof(PackedSemanticVersion), 8);

}  // namespace llfs

#endif  // LLFS_SEMANTIC_VERSION_HPP
