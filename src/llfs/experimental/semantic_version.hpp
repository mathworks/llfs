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

#include <batteries/strong_typedef.hpp>

namespace llfs {

/* TODO [tastolfi 2022-02-07] ?
BATT_STRONG_TYPEDEF(u32, MajorVersion);
BATT_STRONG_TYPEDEF(u16, MinorVersion);
BATT_STRONG_TYPEDEF(u16, PatchVersion);

struct SemanticVersion {
  MajorVersion major;
  MinorVersion minor;
  PatchVersion patch;
};
*/

struct PackedSemanticVersion {
  big_u32 major;
  big_u16 minor;
  big_u16 patch;
};

LLFS_SIMPLE_PACKED_TYPE(PackedSemanticVersion);

}  // namespace llfs

#endif  // LLFS_SEMANTIC_VERSION_HPP
