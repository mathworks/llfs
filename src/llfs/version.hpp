//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VERSION_HPP
#define LLFS_VERSION_HPP

#include <llfs/int_types.hpp>

namespace llfs {

constexpr inline u64 make_version_u64(u32 major, u16 minor, u16 patch)
{
  return ((u64{major} & 0xffffffffull) << 32) | ((u64{minor} & 0xffffull) << 16) |
         (u64{patch} & 0xffffull);
}

}  // namespace llfs

#endif  // LLFS_VERSION_HPP
