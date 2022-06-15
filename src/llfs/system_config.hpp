//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SYSTEM_CONFIG_HPP
#define LLFS_SYSTEM_CONFIG_HPP

#include <llfs/int_types.hpp>

namespace llfs {

// Get the system's memory page size; all mapped address segments must be aligned to this value.
//
usize system_page_size();

// Compute `count` rounded DOWN to the nearest multiple of `system_page_size()`.
//
usize round_down_to_page_size_multiple(usize count);

// Compute `count` rounded UP to the nearest multiple of `system_page_size()`.
//
usize round_up_to_page_size_multiple(usize count);

}  // namespace llfs

#endif  // LLFS_SYSTEM_CONFIG_HPP
