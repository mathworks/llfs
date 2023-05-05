//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_OPTIONS_HPP
#define LLFS_VOLUME_OPTIONS_HPP

#include <llfs/config.hpp>
#include <llfs/optional.hpp>

#include <batteries/strong_typedef.hpp>

#include <boost/uuid/uuid.hpp>

#include <string>

namespace llfs {

// The number of WAL bytes (on average) between updates the to SlotReadSlot that controls WAL
// trimming.
//
BATT_STRONG_TYPEDEF(u64, TrimLockUpdateInterval);

struct VolumeOptions {
  static constexpr usize kMaxNameLength = 160;

  std::string name;

  Optional<boost::uuids::uuid> uuid;

  MaxRefsPerPage max_refs_per_page;

  TrimLockUpdateInterval trim_lock_update_interval;

  TrimDelayByteCount trim_delay_byte_count;
};

}  // namespace llfs

#endif  // LLFS_VOLUME_OPTIONS_HPP
