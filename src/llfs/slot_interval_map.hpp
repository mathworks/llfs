//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLOT_INTERVAL_MAP_HPP
#define LLFS_SLOT_INTERVAL_MAP_HPP

#include <llfs/seq.hpp>
#include <llfs/slot.hpp>

#include <batteries/interval.hpp>
#include <batteries/small_vec.hpp>

#include <map>
#include <vector>

namespace llfs {

using OffsetRange = batt::Interval<isize>;

class SlotIntervalMap;

std::ostream& operator<<(std::ostream& out, const SlotIntervalMap& t);

class SlotIntervalMap
{
 public:
  friend std::ostream& operator<<(std::ostream& out, const SlotIntervalMap& t);

  struct Entry {
    OffsetRange offset_range;
    slot_offset_type slot;
  };

  BoxedSeq<Entry> to_seq() const;

  std::vector<slot_offset_type> to_vec() const;

  batt::SmallVec<Entry, 2> query(OffsetRange query_range) const;

  void update(OffsetRange update_range, slot_offset_type update_slot);

 private:
  std::map<OffsetRange, slot_offset_type, OffsetRange::LinearOrder> map_;
};

}  // namespace llfs

#endif  // LLFS_SLOT_INTERVAL_MAP_HPP
