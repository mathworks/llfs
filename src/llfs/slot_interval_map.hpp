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

#include <llfs/slot.hpp>

#include <batteries/interval.hpp>

#include <map>
#include <vector>

namespace llfs {

using OffsetRange = batt::Interval<isize>;

class SlotIntervalMap
{
 public:
  std::vector<OffsetRange> insert(OffsetRange insert_range, slot_offset_type insert_slot)
  {
    std::vector<OffsetRange> updated_ranges;
    auto [first, last] = this->map_.equal_range(insert_range);
    for (;;) {
      // First handle exit conditions: no more input, no more overlapping/conflicting current state.
      //
      if (insert_range.empty()) {
        break;
      }
      if (first == last) {
        this->map_.emplace(insert_range, insert_slot);
        updated_ranges.emplace_back(insert_range);
        break;
      }

      // Unpack the current segment.
      //
      auto& [current_range, current_slot] = *first;

      auto next = std::next(first);

      if (insert_slot == current_slot) {
        // Increase the current change to include its intersection with the insert range.
        //
        const OffsetRange updated_current_range =
            insert_range.intersection_with(current_range).union_with(current_range);

        updated_ranges.emplace_back(updated_current_range);

        this->map_.erase(first);
        this->map_.emplace(updated_current_range, insert_slot);

        insert_range.lower_bound = updated_current_range.upper_bound;
        BATT_CHECK_GE(insert_range.size(), 0);

      } else if (slot_greater_than(insert_slot, current_slot)) {
        // The slot being inserted is greater, so wherever the insert range intersects the current
        // one, replace/overwrite the slot.
        //
        const OffsetRange replace_range = insert_range.intersection_with(current_range);

        updated_ranges.emplace_back(replace_range);

        // If there are any parts of the current range that don't intersect the insert range, we
        // need to keep the slot as it is.
        //
        const batt::SmallVec<OffsetRange, 2> preserve_ranges = current_range.without(insert_range);

        // Save the current slot value for the current range before we remove the current entry from
        // the map.
        //
        const slot_offset_type current_slot_value = current_slot;
        this->map_.erase(first);

        // Insert the new slot.
        //
        this->map_.emplace(replace_range, insert_slot);

        // Re-insert the parts of the current range that are still up-to-date.
        //
        for (const OffsetRange& range : preserve_ranges) {
          this->map_.emplace(range, current_slot_value);
        }

        // Update the insert range.
        //
        insert_range.lower_bound = replace_range.upper_bound;
        BATT_CHECK_GE(insert_range.size(), 0);

      } else {
        BATT_CHECK(slot_less_than(insert_slot, current_slot));

        // Simplest case; no change to `map_`; just trim the insert range.
        //
        insert_range.lower_bound =
            std::min<isize>(current_range.upper_bound, insert_range.upper_bound);
      }

      first = next;
    }
    return updated_ranges;
  }

  std::map<OffsetRange, slot_offset_type, OffsetRange::LinearOrder> map_;
};

}  // namespace llfs

#endif  // LLFS_SLOT_INTERVAL_MAP_HPP
