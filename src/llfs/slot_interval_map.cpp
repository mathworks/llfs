//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_interval_map.hpp>
//

#include <llfs/logging.hpp>
#include <llfs/seq.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const SlotIntervalMap& t)
{
  out << "{";

  t.to_seq() | seq::for_each([&out](const auto& entry) {
    out << entry.offset_range << " => " << entry.slot << ", ";
  });

  out << "}";

  return out;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<SlotIntervalMap::Entry> SlotIntervalMap::to_seq() const
{
  return ::llfs::as_seq(this->map_.begin(), this->map_.end())  //
         | seq::map([](const auto& kv_pair) {
             return Entry{
                 .offset_range = kv_pair.first,
                 .slot = kv_pair.second,
             };
           })  //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<slot_offset_type> SlotIntervalMap::to_vec() const
{
  if (this->map_.empty()) {
    return {};
  }

  const isize offset_upper_bound = this->map_.rbegin()->first.upper_bound;
  std::vector<slot_offset_type> values(offset_upper_bound, 0);

  for (auto& [interval, slot] : this->map_) {
    for (isize i = interval.lower_bound; i < interval.upper_bound; ++i) {
      values[i] = slot;
    }
  }

  return values;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::SmallVec<SlotIntervalMap::Entry, 2> SlotIntervalMap::query(OffsetRange query_offsets) const
{
  batt::SmallVec<Entry, 2> result;

  auto [first, last] = this->map_.equal_range(query_offsets);
  for (; first != last; ++first) {
    const auto& [offsets, slot] = *first;
    result.emplace_back(Entry{
        .offset_range = offsets.intersection_with(query_offsets),
        .slot = slot,
    });
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotIntervalMap::update(OffsetRange update_offsets, slot_offset_type update_slot)
{
  LLFS_DVLOG(1) << "update(" << BATT_INSPECT(update_offsets) << "," << BATT_INSPECT(update_slot)
                << ")";

  if (update_offsets.empty()) {
    LLFS_DVLOG(1) << "Update range is empty; nothing to do.";
    return;
  }

  auto [first, last] = this->map_.equal_range(update_offsets);

  //----- --- -- -  -  -   -
  // Check to see whether we can merge the update range with an existing range preceeding or
  // following.

  // If there is a previous entry whose offset range is adjacent to update_offsets and whose slot
  // is the same, then remove it and simply adjust the update_offsets to include its offset range.
  //
  LLFS_DVLOG(1) << " -- Checking for merge-with-prev";
  if (first != this->map_.begin()) {
    auto prev = std::prev(first);
    LLFS_DVLOG(1) << " ---- prev: " << prev->first << " => " << prev->second;
    if (prev->first.upper_bound == update_offsets.lower_bound && prev->second == update_slot) {
      update_offsets.lower_bound = prev->first.lower_bound;
      this->map_.erase(prev);
      LLFS_DVLOG(1) << " ---- Extending update_offsets: " << update_offsets;
    }
  } else {
    LLFS_DVLOG(1) << " ---- No prev segment";
  }

  // If there is a next entry whose offset range is adjacent to update_offsets and whose slot
  // is the same, then remove it and simply adjust the update_offsets to include its offset range.
  //
  LLFS_DVLOG(1) << " -- Checking for merge-with-next";
  if (last != this->map_.end()) {
    auto next = last;
    LLFS_DVLOG(1) << " ---- next: " << next->first << " => " << next->second;
    if (update_offsets.upper_bound == next->first.lower_bound && next->second == update_slot) {
      update_offsets.upper_bound = next->first.upper_bound;
      last = this->map_.erase(next);
      LLFS_DVLOG(1) << " ---- Extending update_offsets: " << update_offsets;
    }
  } else {
    LLFS_DVLOG(1) << " ---- No next segment";
  }
  //----- --- -- -  -  -   -

  for (;;) {
    // First handle exit conditions: no more input, no more overlapping/conflicting current state.
    //
    if (update_offsets.empty()) {
      LLFS_DVLOG(1) << "Update range is empty; done!";
      break;
    }

    // Second base case: No conflicting ranges exist.
    //
    if (first == last) {
      LLFS_DVLOG(1) << "No more overlapping segments";
      LLFS_DVLOG(1) << " -- Updateing " << update_offsets << " => " << update_slot;
      this->map_.emplace_hint(first, update_offsets, update_slot);
      break;
    }

    const auto next = std::next(first);

    // Unpack the current segment.
    //
    const auto& [current_offsets, current_slot] = *first;

    LLFS_DVLOG(1) << "Processing: " << current_offsets << " => " << current_slot << ";"
                  << BATT_INSPECT(update_offsets);

    if (update_slot == current_slot) {
      // Adjust the update range to include the current one and remove current from the map.
      //
      update_offsets.lower_bound =
          std::min(update_offsets.lower_bound, current_offsets.lower_bound);

      update_offsets.upper_bound =
          std::max(update_offsets.upper_bound, current_offsets.upper_bound);

      this->map_.erase(first);

    } else {
      if (slot_greater_than(update_slot, current_slot)) {
        // If there are any parts of the current range that don't intersect the update range, we
        // need to keep the slot as it is.
        //
        const batt::SmallVec<OffsetRange, 2> preserve_offsets =
            current_offsets.without(update_offsets);

        // Save the current slot value for the current range before we remove the current entry from
        // the map.
        //
        const slot_offset_type current_slot_value = current_slot;
        LLFS_DVLOG(1) << "Removing segment " << current_offsets << " (update_slot is higher)";
        this->map_.erase(first);

        // Re-update the parts of the current range that are still up-to-date.
        //
        for (const OffsetRange& range : preserve_offsets) {
          LLFS_DVLOG(1) << " -- Preserving segment " << range << " => " << current_slot_value;
          this->map_.emplace(range, current_slot_value);
        }
      } else {
        BATT_CHECK(slot_less_than(update_slot, current_slot));

        // If there is some non-empty offset range before the current one, update that into the map.
        //
        if (update_offsets.lower_bound < current_offsets.lower_bound) {
          this->map_.emplace(
              OffsetRange{
                  .lower_bound = update_offsets.lower_bound,
                  .upper_bound = current_offsets.lower_bound,
              },
              update_slot);

          update_offsets.lower_bound = current_offsets.lower_bound;
        }

        // Discard the remaining prefix of the update range that overlaps with the existing higher
        // slot range.
        //
        update_offsets.lower_bound =
            std::min(current_offsets.upper_bound, update_offsets.upper_bound);
      }
    }

    LLFS_DVLOG(1) << "(next segment)" << BATT_INSPECT(update_offsets);
    first = next;
  }
}

}  // namespace llfs
