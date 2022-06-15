//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotRange merge_slot_ranges(const Optional<SlotRange>& first, const SlotRange& second)
{
  if (!first) {
    return second;
  }

  return SlotRange{
      .lower_bound = slot_min(first->lower_bound, second.lower_bound),
      .upper_bound = slot_max(first->upper_bound, second.upper_bound),
  };
}

}  // namespace llfs
