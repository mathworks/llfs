//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLOT_HPP
#define LLFS_SLOT_HPP

#include <llfs/interval.hpp>
#include <llfs/page_layout.hpp>
#include <llfs/varint.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/buffer.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/stream_util.hpp>
#include <batteries/suppress.hpp>

namespace llfs {

// Slots have a maximum capacity of 4GB.
//
using slot_size_type = u32;

constexpr u64 kSlotDistanceUpperBound = (u64{1} << 63);
constexpr u64 kMaxSlotDistance = kSlotDistanceUpperBound - 1;

BATT_SUPPRESS_IF_GCC("-Wmaybe-uninitialized")

// Returns true iff `first` is strictly less than `second`.
//
inline bool slot_less_than(slot_offset_type first, slot_offset_type second)
{
  const u64 intra_distance = second - first - 1;
  return intra_distance < kSlotDistanceUpperBound;
}

// Returns true iff `first` is strictly greater than `second`.
//
inline bool slot_greater_than(slot_offset_type first, slot_offset_type second)
{
  return slot_less_than(second, first);
}

// Returns true iff `first` is less than or equal to `second`.
//
inline bool slot_less_or_equal(slot_offset_type first, slot_offset_type second)
{
  return !slot_greater_than(first, second);
}

// Returns true iff `first` is less than or equal to `second`.
//
inline bool slot_at_most(slot_offset_type first, slot_offset_type second)
{
  return slot_less_or_equal(first, second);
}

// Returns true iff `first` is greater than or equal to `second`.
//
inline bool slot_greater_or_equal(slot_offset_type first, slot_offset_type second)
{
  return !slot_less_than(first, second);
}

// Returns true iff `first` is greater than or equal to `second`.
//
inline bool slot_at_least(slot_offset_type first, slot_offset_type second)
{
  return slot_greater_or_equal(first, second);
}

struct SlotLess {
  using result_type = bool;

  bool operator()(slot_offset_type first, slot_offset_type second) const
  {
    return slot_less_than(first, second);
  }
};

struct SlotGreater {
  using result_type = bool;

  bool operator()(slot_offset_type first, slot_offset_type second) const
  {
    return slot_greater_than(first, second);
  }
};

BATT_UNSUPPRESS_IF_GCC()

inline slot_offset_type slot_min(slot_offset_type first, slot_offset_type second)
{
  if (slot_less_than(first, second)) {
    return first;
  }
  return second;
}

inline slot_offset_type slot_max(slot_offset_type first, slot_offset_type second)
{
  if (slot_less_than(first, second)) {
    return second;
  }
  return first;
}

inline std::size_t slot_distance(slot_offset_type x, slot_offset_type y)
{
  if (slot_less_than(y, x)) {
    return slot_distance(y, x);
  }
  return y - x;
}

// Sets `active_offset` to at least `min_offset`.  Returns the distance between the old offset and
// the new offset, if any.
//
inline slot_offset_type clamp_min_slot(batt::Watch<slot_offset_type>& active_offset,
                                       slot_offset_type min_offset)
{
  slot_offset_type delta = 0;
  active_offset.modify_if(
      [min_offset, &delta](slot_offset_type current_offset) -> Optional<slot_offset_type> {
        delta = slot_distance(current_offset, new_offset);
        if (slot_less_than(current_offset, min_offset)) {
          return min_offset;
        }
        return None;
      });
  return delta;
}

BATT_IF_GCC(BATT_SUPPRESS("-Wmaybe-uninitialized"));

inline void clamp_min_slot(Optional<slot_offset_type>* target, slot_offset_type min_offset)
{
  BATT_CHECK_NOT_NULLPTR(target);
  if (*target) {
    **target = slot_max(**target, min_offset);
  } else {
    target->emplace(min_offset);
  }
}

BATT_IF_GCC(BATT_UNSUPPRESS());

inline void clamp_min_slot(slot_offset_type* target, slot_offset_type min_offset)
{
  BATT_CHECK_NOT_NULLPTR(target);
  *target = slot_max(*target, min_offset);
}

// Convenience function; wait for a slot offset watch to reach a certain minimum value.
//
inline StatusOr<slot_offset_type> await_slot_offset(const slot_offset_type min_offset,
                                                    batt::Watch<slot_offset_type>& active_offset)
{
  return active_offset.await_true([min_offset](slot_offset_type current_offset) {
    return !slot_less_than(current_offset, min_offset);
  });
}

using SlotRange = Interval<slot_offset_type>;

struct SlotLowerBoundGreater {
  using result_type = bool;

  bool operator()(const SlotRange& first, const SlotRange& second) const
  {
    return slot_less_than(second.lower_bound, first.lower_bound);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline constexpr slot_offset_type slot_relative_min(slot_offset_type offset)
{
  return offset - kMaxSlotDistance;
}

inline constexpr slot_offset_type slot_relative_max(slot_offset_type offset)
{
  return offset + kMaxSlotDistance;
}
//
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

// Return the union of the passed slot ranges.
//
SlotRange merge_slot_ranges(const Optional<SlotRange>& first, const SlotRange& second);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

inline slot_offset_type get_slot_offset(slot_offset_type slot_offset)
{
  return slot_offset;
}

inline slot_offset_type get_slot_offset(PackedSlotOffset packed)
{
  return packed.value();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct SlotOffsetOrder {
  template <typename First, typename Second>
  bool operator()(const First& first, const Second& second) const
  {
    return slot_less_than(get_slot_offset(first), get_slot_offset(second));
  }
};

struct SlotOffsetPriority {
  template <typename First, typename Second>
  bool operator()(const First& first, const Second& second) const
  {
    return slot_greater_than(get_slot_offset(first), get_slot_offset(second));
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

// Partial/complete specification of a SlotRange.
//
struct SlotRangeSpec {
  Optional<slot_offset_type> lower_bound;
  Optional<slot_offset_type> upper_bound;

  static SlotRangeSpec from(const SlotRange& slot_range) noexcept
  {
    return SlotRangeSpec{
        .lower_bound = slot_range.lower_bound,
        .upper_bound = slot_range.upper_bound,
    };
  }
};

inline std::ostream& operator<<(std::ostream& out, const SlotRangeSpec& t)
{
  return out << "SlotRangeSpec{.lower_bound=" << t.lower_bound << ", .upper_bound=" << t.upper_bound
             << ",}";
}

template <typename T>
struct SlotWithPayload {
  SlotRange slot_range;
  T payload;
};
// TODO [tastolfi 2022-01-04] SlotReader should pass `SlotWithPayload` ?

template <typename T>
inline std::ostream& operator<<(std::ostream& out, const SlotWithPayload<T>& t)
{
  return out << "{.slot_range=" << t.slot_range <<            //
         ", .payload=" << batt::make_printable(t.payload) <<  //
         ",}";
}

constexpr usize kMaxSlotHeaderSize = kMaxVarInt32Size;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief Returns the input SlotRange by const ref.
 */
inline const SlotRange& get_slot_range(const SlotRange& s) noexcept
{
  return s;
}

/** \brief Returns the slot range of the input.
 */
template <typename T>
inline const SlotRange& get_slot_range(const SlotWithPayload<T>& s) noexcept
{
  return s.slot_range;
}

/** \brief Defines the partial order over all SlotRange values such that for any pair of SlotRanges
 * (a, b), a < b iff a.upper_bound <= b.lower_bound.
 */
struct SlotRangeOrder {
  template <typename First, typename Second>
  bool operator()(const First& first, const Second& second) const
  {
    return slot_less_or_equal(get_slot_range(first).upper_bound,
                              get_slot_range(second).lower_bound);
  }
};

}  // namespace llfs

#endif  // LLFS_SLOT_HPP
