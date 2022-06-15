//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLOT_SEQUENCER_HPP
#define LLFS_SLOT_SEQUENCER_HPP

#include <llfs/slot.hpp>

#include <batteries/async/latch.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/stream_util.hpp>

#include <functional>
#include <memory>
#include <ostream>

namespace llfs {

// Used to enforce orderings between slots in a log.
//
// Example:
// ```
// llfs::SlotSequencer seq1;
//
// // Will not block because seq1 is the first in its sequence.
// //
// llfs::StatusOr<llfs::SlotRange> slot0 = seq1.await_prev();
// BATT_CHECK_EQ(slot0, llfs::SlotRange{0, 0});
//
// // We can create a second sequencer ordered after seq1 by calling
// // `get_next()`.
// //
// llfs::SlotSequencer seq2 = seq1.get_next();
//
// // If we call `await_prev()` on seq2, it will block until `set_current()`
// // is called on seq1 to establish the position of slot1.  If we want to
// // do a non-blocking poll for the previous slot, we can use `poll_prev()`
// // instead.
// //
// BATT_CHECK_EQ(seq2.poll_prev(), batt::None);
//
// seq1.set_current(llfs::SlotRange{10, 20});
// BATT_CHECK(seq2.await_prev().ok());
// BATT_CHECK_EQ(*seq2.await_prev(), llfs::SlotRange{10, 20});
//
// ```
//
class SlotSequencer
{
 public:
  SlotSequencer() = default;

  SlotSequencer(const SlotSequencer&) = default;
  SlotSequencer& operator=(const SlotSequencer&) = default;

  SlotSequencer(SlotSequencer&&) = default;
  SlotSequencer& operator=(SlotSequencer&&) = default;

  bool has_prev() const;

  bool is_resolved() const;

  Optional<SlotRange> poll_prev() const;

  StatusOr<SlotRange> await_prev() const;

  Optional<SlotRange> get_current() const;

  bool set_current(const SlotRange& slot_range);

  bool set_error(batt::Status status);

  SlotSequencer get_next() const;

  std::function<void(std::ostream&)> debug_info() const;

 private:
  batt::SharedPtr<batt::Latch<SlotRange>> prev_;
  batt::SharedPtr<batt::Latch<SlotRange>> current_ = batt::make_shared<batt::Latch<SlotRange>>();
};

}  // namespace llfs

#endif  // LLFS_SLOT_SEQUENCER_HPP
