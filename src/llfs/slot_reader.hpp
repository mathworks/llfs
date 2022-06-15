//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLOT_READER_HPP
#define LLFS_SLOT_READER_HPP

#include <llfs/buffer.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/log_device.hpp>
#include <llfs/seq.hpp>
#include <llfs/slot_parse.hpp>

#include <batteries/async/types.hpp>
#include <batteries/type_traits.hpp>

#include <glog/logging.h>

#include <functional>

namespace llfs {

using PreSlotFn = std::function<seq::LoopControl(slot_offset_type)>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Invokes a visitor function for each slot in a log.
//
class SlotReader
{
 public:
  SlotReader(const SlotReader&) = delete;
  SlotReader& operator=(const SlotReader&) = delete;

  explicit SlotReader(LogDevice::Reader& log_reader) noexcept;

  void halt();

  slot_offset_type get_consumed_upper_bound() const;

  StatusOr<slot_offset_type> await_consumed_upper_bound(slot_offset_type min_offset);

  void skip(slot_offset_type byte_count);

  // Set a function to be called immediately before each slot parse attempt.
  //
  void set_pre_slot_fn(PreSlotFn&& pre_slot_fn);

  // Parse and consume events from a LogDevice.
  //
  // Args:
  //    log_reader - source from which to read log slots
  //    wait_for_data - whether to operate in blocking mode (true) or non-blocking (false; i.e.,
  //    return as soon as
  //        buffered data is consumed from `log_reader`)
  //    slot_type - a PackedVariant static type token representing the possible slot types for the
  //    given log visitor - a function with signature `Status visitor(SlotRange, const T&)` (see
  //    below)
  //
  // This function will loop reading objects of type `slot_type` from `log_reader` until one of the
  // following occurs:
  //
  //  - the LogDevice is closed
  //  - a parsing error occurs (`visitor` returns non-ok; see below)
  //  - `wait_for_data` is `WaitForResource::kFalse` and no more data can be read from the log
  //  without blocking
  //
  // The passed `visitor` function is invoked for each slot.  It should be an overloaded function
  // that can be invoked with any of the types `Ts...` that are part of the PackedVariant type
  // `slot_type`.  `visitor` must return `Status` to indicate whether or not to continue parsing and
  // consuming log data.  If the `visitor` returns a non-ok status, `read_log_slots` will return
  // that status immediately (without consuming the slot for which the visitor returned non-ok).
  //
  template <typename /*Status(const SlotParse&)*/ VisitorFn>
  StatusOr<usize> run(batt::WaitForResource wait_for_data, VisitorFn&& visitor)
  {
    BATT_DEBUG_INFO("SlotReader::run()");

    usize slot_count = 0;

    this->consumed_upper_bound_.set_value(this->log_reader_.slot_offset());

    for (;;) {
      StatusOr<SlotParse> parsed = this->parse_next(wait_for_data);
      if (!parsed.ok() && ((parsed.status() == batt::StatusCode::kLoopBreak) ||
                           (parsed.status() == StatusCode::kBreakSlotReaderLoop) ||
                           (parsed.status() == StatusCode::kSlotReaderOutOfData &&
                            wait_for_data == batt::WaitForResource::kFalse))) {
        break;
      }
      BATT_REQUIRE_OK(parsed);

      Status visitor_status = visitor(*parsed);
      BATT_REQUIRE_OK(visitor_status);

      this->consume_slot(*parsed);
      slot_count += 1;
    }

    return slot_count;
  }

 private:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<SlotParse> parse_next(batt::WaitForResource wait_for_data);

  void consume_slot(const SlotParse& slot);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The log from which this object reads slots.
  //
  LogDevice::Reader& log_reader_;

  // A function to be called before each slot; its return value determines whether the reader
  // continues or breaks out of the read loop.
  //
  PreSlotFn pre_slot_fn_;

  // Tracks the slot offset upper bound consumed by this reader as it goes.  The reader calls
  // `consume` after each slot.
  //
  batt::Watch<slot_offset_type> consumed_upper_bound_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
class TypedSlotReader;

template <typename... Ts>
class TypedSlotReader<PackedVariant<Ts...>> : public SlotReader
{
 public:
  using Super = SlotReader;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename... CaseHandlerFns>
  static Status visit_slot(const SlotParse& slot, const std::string_view& data,
                           CaseHandlerFns&&... case_handler_fns)
  {
    DataReader data_reader{data};

    Status visitor_status;

    const bool ok = data_reader.read_variant(  //
        batt::StaticType<PackedVariant<Ts...>>{},
        [&](const PackedVariant<Ts...>& head, const auto& tail) {
          auto unpacked_object = unpack_object(tail, &data_reader);

          if (!unpacked_object.ok()) {
            visitor_status = unpacked_object.status();
            return;
          }

          visitor_status =
              batt::make_case_of_visitor(BATT_FORWARD(case_handler_fns)...)(slot, *unpacked_object);
        });

    if (!ok) {
      return {StatusCode::kBadPackedVariant};
    }

    return visitor_status;
  }

  template <typename... CaseHandlerFns>
  static auto make_slot_visitor(CaseHandlerFns&&... case_handler_fns)
  {
    return [&](const SlotParse& slot, const std::string_view& user_data) {
      return TypedSlotReader<PackedVariant<Ts...>>::visit_slot(slot, user_data,
                                                               BATT_FORWARD(case_handler_fns)...);
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using SlotReader::SlotReader;

  template <typename /*Status(const SlotParse&, const CaseT& payload)*/ VisitorFn>
  StatusOr<usize> run(batt::WaitForResource wait_for_data, VisitorFn&& visitor)
  {
    return this->Super::run(wait_for_data, [&visitor](const SlotParse& slot) mutable -> Status {
      return TypedSlotReader::visit_slot(slot, slot.body, BATT_FORWARD(visitor));
    });
  }
};

}  // namespace llfs

#endif  // LLFS_SLOT_READER_HPP
