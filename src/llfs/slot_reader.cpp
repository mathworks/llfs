//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_reader.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SlotReader::SlotReader(LogDevice::Reader& log_reader) noexcept
    : log_reader_{log_reader}
    , pre_slot_fn_{[](auto&&...) {
      return seq::LoopControl::kContinue;
    }}
    , consumed_upper_bound_{this->log_reader_.slot_offset()}
{
  initialize_status_codes();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotReader::halt()
{
  this->consumed_upper_bound_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type SlotReader::get_consumed_upper_bound() const
{
  return this->consumed_upper_bound_.get_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> SlotReader::await_consumed_upper_bound(slot_offset_type min_offset)
{
  return await_slot_offset(min_offset, this->consumed_upper_bound_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotReader::skip(slot_offset_type byte_count)
{
  BATT_CHECK_LE(byte_count, this->log_reader_.data().size());

  this->log_reader_.consume(byte_count);
  this->consumed_upper_bound_.fetch_add(byte_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotReader::set_pre_slot_fn(PreSlotFn&& pre_slot_fn)
{
  this->pre_slot_fn_ = std::move(pre_slot_fn);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotParse> SlotReader::parse_next(batt::WaitForResource wait_for_data)
{
  usize min_bytes_needed = 0;

  for (;;) {
    if (this->log_reader_.is_closed()) {
      LLFS_VLOG(1) << "reader is closed; returning";
      return {batt::StatusCode::kClosed};
    }

    if (min_bytes_needed > 0) {
      if (wait_for_data != batt::WaitForResource::kTrue) {
        LLFS_VLOG(1) << "out of data; wait_for_data == false, returning";
        return {StatusCode::kSlotReaderOutOfData};
      }
      LLFS_VLOG(1) << "waiting for " << min_bytes_needed << " bytes to be available";
      auto status = this->log_reader_.await(BytesAvailable{
          .size = min_bytes_needed,
      });
      LLFS_VLOG(1) << "got event from reader; " << BATT_INSPECT(status);
      BATT_REQUIRE_OK(status);
    }
    min_bytes_needed = 0;

    ConstBuffer data = this->log_reader_.data();
    DataReader data_reader{data};
    const slot_offset_type current_slot = this->log_reader_.slot_offset();

    BATT_CHECK_EQ(current_slot, this->consumed_upper_bound_.get_value());

    LLFS_VLOG(1) << "data.size() = " << data.size() << ", current_slot = " << current_slot;

    if (seq::LoopControl::kBreak == this->pre_slot_fn_(current_slot)) {
      return {batt::StatusCode::kLoopBreak};
    }

    // Save the reader position before parsing a varint so we know how large the varint was.  This
    // is necessary because we can't count on varints being packed in a minimal fashion.
    //
    const usize bytes_available_before = data_reader.bytes_available();

    Optional<u64> slot_body_size = data_reader.read_varint();
    if (!slot_body_size) {
      min_bytes_needed = data.size() + 1;
      continue;
    }
    const usize bytes_available_after = data_reader.bytes_available();

    BATT_CHECK_NE(*slot_body_size, 0u)
        << BATT_INSPECT(bytes_available_before) << BATT_INSPECT(bytes_available_after)
        << BATT_INSPECT(current_slot) << BATT_INSPECT(data.size())
        << BATT_INSPECT(current_slot + data.size()) << BATT_INSPECT(this->slots_parsed_count_);

    // Calculate the header (varint) size from bytes available before and after.
    //
    BATT_CHECK_GT(bytes_available_before, bytes_available_after);
    const usize slot_header_size = bytes_available_before - bytes_available_after;
    const usize slot_size = slot_header_size + *slot_body_size;

    // Read the specified number of bytes; if this results in a short-read, then return false to
    // signal that we need more data.
    //
    std::string_view slot_body = data_reader.read_raw(*slot_body_size);
    if (slot_body.size() < *slot_body_size) {
      min_bytes_needed = data.size() + (*slot_body_size - slot_body.size());
      continue;
    }

    BATT_CHECK_EQ(slot_body.size(), slot_body_size);

    // Initialize the slot_range according to the log reader's offset attribute; we will update
    // `upper_bound` inside the variant visitor passed to `DataReader::read_variant`.
    //
    return SlotParse{
        .offset =
            SlotRange{
                .lower_bound = current_slot,
                .upper_bound = current_slot + slot_size,
            },
        .body = slot_body,
        .depends_on_offset = None,
    };
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotReader::consume_slot(const SlotParse& slot)
{
  const usize slot_size = slot.offset.size();
  BATT_CHECK_GT(slot_size, 0);
  LLFS_VLOG(1) << "log_reader.consume(" << slot_size << ")";
  this->log_reader_.consume(slot_size);
  this->consumed_upper_bound_.fetch_add(slot_size);
}

}  // namespace llfs
