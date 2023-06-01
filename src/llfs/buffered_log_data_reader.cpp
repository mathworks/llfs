//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/buffered_log_data_reader.hpp>
//

#include <batteries/case_of.hpp>

namespace llfs {

/*explicit*/ BufferedLogDataReader::BufferedLogDataReader(slot_offset_type slot_offset,
                                                          const ConstBuffer& buffer) noexcept
    : slot_offset_{slot_offset}
    , buffer_{buffer}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool BufferedLogDataReader::is_closed() /*override*/
{
  return false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer BufferedLogDataReader::data() /*override*/
{
  return this->buffer_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type BufferedLogDataReader::slot_offset() /*override*/
{
  return this->slot_offset_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void BufferedLogDataReader::consume(usize byte_count) /*override*/
{
  byte_count = std::min(this->buffer_.size(), byte_count);
  this->buffer_ += byte_count;
  this->slot_offset_ += byte_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status BufferedLogDataReader::await(ReaderEvent event) /*override*/
{
  return batt::case_of(
      event,

      //----- --- -- -  -  -   -
      [this](const SlotUpperBoundAt& slot_upper_bound_at) -> Status {
        if (slot_less_than(this->slot_offset_ + this->buffer_.size(), slot_upper_bound_at.offset)) {
          return batt::StatusCode::kOutOfRange;
        }
        return batt::OkStatus();
      },

      //----- --- -- -  -  -   -
      [this](const BytesAvailable& bytes_available) -> Status {
        if (bytes_available.size <= this->buffer_.size()) {
          return batt::OkStatus();
        }
        return batt::StatusCode::kOutOfRange;
      });
}

}  // namespace llfs
