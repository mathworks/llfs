//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/data_reader.hpp>
//

#include <llfs/varint.hpp>

#include <batteries/slice.hpp>
#include <batteries/stream_util.hpp>

namespace llfs {

Optional<u64> DataReader::read_varint()
{
  if (this->at_end_) {
    return None;
  }

  Optional<u64> n;
  const u8* const unread_end = this->unread_.end();
  const u8* parse_end;
  std::tie(n, parse_end) = unpack_varint_from(this->unread_.begin(), unread_end);

  if (n && parse_end) {
    this->unread_ = boost::iterator_range<const u8*>{parse_end, unread_end};
    return n;
  }

  this->at_end_ = true;
  return None;
}

}  // namespace llfs
