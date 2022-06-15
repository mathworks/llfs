//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_parse.hpp>
//

#include <batteries/stream_util.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const SlotParse& t)
{
  return out << "SlotParse{.offset=" << t.offset << ", .slot_size=" << t.offset.size()
             << ", .body=" << batt::c_str_literal(t.body) << ", .body_size=" << t.body.size()
             << ", .depends_on=" << t.depends_on_offset << ",}";
}

}  // namespace llfs
