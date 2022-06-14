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
