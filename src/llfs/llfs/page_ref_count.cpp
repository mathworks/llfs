#include <llfs/page_ref_count.hpp>
//

#include <batteries/hash.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator==(const PageRefCount& l, const PageRefCount& r)
{
  return l.page_id == r.page_id && l.ref_count == r.ref_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize hash_value(const PageRefCount& prc)
{
  return batt::hash(prc.page_id, prc.ref_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PageRefCount& t)
{
  out << "{.page=" << std::hex << t.page_id << std::dec << ", .delta=";
  if (t.ref_count > 0) {
    out << "+";
  }
  return out << t.ref_count << ",}";
}

}  // namespace llfs
