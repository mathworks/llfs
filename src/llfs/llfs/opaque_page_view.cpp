#include <llfs/opaque_page_view.hpp>
//

#include <batteries/seq/empty.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageLayoutId OpaquePageView::get_page_layout_id() const /*override*/
{
  const static PageLayoutId rec_ = [] {
    llfs::PageLayoutId rec;

    const char tag[sizeof(rec.value) + 1] = "(opaque)";

    std::memcpy(&rec.value, tag, sizeof(rec.value));

    return rec;
  }();

  return rec_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> OpaquePageView::trace_refs() const /*override*/
{
  return seq::Empty<PageId>{} | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> OpaquePageView::min_key() const /*override*/
{
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> OpaquePageView::max_key() const /*override*/
{
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::shared_ptr<PageFilter> OpaquePageView::build_filter() const /*override*/
{
  return std::make_shared<NullPageFilter>(this->page_id());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void OpaquePageView::dump_to_ostream(std::ostream& out) const /*override*/
{
  out << "(?)";
}

}  // namespace llfs
