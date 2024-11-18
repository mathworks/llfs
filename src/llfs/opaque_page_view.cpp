//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/opaque_page_view.hpp>
//

#include <batteries/seq/empty.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ const PageLayoutId& OpaquePageView::page_layout_id() noexcept
{
  const static PageLayoutId id_ = [] {
    llfs::PageLayoutId id;

    const char tag[sizeof(id.value) + 1] = "(opaque)";

    std::memcpy(&id.value, tag, sizeof(id.value));

    return id;
  }();

  return id_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ PageReader OpaquePageView::page_reader() noexcept
{
  return [](std::shared_ptr<const PageBuffer> page_buffer)
             -> StatusOr<std::shared_ptr<const PageView>> {
    return {std::make_shared<OpaquePageView>(std::move(page_buffer))};
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status OpaquePageView::register_layout(PageCache& cache) noexcept
{
  return cache.register_page_reader(OpaquePageView::page_layout_id(), __FILE__, __LINE__,
                                    OpaquePageView::page_reader());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageLayoutId OpaquePageView::get_page_layout_id() const /*override*/
{
  return OpaquePageView::page_layout_id();
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
usize OpaquePageView::get_keys([[maybe_unused]] LowerBoundParam lower_bound,
                               [[maybe_unused]] KeyView* key_buffer_out,
                               [[maybe_unused]] usize key_buffer_size,
                               [[maybe_unused]] StableStringStore& storage) const /*override*/
{
  return 0;
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
