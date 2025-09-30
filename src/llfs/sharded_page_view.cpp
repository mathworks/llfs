//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/sharded_page_view.hpp>
//

#include <batteries/seq/empty.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ const PageLayoutId& ShardedPageView::page_layout_id() noexcept
{
  const static PageLayoutId id_ = [] {
    llfs::PageLayoutId id;

    const char tag[sizeof(id.value) + 1] = "_pshard_";

    std::memcpy(&id.value, tag, sizeof(id.value));

    return id;
  }();

  return id_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ PageReader ShardedPageView::page_reader() noexcept
{
  return [](std::shared_ptr<const PageBuffer> page_buffer)
             -> StatusOr<std::shared_ptr<const PageView>> {
    return {std::make_shared<ShardedPageView>(std::move(page_buffer))};
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status ShardedPageView::register_layout(PageCache& cache) noexcept
{
  return cache.register_page_reader(ShardedPageView::page_layout_id(), __FILE__, __LINE__,
                                    ShardedPageView::page_reader());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageLayoutId ShardedPageView::get_page_layout_id() const /*override*/
{
  return ShardedPageView::page_layout_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> ShardedPageView::trace_refs() const /*override*/
{
  return seq::Empty<PageId>{} | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> ShardedPageView::min_key() const /*override*/
{
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> ShardedPageView::max_key() const /*override*/
{
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ShardedPageView::dump_to_ostream(std::ostream& out) const /*override*/
{
  out << "(?)";
}

}  // namespace llfs
