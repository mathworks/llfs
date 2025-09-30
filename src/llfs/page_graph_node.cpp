//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_graph_node.hpp>
//

namespace llfs {

namespace {
constexpr usize kMarkerSpacing = 256;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageGraphNodeBuilder

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageGraphNodeBuilder::PageGraphNodeBuilder(
    std::shared_ptr<PageBuffer>&& page_buffer) noexcept
    : page_buffer_{std::move(page_buffer)}
    , available_{this->page_buffer_->mutable_payload()}
    , packed_{reinterpret_cast<PackedPageGraphNode*>(this->available_.data())}
{
  BATT_CHECK_GE(this->available_.size(), sizeof(PackedPageGraphNode));

  {
    MutableBuffer b = this->available_ + kMarkerSpacing;
    u64 marker = this->page_buffer_->page_id().int_value();
    while (b.size() >= sizeof(little_u64)) {
      *((little_u64*)b.data()) = marker;
      ++marker;
      b += kMarkerSpacing;
    }
  }

  this->available_ += sizeof(PackedPageGraphNode);
  this->packed_->edges.initialize(0u);
  {
    PackedPageHeader* header = mutable_page_header(this->page_buffer_.get());
    header->layout_id = PageGraphNodeView::page_layout_id();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageGraphNodeBuilder::add_page(PageId page_id) noexcept
{
  BATT_CHECK_NOT_NULLPTR(this->packed_);
  BATT_CHECK_NOT_NULLPTR(this->page_buffer_);

  if (this->available_.size() < sizeof(PackedPageId)) {
    return false;
  }

  auto* const packed_page_id = reinterpret_cast<PackedPageId*>(this->available_.data());
  *packed_page_id = PackedPageId::from(page_id);

  this->available_ += sizeof(PackedPageId);
  this->packed_->edges.item_count += 1;

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageGraphNodeBuilder::build(PageCacheJob& job) &&
{
  BATT_CHECK_NOT_NULLPTR(this->packed_);
  BATT_CHECK_NOT_NULLPTR(this->page_buffer_);

  this->packed_ = nullptr;

  BATT_ASSIGN_OK_RESULT(std::shared_ptr<PageGraphNodeView> view,
                        PageGraphNodeView::make_shared(this->page_buffer_));

  return job.pin_new(std::move(view), LruPriority{0}, /*callers=*/0);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageGraphNodeView

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ PageLayoutId PageGraphNodeView::page_layout_id()
{
  static const PageLayoutId id = PageLayoutId::from_str("grphnode");
  return id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ PageReader PageGraphNodeView::page_reader()
{
  return [](std::shared_ptr<const PageBuffer> page_buffer)
             -> StatusOr<std::shared_ptr<const PageView>> {
    return {PageGraphNodeView::make_shared(std::move(page_buffer))};
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::Status PageGraphNodeView::register_layout(PageCache& cache)
{
  return cache.register_page_reader(PageGraphNodeView::page_layout_id(), __FILE__, __LINE__,
                                    PageGraphNodeView::page_reader());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::shared_ptr<PageGraphNodeView>> PageGraphNodeView::make_shared(
    std::shared_ptr<const PageBuffer>&& page_buffer)
{
  BATT_ASSIGN_OK_RESULT(
      const PackedPageGraphNode& packed_ref,
      unpack_cast(page_buffer->const_payload(), batt::StaticType<PackedPageGraphNode>{}));

  // Check markers.
  //
  {
    ConstBuffer b =
        page_buffer->const_payload() +
        ((packed_sizeof(packed_ref) + kMarkerSpacing + 1) / kMarkerSpacing) * kMarkerSpacing;

    u64 marker = page_buffer->page_id().int_value();

    while (b.size() >= sizeof(little_u64)) {
      if (*((const little_u64*)b.data()) != marker) {
        return {batt::StatusCode::kDataLoss};
      }
      ++marker;
      b += kMarkerSpacing;
    }
  }

  return std::shared_ptr<PageGraphNodeView>(
      new PageGraphNodeView{std::move(page_buffer), &packed_ref});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageGraphNodeView::PageGraphNodeView(std::shared_ptr<const PageBuffer>&& page_buffer,
                                                  const PackedPageGraphNode* packed) noexcept
    : PageView{std::move(page_buffer)}
    , packed_{packed}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageLayoutId PageGraphNodeView::get_page_layout_id() const /*override*/
{
  return PageGraphNodeView::page_layout_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> PageGraphNodeView::trace_refs() const /*override*/
{
  return as_seq(this->packed_->edges) | seq::map(BATT_OVERLOADS_OF(get_page_id)) | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageGraphNodeView::dump_to_ostream(std::ostream& out) const /*override*/
{
  out << "PageGraphNode";
}

}  //namespace llfs
