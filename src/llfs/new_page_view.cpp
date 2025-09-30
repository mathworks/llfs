//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/new_page_view.hpp>
//

#include <batteries/seq/empty.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ const PageLayoutId& NewPageView::page_layout_id() noexcept
{
  const static PageLayoutId id_ = [] {
    llfs::PageLayoutId id;

    const char tag[sizeof(id.value) + 1] = "????????";

    std::memcpy(&id.value, tag, sizeof(id.value));

    return id;
  }();

  return id_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageLayoutId NewPageView::get_page_layout_id() const /*override*/
{
  const PageView* view = this->active_view_.load();
  if (view != nullptr) {
    return view->get_page_layout_id();
  }
  return NewPageView::page_layout_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> NewPageView::trace_refs() const /*override*/
{
  const PageView* view = this->active_view_.load();
  if (view != nullptr) {
    return view->trace_refs();
  }
  return seq::Empty<PageId>{} | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> NewPageView::min_key() const /*override*/
{
  const PageView* view = this->active_view_.load();
  if (view != nullptr) {
    return view->min_key();
  }
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> NewPageView::max_key() const /*override*/
{
  const PageView* view = this->active_view_.load();
  if (view != nullptr) {
    return view->max_key();
  }
  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void NewPageView::dump_to_ostream(std::ostream& out) const /*override*/
{
  const PageView* view = this->active_view_.load();
  if (view != nullptr) {
    return view->dump_to_ostream(out);
  }
  out << "(?)";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status NewPageView::set_new_page_view(std::shared_ptr<const PageView>&& view) const /*override*/
{
  // Do some quick sanity checks: this view's layout must match what is currently in the page
  // header, and the buffer itself must match.
  //
  if (view->get_page_layout_id() != this->header().layout_id || view->data() != this->data()) {
    return batt::StatusCode::kInvalidArgument;
  }

  // If this thread observes the atomic transition from nullptr to a valid object, then it is
  // granted exclusive access to set the view_impl_ member, for object lifetime purposes only.
  //
  const PageView* expected = nullptr;
  if (!this->active_view_.compare_exchange_strong(expected, view.get())) {
    return batt::StatusCode::kFailedPrecondition;
  }

  this->view_impl_ = std::move(view);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> NewPageView::get_new_page_buffer() const /*override*/
{
  return {std::const_pointer_cast<PageBuffer>(this->data())};
}

}  // namespace llfs
