//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <llfs/page_cache.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

#include <atomic>
#include <memory>

namespace llfs {

/** \brief A placeholder PageView for new pages, whose data buffer is uninitialized.
 *
 * This implementation of PageView allows another PageView object to be attached once the page has
 * been full initialized, via the `set_new_page_view` function.
 *
 * Before this implementation is set, the 'core' member functions of the PageView interface
 * (get_page_layout_id(), trace_refs(), min_key(), max_key(), build_filter(), dump_to_ostream()) act
 * as simple 'no-op' stubs.  After the implementation is set, these functions act as proxies for the
 * same functions in the attached PageView.
 */
class NewPageView : public PageView
{
 public:
  static const PageLayoutId& page_layout_id() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using PageView::PageView;

  // Get the tag for this page view.
  //
  PageLayoutId get_page_layout_id() const override;

  // Returns a sequence of the ids of all pages directly referenced by this one.
  //
  BoxedSeq<PageId> trace_refs() const override;

  // Returns the minimum key value contained within this page.
  //
  Optional<KeyView> min_key() const override;

  // Returns the maximum key value contained within this page.
  //
  Optional<KeyView> max_key() const override;

  // Dump a human-readable representation or summary of the page to the passed stream.
  //
  void dump_to_ostream(std::ostream& out) const override;

  // Sets the PageView implementations for newly created pages.
  //
  Status set_new_page_view(std::shared_ptr<const PageView>&& view) const override;

  // Returns a shared_ptr to the _mutable_ PageBuffer associated with this PageView; only supported
  // by NewPageView.
  //
  StatusOr<std::shared_ptr<PageBuffer>> get_new_page_buffer() const override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  mutable std::atomic<const PageView*> active_view_{nullptr};

  mutable std::shared_ptr<const PageView> view_impl_{};
};

}  // namespace llfs
