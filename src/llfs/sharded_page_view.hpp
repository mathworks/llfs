//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <llfs/page_cache.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

namespace llfs {

class ShardedPageView : public PageView
{
 public:
  static const PageLayoutId& page_layout_id() noexcept;

  static PageReader page_reader() noexcept;

  static Status register_layout(PageCache& cache) noexcept;

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
};

}  //namespace llfs
