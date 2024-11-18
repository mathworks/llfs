//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_OPAQUE_PAGE_VIEW_HPP
#define LLFS_OPAQUE_PAGE_VIEW_HPP

#include <llfs/page_cache.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

namespace llfs {

class OpaquePageView : public PageView
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

  /** \brief Retrieves at most `key_buffer_size` number of keys contained in this page.
   *
   * @param lower_bound This parameter allows for "skipping" to an arbitrary place in the page's key
   * set. The caller can provide either a `KeyView` value or an index into the key set, which
   * represents the starting key from which this function will collect keys from to return.
   *
   * @param key_buffer_out The output buffer that will be filled by this function with the requested
   * keys.
   *
   * @param key_buffer_size The size of the output buffer holding the returned keys.
   *
   * @param storage A `StableStringStore` instance that the caller can provide so that the returned
   * keys can still be a list of `KeyView` even if the keys in the page are stored in a way that
   * isn't contiguous or are compressed. Specific implementations of `PageView` will choose to use
   * this based on their key storage.
   *
   * \return The number of keys filled into `key_buffer_out`. This value will either be
   * `key_buffer_size` or the number of keys between `lower_bound` and the end of the key set,
   * whichever is smaller. In the event that the `lower_bound` parameter provided does not exist in
   * the key set (or is out of the range of the key set), this function will return 0.
   */
  usize get_keys(LowerBoundParam lower_bound, KeyView* key_buffer_out, usize key_buffer_size,
                 StableStringStore& storage) const override;

  // Builds a key-based approximate member query (AMQ) filter for the page, to answer the question
  // whether a given key *might* be contained by the page.
  //
  std::shared_ptr<PageFilter> build_filter() const override;

  // Dump a human-readable representation or summary of the page to the passed stream.
  //
  void dump_to_ostream(std::ostream& out) const override;
};

}  // namespace llfs

#endif  // LLFS_OPAQUE_PAGE_VIEW_HPP
