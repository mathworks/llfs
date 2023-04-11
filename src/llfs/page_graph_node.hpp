//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_GRAPH_NODE_HPP
#define LLFS_PAGE_GRAPH_NODE_HPP

#include <llfs/config.hpp>
//
#include <llfs/optional.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/page_filter.hpp>
#include <llfs/page_view.hpp>
#include <llfs/seq.hpp>
#include <llfs/unpack_cast.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A minimal page layout that references other pages by id.
 */
struct PackedPageGraphNode {
  PackedArray<PackedPageId> edges;
};

inline Status validate_packed_value(const PackedPageGraphNode& packed, const void* buffer_data,
                                    usize buffer_size)
{
  return validate_packed_value(packed.edges, buffer_data, buffer_size);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Builder for PageGraphNode pages.
 */
class PageGraphNodeBuilder
{
 public:
  explicit PageGraphNodeBuilder(std::shared_ptr<PageBuffer>&& page_buffer) noexcept;

  bool add_page(PageId page_id) noexcept;

  StatusOr<PinnedPage> build(PageCacheJob& job) &&;

 private:
  std::shared_ptr<PageBuffer> page_buffer_;
  MutableBuffer available_;
  PackedPageGraphNode* packed_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief
 */
class PageGraphNodeView : public PageView
{
 public:
  /** \brief Returns a shared instance of PageGraphNodeView for the given page data.
   * \return error status if the page is ill-formed
   */
  static StatusOr<std::shared_ptr<PageGraphNodeView>> make_shared(
      std::shared_ptr<const PageBuffer>&& page_buffer);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Get the tag for this page view.
   */
  PageLayoutId get_page_layout_id() const override;

  /** \brief Returns a sequence of the ids of all pages directly referenced by this one.
   */
  BoxedSeq<PageId> trace_refs() const override;

  /** \brief Returns the minimum key value contained within this page.
   */
  Optional<KeyView> min_key() const override
  {
    return None;
  }

  /** \brief Returns the maximum key value contained within this page.
   */
  Optional<KeyView> max_key() const override
  {
    return None;
  }

  /** \brief Builds a key-based approximate member query (AMQ) filter for the page, to answer the
   * question whether a given key *might* be contained by the page.
   */
  std::shared_ptr<PageFilter> build_filter() const override
  {
    return std::make_shared<NullPageFilter>(this->page_id());
  }

  /** \brief Dump a human-readable representation or summary of the page to the passed stream.
   */
  void dump_to_ostream(std::ostream& out) const override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Constructor is private so we can make sure that `unpack_cast` is used to validate the
   * page data layout.
   */
  explicit PageGraphNodeView(std::shared_ptr<const PageBuffer>&& page_buffer,
                             const PackedPageGraphNode* packed) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PackedPageGraphNode* packed_;
};

}  //namespace llfs

#endif  // LLFS_PAGE_GRAPH_NODE_HPP
