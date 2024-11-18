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

#include <batteries/static_assert.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A minimal page layout that references other pages by id.
 */
struct PackedPageGraphNode {
  PackedArray<PackedPageId> edges;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageGraphNode), sizeof(PackedArray<PackedPageId>));

inline usize packed_sizeof(const PackedPageGraphNode& n)
{
  return packed_sizeof(n.edges);
}

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
  static StatusOr<PageGraphNodeBuilder> from_new_page(
      const StatusOr<std::shared_ptr<PageBuffer>>& status_or_page_buffer)
  {
    BATT_REQUIRE_OK(std::move(status_or_page_buffer));

    return PageGraphNodeBuilder{batt::make_copy(*status_or_page_buffer)};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
  /** \brief The page layout id for all instances of this class.
   */
  static PageLayoutId page_layout_id();

  /** \brief Returns the PageReader for this layout.
   */
  static PageReader page_reader();

  /** \brief Registers this page layout with the passed cache, so that pages using the layout can be
   * correctly loaded and parsed by the PageCache.
   */
  static batt::Status register_layout(PageCache& cache);

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
  usize get_keys([[maybe_unused]] LowerBoundParam lower_bound,
                 [[maybe_unused]] KeyView* key_buffer_out, [[maybe_unused]] usize key_buffer_size,
                 [[maybe_unused]] StableStringStore& storage) const override
  {
    return 0;
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
