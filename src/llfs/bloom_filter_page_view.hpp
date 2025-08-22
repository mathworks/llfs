//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BLOOM_FILTER_PAGE_VIEW_HPP
#define LLFS_BLOOM_FILTER_PAGE_VIEW_HPP

#include <llfs/config.hpp>
//
#include <llfs/packed_bloom_filter_page.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

#include <batteries/seq/boxed.hpp>
#include <batteries/seq/empty.hpp>

namespace llfs {

class BloomFilterPageView : public PageView
{
 public:
  /** \brief Returns the PageReader for this layout.
   */
  static PageReader page_reader();

  /** \brief Registers this page layout with the passed cache, so that pages using the layout can be
   * correctly loaded and parsed by the PageCache.
   */
  static batt::Status register_layout(PageCache& cache);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BloomFilterPageView(std::shared_ptr<const PageBuffer>&& page_buffer) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageLayoutId get_page_layout_id() const override
  {
    return PackedBloomFilterPage::page_layout_id();
  }

  BoxedSeq<PageId> trace_refs() const override
  {
    return batt::seq::Empty<PageId>{} | batt::seq::boxed();
  }

  Optional<KeyView> min_key() const override
  {
    return None;
  }

  Optional<KeyView> max_key() const override
  {
    return None;
  }

  void check_integrity() const
  {
    this->packed_->check_integrity();
  }

  void dump_to_ostream(std::ostream& out) const override
  {
    out << "BloomFilter";
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageId src_page_id() const noexcept
  {
    return this->packed_->src_page_id.unpack();
  }

  const PackedBloomFilter& bloom_filter() const noexcept
  {
    return this->packed_->bloom_filter;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const PackedBloomFilterPage* packed_;
};

}  //namespace llfs

#endif  // LLFS_BLOOM_FILTER_PAGE_VIEW_HPP
