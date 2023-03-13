//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PINNED_PAGE_HPP
#define LLFS_PINNED_PAGE_HPP

#include <llfs/cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_view.hpp>

#include <batteries/async/latch.hpp>

#include <memory>

namespace llfs {

class PageView;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// A page view that is cache resident and will not be evicted so long as a copy of the PinnedPage
// exists.
//
class PinnedPage
{
  using PinnedCacheSlotT =
      PinnedCacheSlot<page_id_int, batt::Latch<std::shared_ptr<const PageView>>>;

 public:
  PinnedPage() = default;

  /*implicit*/ PinnedPage(std::nullptr_t) : PinnedPage()
  {
  }

  explicit PinnedPage(const PageView* page_view, PinnedCacheSlotT&& pinned_cache_slot) noexcept
      : page_view_{page_view}
      , pinned_cache_slot_{std::move(pinned_cache_slot)}
  {
    BATT_CHECK_EQ(this->page_view_ != nullptr, bool{this->pinned_cache_slot_});
  }

  const PageView* get() const noexcept
  {
    return this->page_view_;
  }

  // Give a hint to the cache that this page is being replaced and should be deprioritized.
  //
  void hint_obsolete() const
  {
    if (this->pinned_cache_slot_) {
      this->pinned_cache_slot_.slot()->set_obsolete_hint(true);
    }
  }

  const PageView* operator->() const noexcept
  {
    return this->get();
  }

  const PageView& operator*() const noexcept
  {
    return *this->get();
  }

  explicit operator bool() const noexcept
  {
    return this->get() != nullptr;
  }

  PinnedCacheSlotT get_cache_slot() const noexcept
  {
    return this->pinned_cache_slot_;
  }

  std::shared_ptr<const PageBuffer> get_page_buffer() const noexcept
  {
    return this->page_view_->data();
  }

  std::shared_ptr<const PageView> get_shared_view() const
  {
    return BATT_OK_RESULT_OR_PANIC(this->pinned_cache_slot_.get()->get_ready_value_or_panic());
  }

  ConstBuffer const_buffer() const noexcept
  {
    return this->page_view_->const_buffer();
  }

  ConstBuffer const_payload() const noexcept
  {
    return this->page_view_->const_payload();
  }

  friend page_id_int get_page_id_int(const PinnedPage& pinned);

  friend PageId get_page_id(const PinnedPage& pinned);

 private:
  const PageView* page_view_ = nullptr;
  PinnedCacheSlotT pinned_cache_slot_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

bool operator==(const PinnedPage& l, const PinnedPage& r);

bool operator!=(const PinnedPage& l, const PinnedPage& r);

bool operator==(const PinnedPage& l, const std::nullptr_t& r);

bool operator!=(const PinnedPage& l, const std::nullptr_t& r);

bool operator==(const std::nullptr_t& l, const PinnedPage& r);

bool operator!=(const std::nullptr_t& l, const PinnedPage& r);

page_id_int get_page_id_int(const PinnedPage& pinned);

PageId get_page_id(const PinnedPage& pinned);

}  // namespace llfs

#endif  // LLFS_PINNED_PAGE_HPP
