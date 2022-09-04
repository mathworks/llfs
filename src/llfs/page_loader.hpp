//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_LOADER_HPP
#define LLFS_PAGE_LOADER_HPP

#include <llfs/api_types.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_id_slot.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/status.hpp>

namespace llfs {

// Interface for an entity which can resolve `PageId`s into `PinnedPage`s.
//
class PageLoader
{
 public:
  PageLoader(const PageLoader&) = delete;
  PageLoader& operator=(const PageLoader&) = delete;

  virtual ~PageLoader() = default;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  virtual void prefetch_hint(PageId page_id) = 0;

  virtual StatusOr<PinnedPage> get(PageId page_id, const Optional<PageLayoutId>& required_layout,
                                   PinPageToJob pin_page_to_job, OkIfNotFound ok_if_not_found) = 0;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot,
                                   const Optional<PageLayoutId>& required_layout,
                                   PinPageToJob pin_page_to_job, OkIfNotFound ok_if_not_found);

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot,
                                   const Optional<PageLayoutId>& required_layout,
                                   OkIfNotFound ok_if_not_found);

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot, PinPageToJob pin_page_to_job,
                                   OkIfNotFound ok_if_not_found);

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot, OkIfNotFound ok_if_not_found);

  virtual StatusOr<PinnedPage> get(PageId page_id, const Optional<PageLayoutId>& required_layout,
                                   OkIfNotFound ok_if_not_found);

  virtual StatusOr<PinnedPage> get(PageId page_id, PinPageToJob pin_page_to_job,
                                   OkIfNotFound ok_if_not_found);

  virtual StatusOr<PinnedPage> get(PageId page_id, OkIfNotFound ok_if_not_found);

 protected:
  PageLoader() = default;
};

}  // namespace llfs

#endif  // LLFS_PAGE_LOADER_HPP
