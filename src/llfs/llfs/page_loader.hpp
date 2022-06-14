#pragma once
#ifndef LLFS_PAGE_LOADER_HPP
#define LLFS_PAGE_LOADER_HPP

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
                                   PinPageToJob pin_page_to_job) = 0;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot,
                                   const Optional<PageLayoutId>& required_layout,
                                   PinPageToJob pin_page_to_job);

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot,
                                   const Optional<PageLayoutId>& required_layout);

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot, PinPageToJob pin_page_to_job);

  virtual StatusOr<PinnedPage> get(const PageIdSlot& page_id_slot);

  virtual StatusOr<PinnedPage> get(PageId page_id, const Optional<PageLayoutId>& required_layout);

  virtual StatusOr<PinnedPage> get(PageId page_id, PinPageToJob pin_page_to_job);

  virtual StatusOr<PinnedPage> get(PageId page_id);

 protected:
  PageLoader() = default;
};

}  // namespace llfs

#endif  // LLFS_PAGE_LOADER_HPP
