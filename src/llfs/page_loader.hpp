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

#include <llfs/config.hpp>
//
#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_load_options.hpp>
#include <llfs/page_loader_decl.hpp>
#include <llfs/pin_page_to_job.hpp>
#include <llfs/status.hpp>

namespace llfs {

class PageCache;

/** \brief Interface for an entity which can resolve `PageId`s into `PinnedPage`s.
 */
template <typename PinnedPageParamT>
class BasicPageLoader
{
 public:
  /** \brief A movable and copyable type representing a pin on a loaded page.
   */
  using PinnedPageT = PinnedPageParamT;

  /** \brief PageLoaders are not copyable.
   */
  BasicPageLoader(const BasicPageLoader&) = delete;

  /** \brief PageLoaders are not copyable.
   */
  BasicPageLoader& operator=(const BasicPageLoader&) = delete;

  virtual ~BasicPageLoader() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a pointer to the PageCache object associated with this loader.
   */
  virtual PageCache* page_cache() const = 0;

  /** \brief Provides a hint to the loader that the given page is likely to be loaded in the near
   * future.
   *
   * PageLoader implementations do not need to do anything with this information; however, a
   * reasonable implementation might be to start a background load of the given page if there is
   * extra bandwidth available in the underyling storage device.
   */
  virtual void prefetch_hint(PageId page_id) = 0;

  /** \brief Attempts to pin the given page in the cache; guaranteed not to block or perform I/O.
   *
   * If the specified page is not currently in the cache, this function returns
   * batt::StatusCode::kUnavailable.
   */
  virtual StatusOr<PinnedPageT> try_pin_cached_page(PageId page_id,
                                                    const PageLoadOptions& options) = 0;

  /** \brief Pins and returns the given page, loading it if it is not yet in cache.
   *
   * This function may block the caller while performing I/O.
   */
  virtual StatusOr<PinnedPageT> load_page(PageId page_id, const PageLoadOptions& options) = 0;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 protected:
  BasicPageLoader() = default;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
// NOTE: PageLoader is a type alias defined in `<llfs/page_loader_decl.hpp>`.
//
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace llfs

#endif  // LLFS_PAGE_LOADER_HPP
