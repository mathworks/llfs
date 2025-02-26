//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_TRACER_HPP
#define LLFS_PAGE_TRACER_HPP

#include <llfs/page_device_entry.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/seq.hpp>
#include <batteries/status.hpp>

namespace llfs {
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An interface representing an entity that has the capability to trace and/or store data
 * regarding the outgoing reference counts of pages.
 */
class PageTracer
{
 public:
  PageTracer(const PageTracer&) = delete;
  PageTracer& operator=(const PageTracer&) = delete;

  virtual ~PageTracer() = default;

  /** \brief Traces the outgoing references of the page with id `from_page_id`.
   *
   * The returned `BoxedSeq` will remain valid until the `PageTracer` object calling
   * `trace_page_refs` goes out of scope, or until the next call to this function (whichever comes
   * first).
   *
   * \return A sequence of page ids for the pages referenced from the page `from_page_id`.
   */
  virtual batt::StatusOr<batt::BoxedSeq<PageId>> trace_page_refs(PageId from_page_id) noexcept = 0;

 protected:
  PageTracer() = default;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A PageTracer with the ability to load a page and trace its outgoing references to other
 * pages.
 */
class LoadingPageTracer : public PageTracer
{
 public:
  explicit LoadingPageTracer(PageLoader& page_loader, bool ok_if_not_found = false) noexcept;

  ~LoadingPageTracer() noexcept;

  LoadingPageTracer(const LoadingPageTracer&) = delete;
  LoadingPageTracer& operator=(const LoadingPageTracer&) = delete;

  /** \brief Loads the page with page id `from_page_id` and traces its outgoing references.
   * \return A sequence of page ids for the pages referenced from the page `from_page_id`.
   */
  batt::StatusOr<batt::BoxedSeq<PageId>> trace_page_refs(PageId from_page_id) noexcept override;

 private:
  /** \brief The PageLoader used to load the page being traced.
   */
  PageLoader& page_loader_;

  /** \brief The PinnedPage for the page with id `from_page_id` that is loaded in the
   * `trace_page_refs` function.
   */
  PinnedPage pinned_page_;

  /** \brief A boolean to pass into the PageLoader's `get_page` function.
   */
  bool ok_if_not_found_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A PageTracer with the ability to load a page and trace its outgoing references to other
 * pages, as well as look up cached information about a page's outgoing references to other pages.
 * If this PageTracer can find valid cached information stating that a page does not have any
 * outgoing refs, it will not need to load the page like LoadingPageTracer always does. The
 * accessibility of any cached information occurs through a PageDeviceEntry object.
 */
class CachingPageTracer : public PageTracer
{
 public:
  explicit CachingPageTracer(const std::vector<std::shared_ptr<PageDeviceEntry>>& page_devices,
                             PageTracer& loader) noexcept;

  CachingPageTracer(const CachingPageTracer&) = delete;
  CachingPageTracer operator=(const CachingPageTracer&) = delete;

  ~CachingPageTracer() noexcept;

  /** \brief Traces the outgoing references of the page with page id `from_page_id`. First, this
   * function will attempt to see if there exists any cached information about the page's outgoing
   * refs status. If it cannot find any valid information, or if it finds that there are outgoing
   * refs, it will fall back on its wrapped PageTracer to load and trace the outgoing refs.
   *
   * \return A sequence of page ids for the pages referenced from the page `from_page_id`.
   */
  batt::StatusOr<batt::BoxedSeq<PageId>> trace_page_refs(PageId from_page_id) noexcept override;

 private:
  /** \brief A vector of page device entries used to access cached information about a page's
   * outgoing refs status.
   */
  const std::vector<std::shared_ptr<PageDeviceEntry>>& page_devices_;

  /** \brief The "wrapped" PageTracer that this CachingPageTracer instance falls back on in the
   * event that no cached information is found about a page.
   */
  PageTracer& loader_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_TRACER_HPP
